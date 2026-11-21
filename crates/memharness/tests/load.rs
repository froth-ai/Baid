//! Loading and verifying the `tiny` dataset against the in-process backends (memory, and S3
//! over an in-memory object store). The same code runs against Neo4j and SeaweedFS in
//! `tests/live.rs`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use adapters::{MemoryBackend, S3Backend};
use baid_core::{Backend, TenantId};
use memharness::datagen::{write_dataset, Scale};
use memharness::dataset::{Dataset, DatasetError, ROWS};
use memharness::load::{load, LoadOptions};
use memharness::verify::{verify, VerifyOptions, VerifyReport};

/// A temporary directory, removed on drop.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A fresh directory holding a freshly generated `tiny-s1-v1`.
fn tiny() -> (Scratch, PathBuf, Dataset) {
    static N: AtomicU32 = AtomicU32::new(0);
    let root = std::env::temp_dir().join(format!(
        "memharness-load-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&root);
    let (dir, _) = write_dataset(&root, Scale::Tiny, 1).expect("datagen");
    let ds = Dataset::open(&dir).expect("open");
    (Scratch(root), dir, ds)
}

fn quiet() -> LoadOptions {
    LoadOptions {
        concurrency: 8,
        progress_every: None,
    }
}

fn opts(full_text: bool) -> VerifyOptions {
    VerifyOptions {
        samples_per_tenant: 30,
        full_text,
        ..VerifyOptions::default()
    }
}

fn s3() -> S3Backend {
    S3Backend::with_store(
        Arc::new(object_store::memory::InMemory::new()),
        "bench",
        Duration::from_secs(5),
        1_000,
    )
}

fn assert_ok(r: &VerifyReport) {
    assert!(r.ok(), "verification failed:\n{r}");
    assert_eq!(r.tenants, 2);
    assert_eq!(r.rows_sampled, 60);
    assert!(r.entities_sampled >= 30, "{r}");
    assert!(r.items_checked > r.rows_sampled, "{r}");
    assert_eq!(r.cross_tenant_searches, 60);
}

#[tokio::test]
async fn loads_and_verifies_on_the_memory_backend() {
    let (_tmp, _, ds) = tiny();
    let b = MemoryBackend::new();
    let rep = load(&b, &ds, &quiet(), "memory").await.expect("load");
    assert_eq!(rep.written, 200);
    let r = verify(&b, &ds, &opts(true)).await.expect("verify");
    assert_ok(&r);
    assert_eq!(r.full_text_searches, 60);
}

#[tokio::test]
async fn loads_and_verifies_on_s3() {
    let (_tmp, _, ds) = tiny();
    let b = s3();
    load(&b, &ds, &quiet(), "s3").await.expect("load");
    let r = verify(&b, &ds, &opts(false)).await.expect("verify");
    assert_ok(&r);
    assert_eq!(r.full_text_searches, 0);
}

/// Idempotency keys: a second load acknowledges every row with the same node, and searches
/// still find exactly one memory per unique word.
#[tokio::test]
async fn loading_twice_stores_nothing_twice() {
    let (_tmp, _, ds) = tiny();
    for b in [
        Box::new(MemoryBackend::new()) as Box<dyn Backend>,
        Box::new(s3()),
    ] {
        let first = load(b.as_ref(), &ds, &quiet(), "a").await.expect("load");
        let second = load(b.as_ref(), &ds, &quiet(), "b").await.expect("load");
        assert_eq!(first.acks, second.acks, "a re-load stored new memories");
        let r = verify(b.as_ref(), &ds, &opts(false)).await.expect("verify");
        assert!(r.ok(), "{r}");
    }
}

#[test]
fn a_modified_dataset_is_refused() {
    let (_tmp, dir, _) = tiny();
    let path = dir.join(ROWS);
    let mut bytes = std::fs::read(&path).expect("read");
    let i = bytes.iter().position(|&b| b == b'S').expect("an S");
    bytes[i] = b'T';
    std::fs::write(&path, &bytes).expect("write");
    match Dataset::open(&dir) {
        Err(e @ DatasetError::Mismatch { .. }) => {
            let msg = e.to_string();
            assert!(msg.contains("refusing to load"), "{msg}");
            assert!(
                msg.contains("Regenerate it with `memharness datagen --scale tiny --seed 1"),
                "{msg}"
            );
        }
        other => panic!("expected a mismatch, got {other:?}"),
    }

    // Same size, truncated, or missing: all refused.
    bytes.truncate(bytes.len() - 1);
    std::fs::write(&path, &bytes).expect("write");
    assert!(matches!(
        Dataset::open(&dir),
        Err(DatasetError::Mismatch { .. })
    ));
    std::fs::remove_file(&path).expect("rm");
    match Dataset::open(&dir) {
        Err(e @ DatasetError::Missing { .. }) => {
            assert!(e.to_string().contains("memharness datagen"), "{e}")
        }
        other => panic!("expected missing, got {other:?}"),
    }
}

// The verifier must catch what a broken loader would do.

#[tokio::test]
async fn verify_catches_missing_rows() {
    let (_tmp, _, ds) = tiny();
    let b = MemoryBackend::new();
    for row in ds.rows().expect("rows") {
        let row = row.expect("row");
        if row.n % 3 != 0 {
            b.write(&row.tenant, &row.fact(ds.name()))
                .await
                .expect("write");
        }
    }
    let r = verify(&b, &ds, &opts(true)).await.expect("verify");
    assert!(!r.ok());
    assert!(
        r.failures.iter().any(|f| f.contains("expected exactly 1")),
        "{r}"
    );
    assert!(
        r.failures.iter().any(|f| f.contains("neighbourhood")),
        "{r}"
    );
}

#[tokio::test]
async fn verify_catches_duplicates() {
    let (_tmp, _, ds) = tiny();
    let b = MemoryBackend::new();
    for _ in 0..2 {
        for row in ds.rows().expect("rows") {
            let row = row.expect("row");
            let mut fact = row.fact(ds.name());
            fact.idempotency_key = None;
            b.write(&row.tenant, &fact).await.expect("write");
        }
    }
    let r = verify(&b, &ds, &opts(true)).await.expect("verify");
    assert!(
        r.failures
            .iter()
            .any(|f| f.contains("2 items, expected exactly 1")),
        "{r}"
    );
    assert!(
        r.failures.iter().any(|f| f.contains("returned twice")),
        "{r}"
    );
}

#[tokio::test]
async fn verify_catches_rows_in_the_wrong_tenant() {
    let (_tmp, _, ds) = tiny();
    let b = MemoryBackend::new();
    let first = TenantId::new("tiny-s1-v1-0").expect("tenant");
    for row in ds.rows().expect("rows") {
        let row = row.expect("row");
        b.write(&first, &row.fact(ds.name())).await.expect("write");
    }
    let r = verify(&b, &ds, &opts(true)).await.expect("verify");
    assert!(
        r.failures.iter().any(|f| f.contains("belongs to tenant")),
        "{r}"
    );
    assert!(r.failures.iter().any(|f| f.contains("wrong tenant")), "{r}");
}

#[tokio::test]
async fn verify_catches_altered_text_and_entities() {
    let (_tmp, _, ds) = tiny();
    let b = MemoryBackend::new();
    for row in ds.rows().expect("rows") {
        let row = row.expect("row");
        let mut fact = row.fact(ds.name());
        if row.n % 2 == 0 {
            fact.text = fact.text.replace("Ref", "ref:");
        } else {
            fact.entities.truncate(1);
        }
        b.write(&row.tenant, &fact).await.expect("write");
    }
    let r = verify(&b, &ds, &opts(true)).await.expect("verify");
    assert!(
        r.failures
            .iter()
            .any(|f| f.contains("different memory") || f.contains("not from the dataset")),
        "{r}"
    );
    assert!(r.failures.iter().any(|f| f.contains("entities")), "{r}");
}

/// A committed manifest pins the output: regenerating must reproduce it, and a manifest
/// that records different bytes is never overwritten.
#[test]
fn datagen_never_overwrites_a_different_manifest() {
    let (_tmp, dir, ds) = tiny();
    let root = dir.parent().expect("root").to_path_buf();
    // Same output: fine, and the dataset still opens.
    write_dataset(&root, Scale::Tiny, 1).expect("regenerate");
    Dataset::open(&dir).expect("open");

    let mpath = dir.join(memharness::dataset::MANIFEST);
    let mut m = ds.manifest().clone();
    m.blake3 = "0".repeat(64);
    std::fs::write(&mpath, serde_json::to_string_pretty(&m).expect("json")).expect("write");
    let err = write_dataset(&root, Scale::Tiny, 1).expect_err("must refuse");
    assert!(err.to_string().contains("GENERATOR_VERSION"), "{err}");
    let kept: memharness::dataset::Manifest =
        serde_json::from_str(&std::fs::read_to_string(&mpath).expect("read")).expect("json");
    assert_eq!(kept.blake3, "0".repeat(64), "the manifest was overwritten");
}
