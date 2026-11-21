//! The loaders against the local stack: Neo4j (`BAID_TEST_NEO4J_URL`) and S3
//! (`BAID_TEST_S3_URL`, SeaweedFS). Each half is skipped unless its variable is set; `just
//! test-live` sets both. The `tiny-s1-v1` tenants are left in place: re-runs are idempotent.

use std::path::PathBuf;
use std::time::Duration;

use adapters::{BoltBackend, BoltConfig, Resilient, S3Backend, S3Config};
use baid_core::{Backend, BackendQuery, Filters};
use memharness::datagen::{unique_in_text, write_dataset, Scale};
use memharness::dataset::{Dataset, Row};
use memharness::load::{load, LoadOptions};
use memharness::verify::{verify, VerifyOptions};

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tiny(tag: &str) -> (Scratch, Dataset) {
    let root = std::env::temp_dir().join(format!("memharness-live-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let (dir, _) = write_dataset(&root, Scale::Tiny, 1).expect("datagen");
    (Scratch(root), Dataset::open(&dir).expect("open"))
}

fn quiet() -> LoadOptions {
    LoadOptions {
        concurrency: 8,
        progress_every: None,
    }
}

async fn bolt() -> Option<Resilient<BoltBackend>> {
    let Ok(url) = std::env::var("BAID_TEST_NEO4J_URL") else {
        eprintln!("skipped: set BAID_TEST_NEO4J_URL (see `just test-live`)");
        return None;
    };
    let password =
        std::env::var("BAID_TEST_NEO4J_PASSWORD").unwrap_or_else(|_| "baid-dev-password".into());
    let b = BoltBackend::connect(BoltConfig {
        url,
        user: Some("neo4j".into()),
        password: Some(password),
        max_connections: 12,
        call_timeout: Duration::from_secs(10),
    })
    .await
    .expect("connect");
    Some(Resilient::with_defaults(b, Duration::from_secs(30)))
}

fn s3() -> Option<Resilient<S3Backend>> {
    let Ok(url) = std::env::var("BAID_TEST_S3_URL") else {
        eprintln!("skipped: set BAID_TEST_S3_URL (see `just test-live`)");
        return None;
    };
    let b = S3Backend::connect(S3Config {
        url,
        access_key: Some(std::env::var("BAID_TEST_S3_KEY").unwrap_or_else(|_| "baid".into())),
        secret_key: Some(
            std::env::var("BAID_TEST_S3_SECRET").unwrap_or_else(|_| "baid-dev-password".into()),
        ),
        call_timeout: Duration::from_secs(10),
        scan_limit: 1_000,
    })
    .expect("s3");
    Some(Resilient::with_defaults(b, Duration::from_secs(30)))
}

async fn load_twice_and_verify<B: Backend>(b: &B, ds: &Dataset, full_text: bool) {
    let first = load(b, ds, &quiet(), "first").await.expect("load");
    let second = load(b, ds, &quiet(), "second").await.expect("load");
    assert_eq!(first.written, 200);
    assert_eq!(first.acks, second.acks, "a re-load stored new memories");
    let r = verify(
        b,
        ds,
        &VerifyOptions {
            samples_per_tenant: 40,
            full_text,
            ..VerifyOptions::default()
        },
    )
    .await
    .expect("verify");
    eprintln!("{r}");
    assert!(r.ok(), "verification failed:\n{r}");
}

#[tokio::test]
async fn loads_into_neo4j() {
    let Some(b) = bolt().await else { return };
    let (_tmp, ds) = tiny("bolt");
    load_twice_and_verify(&b, &ds, true).await;
}

#[tokio::test]
async fn loads_into_s3() {
    let Some(b) = s3() else { return };
    let (_tmp, ds) = tiny("s3");
    load_twice_and_verify(&b, &ds, false).await;
}

/// Unique words are letters only, so Neo4j's full-text analyser keeps each one as a single
/// token: an unfiltered search for one returns exactly its memory.
#[tokio::test]
async fn a_unique_word_search_on_neo4j_returns_exactly_one_memory() {
    let Some(b) = bolt().await else { return };
    let (_tmp, ds) = tiny("bolt-words");
    load(&b, &ds, &quiet(), "words").await.expect("load");
    let rows: Vec<Row> = ds
        .rows()
        .expect("rows")
        .map(|r| r.expect("row"))
        .filter(|r| r.n % 5 == 0)
        .collect();
    assert_eq!(rows.len(), 40);
    for row in rows {
        let q = BackendQuery::Search {
            text: row.unique.clone(),
            filters: Filters::default(),
            limit: 10,
        };
        let res = b
            .query_batch(&row.tenant, std::slice::from_ref(&q))
            .await
            .expect("search");
        let items = &res[0].items;
        assert_eq!(items.len(), 1, "{}: {items:?}", row.unique);
        assert_eq!(unique_in_text(&items[0].text), Some(row.n));
        assert_eq!(&*items[0].text, row.text.as_str());
    }
}
