//! The generator's output: pinned, deterministic, integer-only, and shaped as documented.

use std::collections::{BTreeMap, BTreeSet};

use memharness::datagen::{
    decode_unique, generate, unique_word, vocabulary, write_rows, Scale, GENERATOR_VERSION, TEAMS,
    TOPICS,
};
use memharness::dataset::Row;

/// blake3 of `tiny` with seed 1 (`tiny-s1-v1`). Runs on every platform in CI (`cargo test --workspace` on
/// Linux) and locally on Windows, so a platform difference in generation fails at once.
/// If the generator changes on purpose, bump `GENERATOR_VERSION` and update this.
const TINY_S1_BLAKE3: &str = "8928efc9da1a86fc5ac1f5340184369f1b9488c4241c08d3760245c05250fea3";
const TINY_S1_BYTES: u64 = 51_793;

fn bytes(scale: Scale, seed: u64) -> Vec<u8> {
    let mut out = Vec::new();
    write_rows(scale, seed, &mut out).expect("generate");
    out
}

fn rows(scale: Scale, seed: u64) -> Vec<Row> {
    let mut v = Vec::new();
    generate(scale, seed, |r| {
        v.push(r);
        Ok(())
    })
    .expect("generate");
    v
}

#[test]
fn the_tiny_dataset_is_pinned() {
    let mut out = Vec::new();
    let m = write_rows(Scale::Tiny, 1, &mut out).expect("generate");
    assert_eq!(GENERATOR_VERSION, 1);
    assert_eq!(
        (m.blake3.as_str(), m.bytes),
        (TINY_S1_BLAKE3, TINY_S1_BYTES),
        "tiny-s1 changed: if on purpose, bump GENERATOR_VERSION and the pinned hash"
    );
    assert_eq!(blake3::hash(&out).to_hex().as_str(), m.blake3);
    assert_eq!(out.len() as u64, m.bytes);
    assert!(!out.contains(&b'\r'), "rows must end in \\n only");
}

#[test]
fn same_seed_same_bytes_other_seed_other_bytes() {
    assert_eq!(bytes(Scale::Small, 42), bytes(Scale::Small, 42));
    // The seed must drive the content, not only the dataset and tenant names.
    let texts = |seed| -> Vec<String> {
        rows(Scale::Tiny, seed)
            .into_iter()
            .map(|r| r.text)
            .collect()
    };
    let (a, b) = (texts(1), texts(2));
    let same = a.iter().zip(&b).filter(|(x, y)| x == y).count();
    assert!(
        same < a.len() / 10,
        "{same} of {} texts identical across seeds",
        a.len()
    );
}

/// G27: no floating point anywhere in generation. libm results differ across platforms.
#[test]
fn generation_uses_integer_maths_only() {
    for (file, src) in [
        ("rng.rs", include_str!("../src/rng.rs")),
        ("datagen.rs", include_str!("../src/datagen.rs")),
    ] {
        for (i, line) in src.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            for banned in [
                "f32", "f64", "powf", "powi", ".ln(", ".exp(", "sqrt", "_f64()",
            ] {
                assert!(
                    !code.contains(banned),
                    "{file}:{}: `{banned}` in generation code: {line}",
                    i + 1
                );
            }
        }
    }
}

#[test]
fn unique_words_round_trip_and_are_letters_only() {
    for n in [0, 1, 25, 26, 675, 676, 2_000_000, 26u64.pow(6) - 1] {
        let w = unique_word(n);
        assert_eq!(w.len(), 8);
        assert!(w.bytes().all(|b| b.is_ascii_lowercase()), "{w}");
        assert_eq!(decode_unique(&w), Some(n));
    }
    assert_eq!(decode_unique("qzaaaa"), None);
    assert_eq!(decode_unique("qzaaaaa1"), None);
    assert_eq!(decode_unique("renewal"), None);
}

#[test]
fn the_vocabulary_never_looks_like_a_unique_word() {
    for w in vocabulary().chain(TEAMS).chain(TOPICS) {
        assert!(
            w.bytes().all(|b| b.is_ascii_lowercase() || b == b' '),
            "{w:?}"
        );
        assert!(!w.contains("qz"), "{w:?}");
    }
}

#[test]
fn rows_have_the_documented_shape() {
    let shape = Scale::Small.shape();
    let rows = rows(Scale::Small, 42);
    assert_eq!(rows.len() as u64, shape.memories * u64::from(shape.tenants));

    let mut degree: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut tokens: BTreeMap<String, u64> = BTreeMap::new();
    let mut last_as_of = BTreeMap::new();
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(r.n, i as u64, "rows are numbered in file order");
        let t = i as u64 / shape.memories;
        assert_eq!(r.tenant.as_str(), format!("small-s42-v1-{t}"));

        // 1-4 distinct entities, the account first.
        assert!((1..=4).contains(&r.entities.len()), "{r:?}");
        assert!(r.entities[0].as_str().starts_with("account:"), "{r:?}");
        let distinct: BTreeSet<_> = r.entities.iter().collect();
        assert_eq!(distinct.len(), r.entities.len(), "{r:?}");
        for e in &r.entities {
            *degree
                .entry((r.tenant.to_string(), e.to_string()))
                .or_default() += 1;
        }

        assert_eq!(r.labels.len(), 1);
        assert_eq!(decode_unique(&r.unique), Some(r.n));
        assert!(r.text.ends_with(&format!(" Ref {}.", r.unique)), "{r:?}");
        for tok in r.text.split(|c: char| !c.is_ascii_alphanumeric()) {
            if !tok.is_empty() {
                *tokens.entry(tok.to_ascii_lowercase()).or_default() += 1;
            }
        }

        // as_of never goes back within a tenant.
        if let Some(prev) = last_as_of.insert(r.tenant.clone(), r.as_of) {
            assert!(prev <= r.as_of, "{r:?}");
        }
    }

    // Each unique word is one token that occurs exactly once in the whole dataset.
    for r in &rows {
        assert_eq!(tokens.get(&r.unique), Some(&1), "{}", r.unique);
    }

    // Hubs: the most mentioned entity per tenant is far above the typical one.
    for t in 0..shape.tenants {
        let tenant = format!("small-s42-v1-{t}");
        let mut d: Vec<u64> = degree
            .iter()
            .filter(|((tt, _), _)| *tt == tenant)
            .map(|(_, n)| *n)
            .collect();
        d.sort_unstable();
        let median = d[d.len() / 2];
        let max = *d.last().expect("entities");
        assert!(max >= 20 * median, "max degree {max}, median {median}");
    }
}

#[test]
fn tenants_share_entity_names() {
    // Same names in every tenant, so verification can detect a leak between them.
    let rows = rows(Scale::Tiny, 1);
    let per_tenant = |t: &str| -> BTreeSet<String> {
        rows.iter()
            .filter(|r| r.tenant.as_str() == t)
            .flat_map(|r| r.entities.iter().map(|e| e.to_string()))
            .filter(|e| e.starts_with("account:"))
            .collect()
    };
    let shared = per_tenant("tiny-s1-v1-0")
        .intersection(&per_tenant("tiny-s1-v1-1"))
        .count();
    assert!(shared >= 5, "{shared} shared accounts");
}
