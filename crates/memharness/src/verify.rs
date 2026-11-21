//! `memharness load --verify`: checks that a backend returns what the dataset says.
//!
//! For each tenant it samples rows, and one entity from each sampled row, then asks the
//! backend through the normal [`Backend`] queries:
//!
//! - **Neighbourhood** (1 hop) of each sampled entity: exactly `min(limit, expected)` items,
//!   no duplicates, and every item a memory of *this* tenant that mentions the entity.
//! - **Entity** query: one item, a memory of this tenant that mentions the entity.
//! - **Unique-word search, filtered** by the row's rarest entity: exactly one item, the row
//!   itself (same text, same entities). Works on every backend, S3 included.
//! - **Unique-word search, full text** (backends with a text index: Neo4j, memory): no
//!   filters, still exactly the row. This is what proves the word is one token to Neo4j's
//!   analyser and that nothing was stored twice.
//! - **Cross-tenant**: the same filtered search in another tenant returns nothing.
//!
//! Items are matched to rows through the unique word at the end of their text. Backend
//! errors are recorded as failures, not raised, so one report shows everything.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use baid_core::{Backend, BackendQuery, EntityId, Filters, MemoryItem, TenantId};

use crate::datagen::unique_in_text;
use crate::dataset::{Dataset, DatasetError, Row};
use crate::rng::SplitMix64;

/// How to verify.
#[derive(Clone, Debug)]
pub struct VerifyOptions {
    /// Rows sampled per tenant (one entity is sampled from each).
    pub samples_per_tenant: usize,
    /// Seed of the sampling.
    pub seed: u64,
    /// Also run unfiltered unique-word searches (needs a text index: not S3).
    pub full_text: bool,
    /// `limit` of the neighbourhood queries.
    pub limit: u16,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        VerifyOptions {
            samples_per_tenant: 50,
            seed: 1,
            full_text: true,
            limit: 100,
        }
    }
}

/// What was checked and what failed.
#[derive(Clone, Debug, Default)]
pub struct VerifyReport {
    /// Tenants checked.
    pub tenants: usize,
    /// Rows sampled.
    pub rows_sampled: usize,
    /// Distinct `(tenant, entity)` pairs sampled.
    pub entities_sampled: usize,
    /// Neighbourhood queries.
    pub neighbourhood_checks: usize,
    /// Entity queries.
    pub entity_checks: usize,
    /// Unique-word searches filtered by an entity.
    pub filtered_searches: usize,
    /// Unique-word searches with no filter.
    pub full_text_searches: usize,
    /// Searches in the wrong tenant (must find nothing).
    pub cross_tenant_searches: usize,
    /// Items returned and checked against the dataset.
    pub items_checked: usize,
    /// One line per failed check.
    pub failures: Vec<String>,
}

impl VerifyReport {
    /// True when every check passed.
    pub fn ok(&self) -> bool {
        self.failures.is_empty()
    }
}

impl fmt::Display for VerifyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "  sampled {} rows and {} distinct entities across {} tenants",
            self.rows_sampled, self.entities_sampled, self.tenants
        )?;
        writeln!(
            f,
            "  {} neighbourhood queries, {} entity queries: right count, right tenant, \
             every item mentions the entity, no duplicates",
            self.neighbourhood_checks, self.entity_checks
        )?;
        writeln!(
            f,
            "  {} unique-word searches filtered by entity, {} unfiltered (full text): \
             exactly the one row, same text and entities",
            self.filtered_searches, self.full_text_searches
        )?;
        writeln!(
            f,
            "  {} searches in another tenant: nothing found",
            self.cross_tenant_searches
        )?;
        write!(
            f,
            "  {} items checked, {} failures",
            self.items_checked,
            self.failures.len()
        )?;
        for line in self.failures.iter().take(20) {
            write!(f, "\n    FAIL {line}")?;
        }
        if self.failures.len() > 20 {
            write!(f, "\n    ... and {} more", self.failures.len() - 20)?;
        }
        Ok(())
    }
}

/// Runs the checks described in the module docs.
pub async fn verify<B: Backend + ?Sized>(
    backend: &B,
    ds: &Dataset,
    opts: &VerifyOptions,
) -> Result<VerifyReport, DatasetError> {
    let m = ds.manifest();
    let per = m.memories_per_tenant;
    let mut rng = SplitMix64::new(opts.seed);

    // Rows to sample, per tenant.
    let mut wanted: BTreeSet<u64> = BTreeSet::new();
    for t in 0..m.tenants.len() as u64 {
        let k = opts.samples_per_tenant.min(per as usize);
        while wanted.range(t * per..(t + 1) * per).count() < k {
            wanted.insert(t * per + rng.below(per));
        }
    }

    // Pass 1: the sampled rows.
    let mut sampled: Vec<Row> = Vec::new();
    for row in ds.rows()? {
        let row = row?;
        if wanted.contains(&row.n) {
            sampled.push(row);
        }
    }

    // Entities to check: one per sampled row, plus every entity of the sampled rows (to
    // find each row's rarest entity for the filtered search).
    let mut checked: BTreeSet<(TenantId, EntityId)> = BTreeSet::new();
    let mut counted: BTreeMap<(TenantId, EntityId), BTreeSet<u64>> = BTreeMap::new();
    for row in &sampled {
        let e = rng.pick(&row.entities).clone();
        checked.insert((row.tenant.clone(), e));
        for e in &row.entities {
            counted.insert((row.tenant.clone(), e.clone()), BTreeSet::new());
        }
    }

    // Pass 2: which rows mention each of those entities.
    for row in ds.rows()? {
        let row = row?;
        for e in &row.entities {
            if let Some(set) = counted.get_mut(&(row.tenant.clone(), e.clone())) {
                set.insert(row.n);
            }
        }
    }

    let mut r = VerifyReport {
        tenants: m.tenants.len(),
        rows_sampled: sampled.len(),
        entities_sampled: checked.len(),
        ..VerifyReport::default()
    };
    let empty = BTreeSet::new();

    for (tenant, entity) in &checked {
        let expected = counted
            .get(&(tenant.clone(), entity.clone()))
            .unwrap_or(&empty);
        let what = format!("{tenant} neighbourhood({entity})");
        r.neighbourhood_checks += 1;
        let q = BackendQuery::Neighbourhood {
            entity: entity.clone(),
            hops: 1,
            limit: opts.limit,
        };
        if let Some(items) = run(backend, tenant, q, &what, &mut r).await {
            let want = expected.len().min(usize::from(opts.limit));
            if items.len() != want {
                r.failures
                    .push(format!("{what}: {} items, expected {want}", items.len()));
            }
            check_items(&items, expected, tenant, &what, ds, &mut r);
        }

        let what = format!("{tenant} entity({entity})");
        r.entity_checks += 1;
        let q = BackendQuery::Entity { id: entity.clone() };
        if let Some(items) = run(backend, tenant, q, &what, &mut r).await {
            if items.len() != 1 {
                r.failures
                    .push(format!("{what}: {} items, expected 1", items.len()));
            }
            check_items(&items, expected, tenant, &what, ds, &mut r);
        }
    }

    for row in &sampled {
        // The rarest entity keeps S3's per-entity listing small.
        let rarest = row
            .entities
            .iter()
            .min_by_key(|e| {
                counted
                    .get(&(row.tenant.clone(), (*e).clone()))
                    .map_or(usize::MAX, BTreeSet::len)
            })
            .cloned()
            .unwrap_or_else(|| row.entities[0].clone());
        let filtered = |limit: u16, by: Option<EntityId>| BackendQuery::Search {
            text: row.unique.clone(),
            filters: Filters {
                entity_ids: by.into_iter().collect(),
                ..Filters::default()
            },
            limit,
        };

        let what = format!("{} search({}, entity {rarest})", row.tenant, row.unique);
        r.filtered_searches += 1;
        if let Some(items) = run(
            backend,
            &row.tenant,
            filtered(10, Some(rarest.clone())),
            &what,
            &mut r,
        )
        .await
        {
            check_exact(&items, row, &what, &mut r);
        }

        if opts.full_text {
            let what = format!("{} search({})", row.tenant, row.unique);
            r.full_text_searches += 1;
            if let Some(items) = run(backend, &row.tenant, filtered(10, None), &what, &mut r).await
            {
                check_exact(&items, row, &what, &mut r);
            }
        }

        if let Some(other) = m.tenants.iter().find(|t| **t != row.tenant) {
            let what = format!("{other} search({}) for a {} row", row.unique, row.tenant);
            r.cross_tenant_searches += 1;
            if let Some(items) =
                run(backend, other, filtered(10, Some(rarest)), &what, &mut r).await
            {
                if !items.is_empty() {
                    r.failures.push(format!(
                        "{what}: found {} items in the wrong tenant",
                        items.len()
                    ));
                }
            }
        }
    }
    Ok(r)
}

async fn run<B: Backend + ?Sized>(
    backend: &B,
    tenant: &TenantId,
    q: BackendQuery,
    what: &str,
    r: &mut VerifyReport,
) -> Option<Vec<MemoryItem>> {
    match backend.query_batch(tenant, std::slice::from_ref(&q)).await {
        Ok(mut results) if results.len() == 1 => results.pop().map(|res| res.items),
        Ok(results) => {
            r.failures
                .push(format!("{what}: {} results for 1 query", results.len()));
            None
        }
        Err(e) => {
            r.failures.push(format!("{what}: backend error: {e}"));
            None
        }
    }
}

/// Every item is a distinct row of `tenant` that is in `expected`.
fn check_items(
    items: &[MemoryItem],
    expected: &BTreeSet<u64>,
    tenant: &TenantId,
    what: &str,
    ds: &Dataset,
    r: &mut VerifyReport,
) {
    let mut seen = BTreeSet::new();
    for item in items {
        r.items_checked += 1;
        let Some(n) = unique_in_text(&item.text) else {
            r.failures
                .push(format!("{what}: item {} is not from the dataset", item.id));
            continue;
        };
        if !seen.insert(n) {
            r.failures.push(format!("{what}: row {n} returned twice"));
        }
        match ds.manifest().tenant_of(n) {
            Some(t) if t == tenant => {}
            Some(t) => {
                r.failures
                    .push(format!("{what}: row {n} belongs to tenant {t}"));
                continue;
            }
            None => {
                r.failures
                    .push(format!("{what}: row {n} is outside the dataset"));
                continue;
            }
        }
        if !expected.contains(&n) {
            r.failures
                .push(format!("{what}: row {n} does not mention the entity"));
        }
    }
}

/// Exactly one item, and it is `row`.
fn check_exact(items: &[MemoryItem], row: &Row, what: &str, r: &mut VerifyReport) {
    r.items_checked += items.len();
    if items.len() != 1 {
        r.failures
            .push(format!("{what}: {} items, expected exactly 1", items.len()));
    }
    let Some(item) = items.first() else { return };
    if unique_in_text(&item.text) != Some(row.n) || &*item.text != row.text.as_str() {
        r.failures.push(format!(
            "{what}: returned a different memory: {:?}",
            item.text
        ));
        return;
    }
    let got: BTreeSet<&EntityId> = item.entities.iter().collect();
    let want: BTreeSet<&EntityId> = row.entities.iter().collect();
    if got != want {
        r.failures.push(format!(
            "{what}: entities {:?}, expected {:?}",
            got.iter().map(|e| e.as_str()).collect::<Vec<_>>(),
            want.iter().map(|e| e.as_str()).collect::<Vec<_>>()
        ));
    }
}
