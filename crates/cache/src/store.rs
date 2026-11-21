//! [`RamStore`]: the RAM tier behind the [`CacheStore`] trait.
//!
//! # Staying correct under concurrent writes
//!
//! A writer does: backend ack → `mark_written` (stamps entities with a new write sequence
//! number) → optionally evicts dependent keys.
//! A loader does: read `write_seq` → backend call → `put` with `loaded_at` set to that
//! number.
//!
//! An entry is valid only while none of its entities (query-named or result-mentioned)
//! carries a stamp newer than `loaded_at` (see [`WriteLog`]). Two checks enforce it:
//!
//! - **On put**: check, insert, then check again and remove the entry if a stamp moved in
//!   between. Without the second check, `check → write stamps → insert` would store stale
//!   data.
//! - **On get**: every hit re-checks. So even an entry that slipped in, or one the
//!   invalidator never heard about, is never served after a write that touched it.
//!
//! Removing in the put's second check may also drop a fresher entry that another loader
//! put for the same key in between. That costs a miss, never a wrong answer.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use baid_core::{
    CacheEntry, CacheStore, Config, EntityId, FlushScope, Key, PutOutcome, TenantId, WriteSeq,
};
use dashmap::DashMap;

use crate::clock::{Clock, SystemClock};
use crate::tier::{Tier, TierConfig, TierStats};
use crate::versions::WriteLog;

/// Test-only hook run between the first staleness check and the insert.
#[cfg(test)]
type Hook = Arc<dyn Fn(&RamStore) + Send + Sync>;

/// The RAM cache tier for every tenant, plus the write log.
pub struct RamStore {
    tiers: DashMap<TenantId, Arc<Tier>>,
    writes: WriteLog,
    clock: Arc<dyn Clock>,
    #[cfg(test)]
    before_insert: std::sync::Mutex<Option<Hook>>,
}

impl RamStore {
    /// An empty store with no tenants.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        RamStore {
            tiers: DashMap::new(),
            writes: WriteLog::new(),
            clock,
            #[cfg(test)]
            before_insert: std::sync::Mutex::new(None),
        }
    }

    /// A store with one tier per configured tenant, sized by `tenants[].ram_bytes`.
    pub fn from_config(config: &Config) -> Self {
        let store = Self::new(Arc::new(SystemClock));
        for t in &config.tenants {
            let bytes = usize::try_from(t.ram_bytes.bytes()).unwrap_or(usize::MAX);
            store.add_tenant(t.id.clone(), TierConfig::with_capacity(bytes));
        }
        store
    }

    /// Creates (or replaces, dropping its entries) the tier for `tenant`.
    pub fn add_tenant(&self, tenant: TenantId, config: TierConfig) {
        self.tiers.insert(tenant, Arc::new(Tier::new(config)));
    }

    /// Drops a tenant's tier and all its entries. Returns true if it existed.
    pub fn remove_tenant(&self, tenant: &TenantId) -> bool {
        self.tiers.remove(tenant).is_some()
    }

    /// Counters for one tenant's tier.
    pub fn stats(&self, tenant: &TenantId) -> Option<TierStats> {
        self.tier(tenant).map(|t| t.stats())
    }

    /// The write log.
    pub fn writes(&self) -> &WriteLog {
        &self.writes
    }

    /// Clones the tier's `Arc` out, so no DashMap shard lock is held while it is used.
    fn tier(&self, tenant: &TenantId) -> Option<Arc<Tier>> {
        self.tiers.get(tenant).map(|t| Arc::clone(t.value()))
    }

    /// The entry under `key` if no write invalidated it and it expired less than `grace`
    /// ago. The write-log check is the same for `get` and `get_stale`: only the TTL bound
    /// differs.
    fn current(&self, key: &Key, grace: Duration) -> Option<Arc<CacheEntry>> {
        let tier = self.tier(key.tenant())?;
        let entry = Arc::clone(tier.cache.get(key)?.value());
        if !self.is_current(key.tenant(), &entry) {
            // Written since it was loaded: never serve it, and free the space.
            tier.cache.remove(key);
            return None;
        }
        let until = entry
            .expires_at
            .checked_add(grace)
            .unwrap_or(entry.expires_at);
        (self.clock.now() < until).then_some(entry)
    }

    fn is_current(&self, tenant: &TenantId, entry: &CacheEntry) -> bool {
        self.writes
            .is_current(tenant, entry.entities(), entry.loaded_at)
    }

    /// The single insert path, used by `put` and `promote`. See the module docs.
    fn guarded_insert(&self, key: Key, entry: Arc<CacheEntry>) -> PutOutcome {
        let tenant = key.tenant().clone();
        let Some(tier) = self.tier(&tenant) else {
            return PutOutcome::UnknownTenant;
        };
        if entry.approx_bytes as usize > tier.config.max_entry_bytes() {
            return PutOutcome::TooLarge;
        }
        if !self.is_current(&tenant, &entry) {
            return PutOutcome::StaleVersion;
        }
        #[cfg(test)]
        self.run_hook();

        tier.insert(key.clone(), Arc::clone(&entry));

        if !self.is_current(&tenant, &entry) {
            tier.cache.remove(&key);
            return PutOutcome::StaleVersion;
        }
        PutOutcome::Stored
    }

    #[cfg(test)]
    fn run_hook(&self) {
        let hook = self
            .before_insert
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(h) = hook {
            h(self);
        }
    }

    #[cfg(test)]
    fn set_hook(&self, hook: Option<Hook>) {
        *self
            .before_insert
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = hook;
    }
}

#[async_trait]
impl CacheStore for RamStore {
    async fn get(&self, key: &Key) -> Option<Arc<CacheEntry>> {
        // Expired entries stay in place (LRU evicts them) so stale-if-error can still
        // reach them; they are just never returned here.
        self.current(key, Duration::ZERO)
    }

    async fn get_stale(&self, key: &Key, grace: Duration) -> Option<Arc<CacheEntry>> {
        self.current(key, grace)
    }

    fn contains(&self, key: &Key) -> bool {
        // foyer's `contains` does not touch LRU order, so checking before a prefetch does
        // not keep entries artificially hot.
        self.tier(key.tenant())
            .is_some_and(|t| t.cache.contains(key))
    }

    async fn put(&self, key: Key, entry: CacheEntry) -> PutOutcome {
        self.guarded_insert(key, Arc::new(entry))
    }

    fn promote(&self, key: &Key) -> bool {
        let Some(tier) = self.tier(key.tenant()) else {
            return false;
        };
        let Some(current) = tier.cache.get(key) else {
            return false;
        };
        if !current.value().speculative {
            return false;
        }
        let promoted = Arc::new(current.value().promoted());
        drop(current);
        // Through the guarded path: if a write invalidated the entry since it was loaded,
        // the promotion is refused instead of resurrecting it.
        self.guarded_insert(key.clone(), promoted) == PutOutcome::Stored
    }

    fn remove(&self, key: &Key) -> bool {
        self.tier(key.tenant())
            .is_some_and(|t| t.cache.remove(key).is_some())
    }

    async fn flush(&self, scope: FlushScope) -> usize {
        let clear = |tier: &Tier| {
            let n = tier.stats().entries;
            tier.cache.clear();
            usize::try_from(n).unwrap_or(usize::MAX)
        };
        match scope {
            FlushScope::All => {
                let tiers: Vec<Arc<Tier>> =
                    self.tiers.iter().map(|t| Arc::clone(t.value())).collect();
                tiers.iter().map(|t| clear(t)).sum()
            }
            FlushScope::Tenant(t) => self.tier(&t).map_or(0, |t| clear(&t)),
            FlushScope::Key(k) => usize::from(self.remove(&k)),
        }
    }

    fn write_seq(&self, tenant: &TenantId) -> WriteSeq {
        self.writes.seq(tenant)
    }

    fn mark_written(&self, tenant: &TenantId, entities: &[EntityId]) -> WriteSeq {
        self.writes.mark(tenant, entities)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use baid_core::{AdapterId, BackendQuery, LoadInfo, MemoryItem, NodeId, Priority};
    use futures::executor::block_on;

    use super::*;
    use crate::clock::ManualClock;

    fn t(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }
    fn e(s: &str) -> EntityId {
        EntityId::new(s).unwrap()
    }
    fn adapter() -> AdapterId {
        AdapterId::new("mock").unwrap()
    }
    fn key(tenant: &str, entity: &str) -> Key {
        Key::derive(
            &t(tenant),
            &adapter(),
            &BackendQuery::Entity { id: e(entity) },
        )
    }
    fn item(text: &str, entities: &[&str]) -> MemoryItem {
        MemoryItem {
            id: NodeId::new(format!("n-{text}")).unwrap(),
            text: Arc::from(text),
            score: 1.0,
            tokens: 0,
            entities: entities.iter().map(|x| e(x)).collect(),
            as_of: None,
            source: adapter(),
        }
    }

    struct Fixture {
        store: Arc<RamStore>,
        clock: Arc<ManualClock>,
    }

    fn fixture(tenants: &[&str], capacity: usize) -> Fixture {
        let clock = Arc::new(ManualClock::new());
        let store = Arc::new(RamStore::new(clock.clone()));
        for tn in tenants {
            store.add_tenant(
                t(tn),
                TierConfig {
                    capacity_bytes: capacity,
                    shards: 1,
                    normal_pool_ratio: 0.9,
                },
            );
        }
        Fixture { store, clock }
    }

    impl Fixture {
        /// An entry built from a load that starts now: query names `query_entities`, the
        /// single result item mentions `item_entities`.
        fn load(
            &self,
            tenant: &str,
            query_entities: &[&str],
            item_entities: &[&str],
            text: &str,
            speculative: bool,
        ) -> CacheEntry {
            let q: Vec<EntityId> = query_entities.iter().map(|x| e(x)).collect();
            CacheEntry::new(
                vec![item(text, item_entities)],
                LoadInfo {
                    query_entities: &q,
                    loaded_at: self.store.write_seq(&t(tenant)),
                    source: adapter(),
                    ttl: Duration::from_secs(60),
                    speculative,
                },
                self.clock.now(),
            )
        }
        fn entry(&self, tenant: &str, entity: &str, text: &str, speculative: bool) -> CacheEntry {
            self.load(tenant, &[entity], &[entity], text, speculative)
        }
        fn text(&self, k: &Key) -> Option<String> {
            block_on(self.store.get(k)).map(|e| e.items[0].text.to_string())
        }
    }

    #[test]
    fn put_then_get() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "x");
        let out = block_on(f.store.put(k.clone(), f.entry("acme", "x", "hello", false)));
        assert_eq!(out, PutOutcome::Stored);
        assert_eq!(f.text(&k).as_deref(), Some("hello"));
        assert!(f.store.contains(&k));
        assert_eq!(f.store.stats(&t("acme")).unwrap().entries, 1);
    }

    #[test]
    fn unknown_tenant_is_refused() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("ghost", "x");
        let out = block_on(f.store.put(k.clone(), f.entry("ghost", "x", "boo", false)));
        assert_eq!(out, PutOutcome::UnknownTenant);
        assert_eq!(f.text(&k), None);
    }

    #[test]
    fn tenants_with_same_entity_do_not_share() {
        let f = fixture(&["a", "b"], 1 << 20);
        block_on(
            f.store
                .put(key("a", "x"), f.entry("a", "x", "a's data", false)),
        );
        assert_eq!(f.text(&key("b", "x")), None);
    }

    #[test]
    fn expired_entries_are_not_returned() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "x");
        block_on(f.store.put(k.clone(), f.entry("acme", "x", "hello", false)));
        f.clock.advance(Duration::from_secs(59));
        assert!(f.text(&k).is_some());
        f.clock.advance(Duration::from_secs(1));
        assert_eq!(f.text(&k), None);
    }

    #[test]
    fn get_stale_serves_expired_entries_within_the_grace_only() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "x");
        let stale = |k: &Key, grace: u64| {
            block_on(f.store.get_stale(k, Duration::from_secs(grace)))
                .map(|e| e.items[0].text.to_string())
        };
        block_on(f.store.put(k.clone(), f.entry("acme", "x", "hello", false)));
        assert_eq!(stale(&k, 0).as_deref(), Some("hello"), "fresh is fine");
        f.clock.advance(Duration::from_secs(60)); // ttl reached
        assert_eq!(f.text(&k), None, "get refuses it");
        assert_eq!(stale(&k, 0), None, "no grace, no answer");
        assert_eq!(stale(&k, 30).as_deref(), Some("hello"), "inside the window");
        f.clock.advance(Duration::from_secs(30));
        assert_eq!(stale(&k, 30), None, "the window is exclusive at its end");
        assert!(f.store.contains(&k), "expired entries are left for LRU");
    }

    /// Stale-if-error relaxes only the TTL: an entry a write invalidated is never served,
    /// however wide the window.
    #[test]
    fn get_stale_never_serves_invalidated_entries() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "q");
        let e1 = f.load("acme", &[], &["acme"], "acme renews in March", false);
        block_on(f.store.put(k.clone(), e1));
        f.clock.advance(Duration::from_secs(61));
        f.store.mark_written(&t("acme"), &[e("acme")]);
        assert!(block_on(f.store.get_stale(&k, Duration::from_secs(3_600))).is_none());
        assert!(!f.store.contains(&k), "and the space is freed");
    }

    #[test]
    fn put_after_a_write_is_rejected() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "x");
        let stale = f.entry("acme", "x", "old", false); // load started before the write
        f.store.mark_written(&t("acme"), &[e("x")]);
        assert_eq!(
            block_on(f.store.put(k.clone(), stale)),
            PutOutcome::StaleVersion
        );
        assert_eq!(f.text(&k), None);
    }

    /// The hole in the original design: the write touched an entity the query never named,
    /// which only showed up in the results.
    #[test]
    fn write_to_an_entity_seen_only_in_results_is_caught() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "q");
        let stale = f.load("acme", &[], &["acme"], "acme renews in March", false);
        f.store.mark_written(&t("acme"), &[e("acme")]);
        assert_eq!(
            block_on(f.store.put(k.clone(), stale)),
            PutOutcome::StaleVersion
        );
    }

    /// Even with no invalidator evicting anything, a hit is never served after a write
    /// to one of its entities.
    #[test]
    fn get_refuses_entries_written_since_load() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "q");
        let fresh = f.load("acme", &[], &["acme", "globex"], "both", false);
        assert_eq!(block_on(f.store.put(k.clone(), fresh)), PutOutcome::Stored);
        f.store.mark_written(&t("acme"), &[e("other")]);
        assert!(f.text(&k).is_some(), "unrelated write keeps it");
        f.store.mark_written(&t("acme"), &[e("globex")]);
        assert_eq!(f.text(&k), None, "write to a mentioned entity drops it");
        assert!(!f.store.contains(&k), "and frees the space");
    }

    /// The G1 race, forced deterministically: the write lands between the put's first
    /// check and its insert.
    #[test]
    fn write_between_check_and_insert_is_caught() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "x");
        let k2 = k.clone();
        f.store.set_hook(Some(Arc::new(move |s: &RamStore| {
            s.mark_written(&t("acme"), &[e("x")]);
            s.remove(&k2); // key not inserted yet: the writer's evict finds nothing
        })));
        let out = block_on(f.store.put(k.clone(), f.entry("acme", "x", "old", false)));
        f.store.set_hook(None);
        assert_eq!(out, PutOutcome::StaleVersion);
        assert!(!f.store.contains(&k), "pre-write data must not survive");
    }

    #[test]
    fn promote_marks_speculative_entries_used() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "x");
        block_on(f.store.put(k.clone(), f.entry("acme", "x", "guess", true)));
        assert!(block_on(f.store.get(&k)).unwrap().speculative);
        assert!(f.store.promote(&k));
        let got = block_on(f.store.get(&k)).unwrap();
        assert!(!got.speculative);
        assert_eq!(got.priority, Priority::Normal);
        assert!(!f.store.promote(&k), "already promoted");
        let s = f.store.stats(&t("acme")).unwrap();
        assert_eq!((s.speculative_wasted, s.entries), (0, 1));
    }

    #[test]
    fn promote_cannot_resurrect_invalidated_data() {
        let f = fixture(&["acme"], 1 << 20);
        let k = key("acme", "x");
        block_on(f.store.put(k.clone(), f.entry("acme", "x", "guess", true)));
        f.store.mark_written(&t("acme"), &[e("x")]);
        assert!(!f.store.promote(&k));
    }

    #[test]
    fn speculative_entries_are_evicted_first() {
        let probe = fixture(&["acme"], 1 << 20);
        let weight = probe.entry("acme", "e0", "payload", false).approx_bytes as usize + 200;
        // Room for about 6 entries; the per-entry limit is a quarter of the capacity.
        let f = fixture(&["acme"], weight * 6);
        let spec: Vec<Key> = (0..3).map(|i| key("acme", &format!("s{i}"))).collect();
        let norm: Vec<Key> = (0..5).map(|i| key("acme", &format!("n{i}"))).collect();
        for (i, k) in spec.iter().enumerate() {
            let entry = f.entry("acme", &format!("s{i}"), "payload", true);
            assert_eq!(block_on(f.store.put(k.clone(), entry)), PutOutcome::Stored);
        }
        for (i, k) in norm.iter().enumerate() {
            block_on(f.store.put(
                k.clone(),
                f.entry("acme", &format!("n{i}"), "payload", false),
            ));
        }
        assert!(
            norm.iter().all(|k| f.store.contains(k)),
            "normal entries kept"
        );
        let spec_left = spec.iter().filter(|k| f.store.contains(k)).count();
        assert!(spec_left < 3, "a speculative entry made room");
        let s = f.store.stats(&t("acme")).unwrap();
        assert_eq!(s.speculative_wasted, (3 - spec_left) as u64);
        assert!(s.usage_bytes <= s.capacity_bytes);
    }

    #[test]
    fn oversized_entries_are_refused() {
        let f = fixture(&["acme"], 4_000);
        let mut big = f.entry("acme", "x", "x", false);
        big.approx_bytes = 2_000; // limit is 4_000 / 1 shard / 4 = 1_000
        assert_eq!(
            block_on(f.store.put(key("acme", "x"), big)),
            PutOutcome::TooLarge
        );
    }

    #[test]
    fn flush_scopes() {
        let f = fixture(&["a", "b"], 1 << 20);
        for (tn, ent) in [("a", "x"), ("a", "y"), ("b", "x")] {
            block_on(f.store.put(key(tn, ent), f.entry(tn, ent, "v", false)));
        }
        assert_eq!(block_on(f.store.flush(FlushScope::Key(key("a", "y")))), 1);
        assert_eq!(block_on(f.store.flush(FlushScope::Tenant(t("a")))), 1);
        assert_eq!(f.text(&key("a", "x")), None);
        assert!(f.text(&key("b", "x")).is_some(), "other tenant untouched");
        assert_eq!(block_on(f.store.flush(FlushScope::All)), 1);
        assert_eq!(f.store.stats(&t("b")).unwrap().entries, 0);
    }

    /// Real threads racing a loader against a writer, many times over. Afterwards nothing
    /// cached may predate the write. Checked on the raw tier, bypassing `get`'s own
    /// re-check, so this exercises the put path alone.
    #[test]
    fn concurrent_put_and_write_never_store_stale_data() {
        let f = fixture(&["acme"], 1 << 22);
        let k = key("acme", "x");
        for round in 0..2_000 {
            let entry = f.entry("acme", "x", &format!("r{round}"), false);
            let (s1, s2, k1, k2) = (f.store.clone(), f.store.clone(), k.clone(), k.clone());
            let loader = std::thread::spawn(move || block_on(s1.put(k1, entry)));
            let writer = std::thread::spawn(move || {
                s2.mark_written(&t("acme"), &[e("x")]);
                s2.remove(&k2);
            });
            loader.join().unwrap();
            writer.join().unwrap();
            let tier = f.store.tier(&t("acme")).unwrap();
            if let Some(raw) = tier.cache.get(&k) {
                assert!(
                    f.store.is_current(&t("acme"), raw.value()),
                    "round {round}: stale entry survived a write"
                );
            }
        }
    }
}
