//! Fail open and stale-if-error (plan T17; ADR-009, G23, G24, G25).
//!
//! A cache store that panics on demand ([`Faulty`]), a packer that panics on demand and a
//! backend that can hold, fail or panic ([`TestBackend`]) drive the real engine through every
//! failure case. Each guard here was also removed by hand to see its test fail.

#![allow(clippy::unwrap_used)] // test file

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adapters::{BreakerPolicy, Registry, Resilient, RetryPolicy};
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use baid_core::{
    AdapterId, Backend, BackendError, BackendQuery, BackendResult, CacheEntry, CacheOutcome,
    CacheStore, Capabilities, Config, EntityId, Fact, Filters, FlushScope, Health, Key,
    MemoryBackend, MemoryItem, PackedContext, Packer, PutOutcome, RecallRequest, SessionId,
    TenantId, TokenBudget, WriteAck, WriteSeq,
};
use cache::{ManualClock, RamStore, TierConfig};
use pack::TruncatingPacker;
use serde_json::{json, Value};
use server::auth::Tenant;
use server::config::resolve_secrets;
use server::engine::{DegradedReason, Engine, Recall};
use server::failopen::{Component, ComponentPolicy, Counters};
use server::http::error::ApiError;
use server::http::{admin_router, public_router};
use server::run::supervise;
use server::state::State;
use session::Sessions;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

// ---------------------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------------------

/// Which `Faulty` methods panic.
#[derive(Default)]
struct Flags {
    get: AtomicBool,
    get_stale: AtomicBool,
    write_seq: AtomicBool,
    put: AtomicBool,
    mark_written: AtomicBool,
    flush: AtomicBool,
    remove: AtomicBool,
    contains: AtomicBool,
}

type PutHook = Box<dyn FnOnce(&RamStore) + Send>;

/// A `RamStore` whose methods panic while their flag is set. A panicking method panics
/// *before* touching the store, like a bug on entry.
struct Faulty {
    inner: RamStore,
    on: Flags,
    /// Runs once, inside the next `put`, before it reaches the store.
    put_hook: Mutex<Option<PutHook>>,
}

fn boom(flag: &AtomicBool, what: &str) {
    assert!(!flag.load(Ordering::SeqCst), "injected panic in {what}");
}

#[async_trait]
impl CacheStore for Faulty {
    async fn get(&self, key: &Key) -> Option<Arc<CacheEntry>> {
        boom(&self.on.get, "get");
        self.inner.get(key).await
    }
    async fn get_stale(&self, key: &Key, grace: Duration) -> Option<Arc<CacheEntry>> {
        boom(&self.on.get_stale, "get_stale");
        self.inner.get_stale(key, grace).await
    }
    fn contains(&self, key: &Key) -> bool {
        boom(&self.on.contains, "contains");
        self.inner.contains(key)
    }
    async fn put(&self, key: Key, entry: CacheEntry) -> PutOutcome {
        boom(&self.on.put, "put");
        let hook = self.put_hook.lock().unwrap().take();
        if let Some(h) = hook {
            h(&self.inner);
        }
        self.inner.put(key, entry).await
    }
    fn promote(&self, key: &Key) -> bool {
        self.inner.promote(key)
    }
    fn remove(&self, key: &Key) -> bool {
        boom(&self.on.remove, "remove");
        self.inner.remove(key)
    }
    async fn flush(&self, scope: FlushScope) -> usize {
        boom(&self.on.flush, "flush");
        self.inner.flush(scope).await
    }
    fn write_seq(&self, tenant: &TenantId) -> WriteSeq {
        boom(&self.on.write_seq, "write_seq");
        self.inner.write_seq(tenant)
    }
    fn mark_written(&self, tenant: &TenantId, entities: &[EntityId]) -> WriteSeq {
        boom(&self.on.mark_written, "mark_written");
        self.inner.mark_written(tenant, entities)
    }
}

/// The truncating packer, or a panic while `panic` is set.
#[derive(Default)]
struct FlakyPacker {
    panic: AtomicBool,
    calls: AtomicUsize,
}

impl Packer for FlakyPacker {
    fn pack(&self, items: &[MemoryItem], budget: TokenBudget) -> PackedContext {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic.load(Ordering::SeqCst), "injected packer panic");
        TruncatingPacker.pack(items, budget)
    }
}

/// An in-memory backend that reads at once, then optionally waits for `gate`, then fails
/// or panics as configured.
#[derive(Default)]
struct TestBackend {
    inner: MemoryBackend,
    /// Every call fails with this while set.
    fail: Mutex<Option<BackendError>>,
    panic_reads: AtomicBool,
    panic_writes: AtomicBool,
    panic_id: AtomicBool,
    gate: Mutex<Option<Arc<Notify>>>,
    reads: AtomicUsize,
}

impl TestBackend {
    fn down(&self, e: Option<BackendError>) {
        *self.fail.lock().unwrap() = e;
    }
    fn hold(&self) -> Arc<Notify> {
        let n = Arc::new(Notify::new());
        *self.gate.lock().unwrap() = Some(n.clone());
        n
    }
    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Backend for TestBackend {
    fn id(&self) -> AdapterId {
        assert!(!self.panic_id.load(Ordering::SeqCst), "injected id panic");
        self.inner.id()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn query_batch(
        &self,
        t: &TenantId,
        qs: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let answer = self.inner.query_now(t, qs);
        let gate = self.gate.lock().unwrap().take();
        if let Some(g) = gate {
            g.notified().await;
        }
        assert!(
            !self.panic_reads.load(Ordering::SeqCst),
            "injected adapter panic"
        );
        if let Some(e) = self.fail.lock().unwrap().clone() {
            return Err(e);
        }
        Ok(answer)
    }
    async fn write(&self, t: &TenantId, f: &Fact) -> Result<WriteAck, BackendError> {
        assert!(
            !self.panic_writes.load(Ordering::SeqCst),
            "injected adapter panic"
        );
        if let Some(e) = self.fail.lock().unwrap().clone() {
            return Err(e);
        }
        Ok(self.inner.write_now(t, f))
    }
    async fn health(&self) -> Health {
        Health::Up
    }
}

// ---------------------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------------------

const KEY: &str = "acme-key-0123456789";

fn tenant_config(stale_if_error: &str) -> Config {
    toml::from_str(&format!(
        "[[tenants]]\nid = \"acme\"\napi_key_env = \"K_ACME\"\nadapter = \"memory\"\nadapter_url = \"memory://\"\nttl = \"60s\"\nstale_if_error = \"{stale_if_error}\"\nbackend_timeout = \"2s\"\n"
    ))
    .unwrap()
}

struct Rig {
    engine: Arc<Engine>,
    store: Arc<Faulty>,
    backend: Arc<TestBackend>,
    packer: Arc<FlakyPacker>,
    clock: Arc<ManualClock>,
    t: Tenant,
    config: Config,
}

fn acme() -> TenantId {
    TenantId::new("acme").unwrap()
}

/// `wrap`: put the backend behind `Resilient` (threshold 1), so breaker state is visible.
fn rig_with(stale_if_error: &str, policy: ComponentPolicy, wrap: bool) -> Rig {
    let config = tenant_config(stale_if_error);
    let t = Tenant {
        id: config.tenants[0].id.clone(),
        config: Arc::new(config.tenants[0].clone()),
    };
    let clock = Arc::new(ManualClock::new());
    let inner = RamStore::new(clock.clone());
    inner.add_tenant(acme(), TierConfig::with_capacity(16 << 20));
    let store = Arc::new(Faulty {
        inner,
        on: Flags::default(),
        put_hook: Mutex::new(None),
    });
    let backend = Arc::new(TestBackend::default());
    let mut reg = Registry::new();
    if wrap {
        reg.insert_resilient(
            acme(),
            Arc::new(Resilient::new(
                SharedBackend(backend.clone()),
                RetryPolicy {
                    max_attempts: 1,
                    ..RetryPolicy::default()
                },
                BreakerPolicy {
                    failure_threshold: 1,
                    open_for: Duration::from_secs(60),
                },
                Duration::from_secs(2),
            )),
        );
    } else {
        reg.insert(acme(), backend.clone());
    }
    let packer = Arc::new(FlakyPacker::default());
    let engine = Engine::new(
        store.clone(),
        reg,
        packer.clone(),
        Arc::new(Sessions::new()),
    )
    .with_component_policy(policy);
    Rig {
        engine: Arc::new(engine),
        store,
        backend,
        packer,
        clock,
        t,
        config,
    }
}

fn rig() -> Rig {
    rig_with("30s", ComponentPolicy::default(), false)
}

/// Lets `Resilient` wrap the shared test backend.
struct SharedBackend(Arc<TestBackend>);

#[async_trait]
impl Backend for SharedBackend {
    fn id(&self) -> AdapterId {
        self.0.id()
    }
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }
    async fn query_batch(
        &self,
        t: &TenantId,
        qs: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        self.0.query_batch(t, qs).await
    }
    async fn write(&self, t: &TenantId, f: &Fact) -> Result<WriteAck, BackendError> {
        self.0.write(t, f).await
    }
    async fn health(&self) -> Health {
        Health::Up
    }
}

fn req(session: &str, query: &str) -> RecallRequest {
    RecallRequest {
        session: SessionId::new(session).unwrap(),
        query: query.into(),
        budget: TokenBudget::default(),
        filters: Filters::default(),
        max_staleness: None,
    }
}

fn fact(text: &str, entity: &str) -> Fact {
    Fact {
        text: text.into(),
        entities: vec![EntityId::new(entity).unwrap()],
        labels: vec![],
        as_of: None,
        idempotency_key: None,
    }
}

fn sid(s: &str) -> SessionId {
    SessionId::new(s).unwrap()
}

fn texts(r: &Recall) -> Vec<String> {
    let mut v: Vec<String> = r.packed.items.iter().map(|i| i.text.to_string()).collect();
    v.sort();
    v
}

impl Rig {
    async fn recall(&self, session: &str, query: &str) -> Result<Recall, ApiError> {
        self.engine.recall(&self.t, req(session, query)).await
    }
    async fn remember(&self, session: &str, text: &str, entity: &str) -> Result<(), ApiError> {
        self.engine
            .remember(&self.t, &sid(session), fact(text, entity))
            .await
            .map(|_| ())
    }
    fn counters(&self) -> &Counters {
        self.engine.fail_open().counters()
    }
    fn epoch(&self) -> u64 {
        self.engine.fail_open().epochs().get(&acme())
    }
    /// Seeds "acme v1" and caches it through a recall.
    async fn cached_v1(&self) {
        self.backend.inner.seed(&acme(), "acme v1", &["acme"]);
        let r = self.recall("s", "acme").await.unwrap();
        assert_eq!(r.outcome, CacheOutcome::Miss);
        assert_eq!(
            self.recall("s", "acme").await.unwrap().outcome,
            CacheOutcome::Hit
        );
    }
    /// Moves the cache's clock past the 60 s TTL, inside the 30 s window.
    fn expire(&self) {
        self.clock.advance(Duration::from_secs(70));
    }
}

async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------------------
// Fail open: each component
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn a_cache_read_panic_is_a_miss() {
    let r = rig();
    r.cached_v1().await;
    r.store.on.get.store(true, Ordering::SeqCst);
    let got = r.recall("s", "acme").await.unwrap();
    assert_eq!(
        (got.outcome, texts(&got)),
        (CacheOutcome::Miss, vec!["acme v1".into()])
    );
    assert_eq!(r.backend.reads(), 2, "fell through to the backend");
    assert!(r.counters().internal_errors(Component::Cache) >= 1);
}

#[tokio::test]
async fn a_write_seq_panic_answers_but_does_not_cache() {
    let r = rig();
    r.backend.inner.seed(&acme(), "acme v1", &["acme"]);
    r.store.on.write_seq.store(true, Ordering::SeqCst);
    assert_eq!(texts(&r.recall("s", "acme").await.unwrap()), ["acme v1"]);
    r.store.on.write_seq.store(false, Ordering::SeqCst);
    let again = r.recall("s", "acme").await.unwrap();
    assert_eq!(again.outcome, CacheOutcome::Miss, "nothing was cached");
    assert_eq!(r.backend.reads(), 2);
}

#[tokio::test]
async fn a_put_panic_still_answers_with_one_backend_call() {
    let r = rig();
    r.backend.inner.seed(&acme(), "acme v1", &["acme"]);
    r.store.on.put.store(true, Ordering::SeqCst);
    assert_eq!(texts(&r.recall("s", "acme").await.unwrap()), ["acme v1"]);
    assert_eq!(r.backend.reads(), 1, "no second backend call");
    assert_eq!(r.counters().internal_errors(Component::Cache), 1);
}

/// The index evicts through `remove`; when that panics the write still succeeds and the
/// write log still refuses the stale entry (ADR-011).
#[tokio::test]
async fn an_index_failure_never_serves_stale_data() {
    let r = rig();
    r.cached_v1().await;
    r.store.on.remove.store(true, Ordering::SeqCst);
    r.remember("w", "acme v2", "acme").await.unwrap();
    assert!(r.counters().internal_errors(Component::Index) >= 1);
    r.store.on.remove.store(false, Ordering::SeqCst);
    assert_eq!(
        texts(&r.recall("s", "acme").await.unwrap()),
        ["acme v1", "acme v2"]
    );

    r.store.on.contains.store(true, Ordering::SeqCst);
    let done = r
        .engine
        .housekeeping(server::engine::clock_now(), Duration::from_secs(60));
    assert_eq!(done.index_refs_pruned, 0, "prune failed quietly");
}

#[tokio::test]
async fn a_packer_panic_uses_the_fallback_within_budget() {
    let r = rig();
    for i in 0..50 {
        r.backend.inner.seed(
            &acme(),
            &format!("acme fact number {i} with some words"),
            &["acme"],
        );
    }
    r.packer.panic.store(true, Ordering::SeqCst);
    let mut q = req("s", "acme");
    q.budget = TokenBudget::new(64).unwrap();
    let got = r.engine.recall(&r.t, q).await.unwrap();
    assert!(!got.packed.items.is_empty());
    assert!(got.packed.tokens <= 64, "fallback respects the budget");
    assert_eq!(r.counters().internal_errors(Component::Packer), 1);
}

#[tokio::test]
async fn the_component_breaker_bypasses_a_packer_that_keeps_panicking() {
    let r = rig_with(
        "30s",
        ComponentPolicy {
            threshold: 3,
            window: Duration::from_secs(60),
            cooldown: Duration::from_millis(200),
        },
        false,
    );
    r.backend.inner.seed(&acme(), "acme v1", &["acme"]);
    r.packer.panic.store(true, Ordering::SeqCst);
    for _ in 0..6 {
        assert_eq!(texts(&r.recall("s", "acme").await.unwrap()), ["acme v1"]);
    }
    assert_eq!(
        r.packer.calls.load(Ordering::SeqCst),
        3,
        "not called once tripped"
    );
    assert_eq!(r.counters().trips(Component::Packer), 1);
    assert_eq!(r.engine.degraded().components, [Component::Packer]);

    r.packer.panic.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(250)).await;
    r.recall("s", "acme").await.unwrap();
    assert_eq!(
        r.packer.calls.load(Ordering::SeqCst),
        4,
        "probe after the cooldown"
    );
    assert!(r.engine.degraded().is_empty(), "a good probe closes it");
}

/// A panic inside a shared load reaches no waiter as a panic: leader and follower both get
/// `backend_unavailable`, and the backend was called once.
#[tokio::test]
async fn an_adapter_panic_in_a_shared_load_is_an_error_for_every_waiter() {
    let r = rig();
    let gate = r.backend.hold();
    r.backend.panic_reads.store(true, Ordering::SeqCst);
    let (a, b, ()) = tokio::join!(r.recall("s1", "acme"), r.recall("s2", "acme"), async {
        settle().await;
        gate.notify_one();
    });
    for res in [a, b] {
        assert_eq!(res.unwrap_err().code(), "backend_unavailable");
    }
    assert_eq!(r.backend.reads(), 1);
    assert_eq!(r.counters().internal_errors(Component::Adapter), 1);
}

/// A write whose adapter panicked may have been applied: treated as ambiguous (G15).
#[tokio::test]
async fn an_adapter_panic_in_a_write_is_an_ambiguous_failure() {
    let r = rig();
    r.cached_v1().await;
    r.backend.panic_writes.store(true, Ordering::SeqCst);
    let e = r.remember("w", "acme v2", "acme").await.unwrap_err();
    assert_eq!(e.code(), "backend_unavailable");
    let got = r.recall("s", "acme").await.unwrap();
    assert_eq!(got.outcome, CacheOutcome::Miss, "its entities were stamped");
}

// ---------------------------------------------------------------------------------------
// G24: a write record that fails puts the tenant into cache bypass
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn a_failed_write_record_flushes_and_never_serves_pre_write_data() {
    let r = rig();
    r.cached_v1().await;
    r.store.on.mark_written.store(true, Ordering::SeqCst);
    r.store.on.remove.store(true, Ordering::SeqCst); // the index cannot evict either
    r.remember("w", "acme v2", "acme").await.unwrap(); // the ack still comes back
    r.store.on.mark_written.store(false, Ordering::SeqCst);
    r.store.on.remove.store(false, Ordering::SeqCst);
    assert_eq!(r.counters().bypass_entered(), 1);
    assert_eq!(r.epoch(), 2, "flushed at once, so in and out of bypass");
    assert_eq!(
        texts(&r.recall("s", "acme").await.unwrap()),
        ["acme v1", "acme v2"],
        "the pre-write entry is gone"
    );
}

#[tokio::test]
async fn a_failing_flush_keeps_the_tenant_in_bypass_and_visible() {
    let r = rig();
    r.cached_v1().await;
    r.store.on.mark_written.store(true, Ordering::SeqCst);
    r.store.on.flush.store(true, Ordering::SeqCst);
    r.store.on.remove.store(true, Ordering::SeqCst); // the index cannot evict either
    r.remember("w", "acme v2", "acme").await.unwrap();
    r.store.on.remove.store(false, Ordering::SeqCst);
    assert_eq!(r.engine.fail_open().epochs().bypassed(), [acme()]);
    assert_eq!(r.counters().bypass_flush_failures(), 1);

    // Correct but slow: every recall goes to the backend and nothing is cached.
    for n in 1..=2 {
        let got = r.recall("s", "acme").await.unwrap();
        assert_eq!(got.outcome, CacheOutcome::Miss);
        assert_eq!(texts(&got), ["acme v1", "acme v2"]);
        assert_eq!(r.backend.reads(), 1 + n);
    }
    let d = r.engine.degraded();
    assert_eq!(d.tenants, [(acme(), vec![DegradedReason::CacheBypass])]);

    assert_eq!(r.engine.recover().await, 1, "flush still failing");
    assert_eq!(r.counters().bypass_flush_failures(), 2);
    r.store.on.flush.store(false, Ordering::SeqCst);
    r.store.on.mark_written.store(false, Ordering::SeqCst);
    assert_eq!(r.engine.recover().await, 0);
    assert!(r.engine.degraded().is_empty());
    assert_eq!(
        r.recall("s", "acme").await.unwrap().outcome,
        CacheOutcome::Miss
    );
    assert_eq!(
        r.recall("s", "acme").await.unwrap().outcome,
        CacheOutcome::Hit
    );
}

/// In bypass the write log is missing stamps, so a stale entry that `get_stale` would pass
/// must not be served. The bypass starts *during* the failing load, so only the re-read of
/// the epoch in the stale path can catch it.
#[tokio::test]
async fn no_stale_answer_once_the_tenant_is_in_bypass() {
    let r = rig();
    r.cached_v1().await;
    r.expire();
    let gate = r.backend.hold();
    let (res, ()) = tokio::join!(r.recall("s", "acme"), async {
        settle().await;
        r.store.on.mark_written.store(true, Ordering::SeqCst);
        r.store.on.flush.store(true, Ordering::SeqCst);
        r.store.on.remove.store(true, Ordering::SeqCst); // the index cannot evict either
        r.remember("w", "acme v2", "acme").await.unwrap();
        r.backend
            .down(Some(BackendError::Unavailable("down".into())));
        gate.notify_one();
    });
    assert!(r.engine.fail_open().epochs().bypassed().contains(&acme()));
    assert_eq!(res.unwrap_err().code(), "backend_unavailable");
    assert_eq!(r.counters().stale_served(), 0);

    // And a recall that starts in bypass gets no stale answer either.
    assert_eq!(
        r.recall("s", "acme").await.unwrap_err().code(),
        "backend_unavailable"
    );
}

/// A load that read pre-write data before the failed write record must not store it, even
/// though the bypass is over by the time it finishes.
#[tokio::test]
async fn a_load_from_before_the_bypass_cannot_store_its_result() {
    let r = rig();
    r.backend.inner.seed(&acme(), "acme v1", &["acme"]);
    let gate = r.backend.hold();
    let (first, ()) = tokio::join!(r.recall("s", "acme"), async {
        settle().await; // the load has read v1 and is waiting
        r.store.on.mark_written.store(true, Ordering::SeqCst);
        r.remember("w", "acme v2", "acme").await.unwrap();
        r.store.on.mark_written.store(false, Ordering::SeqCst);
        assert_eq!(r.epoch(), 2, "bypass entered and left");
        gate.notify_one();
    });
    assert_eq!(
        texts(&first.unwrap()),
        ["acme v1"],
        "concurrent: either is fine"
    );
    assert_eq!(
        texts(&r.recall("s", "acme").await.unwrap()),
        ["acme v1", "acme v2"],
        "the old result was not cached"
    );
}

/// The whole bypass cycle (a write, a failed record, the flush, the exit) lands between the
/// load's last check and its put. Only the check *after* the put can catch that.
#[tokio::test]
async fn a_bypass_cycle_inside_the_put_is_caught_after_it() {
    let r = rig();
    r.backend.inner.seed(&acme(), "acme v1", &["acme"]);
    let (engine, backend) = (r.engine.clone(), r.backend.clone());
    *r.store.put_hook.lock().unwrap() = Some(Box::new(move |store: &RamStore| {
        backend.inner.write_now(&acme(), &fact("acme v2", "acme"));
        let fo = engine.fail_open();
        assert!(fo.enter_bypass(&acme()));
        futures::executor::block_on(store.flush(FlushScope::Tenant(acme())));
        assert!(fo.leave_bypass(&acme(), 1));
    }));
    assert_eq!(texts(&r.recall("s", "acme").await.unwrap()), ["acme v1"]);
    let next = r.recall("s", "acme").await.unwrap();
    assert_eq!(
        next.outcome,
        CacheOutcome::Miss,
        "the stored v1 was taken out"
    );
    assert_eq!(texts(&next), ["acme v1", "acme v2"]);
}

// ---------------------------------------------------------------------------------------
// G23: stale-if-error
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn stale_is_served_within_the_window_for_qualifying_errors_only() {
    for err in [
        BackendError::Unavailable("down".into()),
        BackendError::Timeout(Duration::from_secs(2)),
        BackendError::CircuitOpen,
    ] {
        let r = rig();
        r.cached_v1().await;
        r.expire();
        r.backend.down(Some(err.clone()));
        let got = r.recall("s", "acme").await.unwrap();
        assert_eq!(got.outcome, CacheOutcome::Stale, "{err:?}");
        assert_eq!(texts(&got), ["acme v1"]);
        assert!(got.age.is_some());
        assert_eq!(r.counters().stale_served(), 1);
    }
    let r = rig();
    r.cached_v1().await;
    r.expire();
    r.backend
        .down(Some(BackendError::Rejected("bad query".into())));
    assert_eq!(
        r.recall("s", "acme").await.unwrap_err().code(),
        "backend_error",
        "a rejection is a real answer, not an outage"
    );
}

#[tokio::test]
async fn nothing_is_served_past_the_window_or_when_it_is_off() {
    let r = rig();
    r.cached_v1().await;
    r.clock.advance(Duration::from_secs(91)); // ttl 60 + window 30, plus one
    r.backend
        .down(Some(BackendError::Unavailable("down".into())));
    assert!(r.recall("s", "acme").await.is_err());

    let off = rig_with("0s", ComponentPolicy::default(), false);
    off.cached_v1().await;
    off.expire();
    off.backend
        .down(Some(BackendError::Unavailable("down".into())));
    assert!(off.recall("s", "acme").await.is_err());
}

/// Only the TTL is relaxed. Normally the index evicts the entry at write time and the next
/// read drops it; with both failing, the write-log check inside `get_stale` is what refuses
/// it.
#[tokio::test]
async fn an_invalidated_entry_is_never_served_stale() {
    let r = rig();
    r.cached_v1().await;
    r.store.on.remove.store(true, Ordering::SeqCst);
    r.remember("w", "acme v2", "acme").await.unwrap();
    r.store.on.get.store(true, Ordering::SeqCst); // the hit-path read fails: a miss
    r.expire();
    r.backend
        .down(Some(BackendError::Unavailable("down".into())));
    assert_eq!(
        r.recall("s", "acme").await.unwrap_err().code(),
        "backend_unavailable"
    );
}

/// A session never gets a stale entry loaded before its own last write, even one the write
/// did not touch (G14 additions); another session may.
#[tokio::test]
async fn stale_answers_keep_read_your_writes() {
    let r = rig();
    r.cached_v1().await; // session "s", query "acme"
    r.remember("s", "unrelated zulu note", "zulu")
        .await
        .unwrap();
    r.expire();
    r.backend
        .down(Some(BackendError::Unavailable("down".into())));
    assert!(r.recall("s", "acme").await.is_err(), "own write is newer");
    let other = r.recall("other", "acme").await.unwrap();
    assert_eq!(other.outcome, CacheOutcome::Stale);
}

/// The caller's `max_staleness` still bounds the age; `0` never gets a cached answer.
#[tokio::test(start_paused = true)]
async fn stale_answers_honour_max_staleness_and_report_their_age() {
    let r = rig();
    r.cached_v1().await;
    r.expire();
    tokio::time::advance(Duration::from_secs(70)).await; // the engine's clock too
    r.backend
        .down(Some(BackendError::Unavailable("down".into())));
    let with = |max: Option<Duration>| {
        let mut q = req("s", "acme");
        q.max_staleness = max;
        r.engine.recall(&r.t, q)
    };
    assert!(
        with(Some(Duration::from_secs(60))).await.is_err(),
        "too old"
    );
    assert!(
        with(Some(Duration::ZERO)).await.is_err(),
        "0 bypasses the cache"
    );
    let got = with(Some(Duration::from_secs(80))).await.unwrap();
    assert_eq!(got.outcome, CacheOutcome::Stale);
    assert_eq!(got.age, Some(Duration::from_secs(70)));
}

// ---------------------------------------------------------------------------------------
// HTTP: /readyz (G25), the response fields, and the last-resort panic catch
// ---------------------------------------------------------------------------------------

fn state(r: Rig) -> (server::state::AppState, Arc<TestBackend>, Arc<ManualClock>) {
    let env: HashMap<&str, &str> = [("K_ACME", KEY)].into();
    let secrets = resolve_secrets(&r.config, |k| env.get(k).map(|v| v.to_string())).unwrap();
    let engine = Arc::try_unwrap(r.engine).ok().unwrap();
    (State::new(r.config, secrets, engine), r.backend, r.clock)
}

async fn send(app: Router, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"));
    let body = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.oneshot(b.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn readyz_reports_degraded_publicly_without_details_and_in_full_on_admin() {
    let (st, backend, clock) = state(rig_with("30s", ComponentPolicy::default(), true));
    backend.inner.seed(&acme(), "acme v1", &["acme"]);
    let recall = json!({ "session_id": "s", "query": "acme" });

    let (s, body) = send(public_router(st.clone()), "GET", "/readyz", None).await;
    assert_eq!((s, body), (StatusCode::OK, json!({ "status": "ready" })));

    let (s, body) = send(
        public_router(st.clone()),
        "POST",
        "/v1/recall",
        Some(recall.clone()),
    )
    .await;
    assert_eq!((s, &body["cache"]), (StatusCode::OK, &json!("miss")));
    assert!(body.get("age_ms").is_none(), "no age for a fresh load");

    // The backend goes down: the breaker opens, the expired entry is served stale.
    clock.advance(Duration::from_secs(70));
    backend.down(Some(BackendError::Unavailable("down".into())));
    let (s, body) = send(
        public_router(st.clone()),
        "POST",
        "/v1/recall",
        Some(recall),
    )
    .await;
    assert_eq!((s, &body["cache"]), (StatusCode::OK, &json!("stale")));
    assert!(body["age_ms"].is_u64());

    let (s, body) = send(public_router(st.clone()), "GET", "/readyz", None).await;
    assert_eq!(
        (s, &body),
        (StatusCode::OK, &json!({ "status": "degraded" }))
    );
    assert!(
        !body.to_string().contains("acme"),
        "public probe names no tenant"
    );

    st.engine.fail_open().enter_bypass(&acme());
    let (s, body) = send(admin_router(st.clone()), "GET", "/readyz", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        body,
        json!({
            "status": "degraded",
            "tenants": [{ "tenant": "acme", "reasons": ["circuit_open", "cache_bypass"] }],
            "components": [],
        })
    );

    let (s, body) = send(public_router(st.clone()), "GET", "/healthz", None).await;
    assert_eq!(
        (s, body),
        (StatusCode::OK, json!({ "status": "ok" })),
        "liveness unaffected"
    );

    st.set_ready(false);
    let (s, body) = send(public_router(st.clone()), "GET", "/readyz", None).await;
    assert_eq!(
        (s, body),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "status": "draining" })
        )
    );
}

/// A panic no inner guard catches (here in the adapter's `id`, used to build the cache key)
/// is answered with the standard 500 and counted; the next request is served normally.
#[tokio::test]
async fn an_uncaught_panic_is_a_500_not_a_dropped_connection() {
    let (st, backend, _) = state(rig());
    let recall = json!({ "session_id": "s", "query": "acme" });
    backend.panic_id.store(true, Ordering::SeqCst);
    let (s, body) = send(
        public_router(st.clone()),
        "POST",
        "/v1/recall",
        Some(recall.clone()),
    )
    .await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"]["code"], "internal");
    assert_eq!(
        st.engine
            .fail_open()
            .counters()
            .internal_errors(Component::Request),
        1
    );
    backend.panic_id.store(false, Ordering::SeqCst);
    let (s, _) = send(public_router(st), "POST", "/v1/recall", Some(recall)).await;
    assert_eq!(s, StatusCode::OK);
}

// ---------------------------------------------------------------------------------------
// Background task supervisor
// ---------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_supervisor_restarts_a_panicking_task_with_backoff() {
    let fo = server::failopen::FailOpen::default();
    let runs = AtomicU32::new(0);
    let shutdown = CancellationToken::new();
    let start = tokio::time::Instant::now();
    supervise("test", fo.counters(), &shutdown, || {
        let n = runs.fetch_add(1, Ordering::SeqCst);
        async move {
            assert!(n >= 2, "injected task panic");
        }
    })
    .await;
    assert_eq!(runs.load(Ordering::SeqCst), 3);
    assert_eq!(fo.counters().task_restarts(), 2);
    assert!(start.elapsed() >= Duration::from_secs(3), "1 s, then 2 s");
}

#[tokio::test(start_paused = true)]
async fn the_supervisor_stops_on_shutdown_while_backing_off() {
    let fo = server::failopen::FailOpen::default();
    let shutdown = CancellationToken::new();
    let ((), ()) = tokio::join!(
        supervise("test", fo.counters(), &shutdown, || async {
            panic!("injected task panic")
        }),
        async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            shutdown.cancel();
        }
    );
    let restarts = fo.counters().task_restarts();
    assert!((3..=5).contains(&restarts), "{restarts} restarts in 10 s");
}
