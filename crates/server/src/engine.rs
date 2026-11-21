//! The recall engine: recall, remember and session end, over the cache, the tenant's
//! backend and the packer.
//!
//! # Guarantees
//!
//! - **Tenant isolation.** The tenant comes from the API key; every key, cache tier,
//!   backend call and session is scoped by it.
//! - **A session always sees its own writes.** Each session remembers the write sequence
//!   number of its last `remember`; a cached entry loaded before that is a miss for that
//!   session.
//! - **No session sees data a write changed.** A write stamps the entities it touched;
//!   the cache refuses any entry that depends on them and was loaded earlier.
//! - **Results a write would *add* to someone else's cached search** appear within the
//!   tenant's TTL, the same bound as writes made outside BAiD.
//! - **One backend call per key at a time.** Concurrent misses on the same key share one
//!   load. Followers then re-read through the cache, so sharing never weakens the
//!   guarantees above.
//! - **Fail open** (ADR-009, [`crate::failopen`]). A panic in the cache, index, packer or
//!   session tracker is caught and the request carries on without it: a cache failure is a
//!   miss, a packer failure uses [`fallback_pack`]. A failure that would lose a write record
//!   puts the tenant into cache bypass until its cache is flushed (G24).
//! - **Stale-if-error** (G23). When the backend times out, is unavailable or its breaker is
//!   open, an entry that expired less than the tenant's `stale_if_error` ago may be served,
//!   marked `stale`. Only the TTL is relaxed: write invalidation, read-your-writes and the
//!   caller's `max_staleness` all still apply, and a tenant in bypass never gets one.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use adapters::{BreakerState, Registry};
use baid_core::{
    Backend, BackendError, BackendQuery, CacheEntry, CacheOutcome, CacheStore, EntityId, Fact,
    FlushScope, Invalidator, Key, LoadInfo, MemoryItem, NodeId, PackedContext, Packer, PutOutcome,
    RecallRequest, SessionId, TenantId, WriteSeq,
};
use dashmap::DashMap;
use exec::{Role, SingleFlight};
use futures::FutureExt;
use invalidate::EntityIndex;
use session::Sessions;
use tracing::{debug, error, warn};

use crate::auth::Tenant;
use crate::failopen::{fallback_pack, Component, ComponentPolicy, FailOpen, TenantEpochs};
use crate::http::error::ApiError;

/// What a recall returns.
#[derive(Clone, Debug)]
pub struct Recall {
    /// The packed items.
    pub packed: PackedContext,
    /// Hit, miss or stale.
    pub outcome: CacheOutcome,
    /// Age of the cached entry served (hit or stale); `None` when loaded for this call.
    pub age: Option<Duration>,
}

/// Why a tenant shows as degraded in `/readyz` (G25).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DegradedReason {
    /// The backend's circuit breaker is open: calls fail fast.
    CircuitOpen,
    /// The breaker is letting a probe through.
    CircuitHalfOpen,
    /// The tenant's cache is bypassed after a failed write record (G24).
    CacheBypass,
}

impl DegradedReason {
    /// Lower-case name, for `/readyz`.
    pub fn as_str(self) -> &'static str {
        match self {
            DegradedReason::CircuitOpen => "circuit_open",
            DegradedReason::CircuitHalfOpen => "circuit_half_open",
            DegradedReason::CacheBypass => "cache_bypass",
        }
    }
}

/// What is degraded right now, for `/readyz`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Degraded {
    /// Tenants with something wrong, sorted, each with its reasons.
    pub tenants: Vec<(TenantId, Vec<DegradedReason>)>,
    /// Internal components bypassed by their breaker.
    pub components: Vec<Component>,
}

impl Degraded {
    /// True if nothing is degraded.
    pub fn is_empty(&self) -> bool {
        self.tenants.is_empty() && self.components.is_empty()
    }
}

/// What one housekeeping pass cleaned up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Housekeeping {
    /// Idle sessions forgotten.
    pub sessions_expired: usize,
    /// Idempotency keys past their TTL.
    pub idempotency_keys_expired: usize,
    /// Index references to keys the cache no longer holds.
    pub index_refs_pruned: usize,
}

impl Housekeeping {
    /// True if nothing was cleaned up.
    pub fn is_empty(&self) -> bool {
        *self == Housekeeping::default()
    }
}

/// What a remember returns.
#[derive(Clone, Debug, PartialEq)]
pub struct Remembered {
    /// The backend's id for the stored fact.
    pub node: NodeId,
    /// Every entity the write touched (client-named plus backend-linked).
    pub entities: Vec<EntityId>,
    /// The item's version, if the backend tracks versions.
    pub version: Option<u64>,
    /// True when this answer was replayed for a repeated idempotency key.
    pub replayed: bool,
}

/// The engine's "now". Tokio's clock, which is the system clock except under a paused test
/// runtime, so tests can age cache entries deterministically.
pub fn clock_now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// Extra time the engine allows beyond the tenant's backend timeout. The adapter layer
/// (`adapters::Resilient`) enforces the real deadline and records the failure for its
/// circuit breaker; this outer bound only catches a backend that ignores it.
pub const BACKSTOP_MARGIN: Duration = Duration::from_millis(250);

fn backstop(t: &Tenant) -> Duration {
    t.config.backend_timeout.get() + BACKSTOP_MARGIN
}

/// Longest accepted idempotency key.
pub const MAX_IDEMPOTENCY_KEY: usize = 128;
/// How long an idempotency key is remembered.
pub const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 3600);
/// Above this many remembered keys, expired ones are pruned on insert.
const IDEMPOTENCY_PRUNE_AT: usize = 100_000;

/// Recent `remember` results by idempotency key, so a retried write is applied once.
///
/// Two *concurrent* requests with the same new key may both reach the backend; this guards
/// against retries, which are sequential.
#[derive(Debug, Default)]
struct Idempotency {
    map: DashMap<(TenantId, String), (Remembered, Instant)>,
}

impl Idempotency {
    fn get(&self, tenant: &TenantId, key: &str, now: Instant) -> Option<Remembered> {
        let hit = self.map.get(&(tenant.clone(), key.to_owned()))?;
        let (r, at) = hit.value();
        (now.saturating_duration_since(*at) < IDEMPOTENCY_TTL).then(|| r.clone())
    }

    fn insert(&self, tenant: TenantId, key: String, r: Remembered, now: Instant) {
        if self.map.len() >= IDEMPOTENCY_PRUNE_AT {
            self.prune(now);
        }
        self.map.insert((tenant, key), (r, now));
    }

    fn prune(&self, now: Instant) -> usize {
        let before = self.map.len();
        self.map
            .retain(|_, (_, at)| now.saturating_duration_since(*at) < IDEMPOTENCY_TTL);
        before.saturating_sub(self.map.len())
    }
}

/// What a shared load yields: the items, or the backend failure every waiter sees.
type LoadResult = Result<Arc<Vec<MemoryItem>>, BackendError>;

/// Calls the backend under `deadline`. A panic in an adapter that is not wrapped in
/// `Resilient` (which catches its own) becomes `Unavailable`; it is not retried here, so
/// `Resilient` stays the only retry layer.
async fn call_backend<T>(
    fo: &FailOpen,
    deadline: Duration,
    call: impl Future<Output = Result<T, BackendError>>,
) -> Result<T, BackendError> {
    match tokio::time::timeout(deadline, AssertUnwindSafe(call).catch_unwind()).await {
        Err(_) => Err(BackendError::Timeout(deadline)),
        Ok(Ok(r)) => r,
        Ok(Err(_panic)) => {
            fo.panicked(Component::Adapter);
            Err(BackendError::Unavailable("adapter panicked".into()))
        }
    }
}

/// Backend failures that stale-if-error answers. `Rejected` is not one: the backend did
/// answer, and serving old data would hide a real error.
fn stale_eligible(e: &BackendError) -> bool {
    matches!(
        e,
        BackendError::Timeout(_) | BackendError::Unavailable(_) | BackendError::CircuitOpen
    )
}

/// One backend call plus the cache put, owning everything it needs so concurrent misses can
/// share it (see [`SingleFlight`]).
struct Load {
    cache: Arc<dyn CacheStore>,
    index: Arc<EntityIndex>,
    fo: Arc<FailOpen>,
    /// The tenant's cache epoch when the request began (G24). Odd: do not cache.
    epoch: u64,
    backend: Arc<dyn Backend>,
    tenant: TenantId,
    key: Key,
    query: BackendQuery,
    ttl: Duration,
    deadline: Duration,
    now: Instant,
}

impl Load {
    async fn run(self) -> LoadResult {
        // Read before the call: a write acknowledged after this point is newer than the
        // entry, so the cache refuses it if the write touched any of its entities. If the
        // read fails, the result is simply not cached.
        let loaded_at = if TenantEpochs::is_bypass(self.epoch) {
            None
        } else {
            self.fo
                .run(Component::Cache, || self.cache.write_seq(&self.tenant))
        };
        let mut results = call_backend(
            &self.fo,
            self.deadline,
            self.backend
                .query_batch(&self.tenant, std::slice::from_ref(&self.query)),
        )
        .await?;
        if results.len() != 1 {
            error!(tenant = %self.tenant, got = results.len(), "adapter returned the wrong number of results");
            return Err(BackendError::Rejected(format!(
                "adapter returned {} results for 1 query",
                results.len()
            )));
        }
        let items = results.pop().map(|r| r.items).unwrap_or_default();
        if let Some(loaded_at) = loaded_at {
            self.store(&items, loaded_at).await;
        }
        Ok(Arc::new(items))
    }

    /// Caches the result. Best effort: any failure here costs a future miss, never this
    /// answer.
    async fn store(&self, items: &[MemoryItem], loaded_at: WriteSeq) {
        let Some(entry) = self.fo.run(Component::Cache, || {
            CacheEntry::new(
                items.to_vec(),
                LoadInfo {
                    query_entities: self.query.entities(),
                    loaded_at,
                    source: self.backend.id(),
                    ttl: self.ttl,
                    speculative: false,
                },
                self.now,
            )
        }) else {
            return;
        };
        let deps = entry.entities.to_vec();
        let epochs = self.fo.epochs();
        if epochs.get(&self.tenant) != self.epoch {
            // The tenant entered (or left) cache bypass during the call. Only saves a
            // pointless insert: the check after the put is the guard.
            return;
        }
        let outcome = self
            .fo
            .run_async(Component::Cache, self.cache.put(self.key.clone(), entry))
            .await;
        if epochs.get(&self.tenant) != self.epoch {
            // G24: a write record failed while we were storing, so this result may predate
            // that write and nothing in the write log would refuse it. Take it back out.
            // If even that fails, (re-)enter bypass: its flush removes it.
            if self
                .fo
                .always(Component::Cache, || self.cache.remove(&self.key))
                .is_none()
            {
                self.fo.enter_bypass(&self.tenant);
            }
            return;
        }
        match outcome {
            // Indexed after the put: an entry the put refused needs no eviction, and a write
            // that lands in between is caught by the cache's own check on read.
            Some(PutOutcome::Stored) => {
                self.fo
                    .run(Component::Index, || self.index.register(&self.key, &deps));
            }
            other => debug!(tenant = %self.tenant, outcome = ?other, "result not cached"),
        }
    }
}

/// The engine. Cheap to share behind an `Arc`.
pub struct Engine {
    cache: Arc<dyn CacheStore>,
    backends: Registry,
    packer: Arc<dyn Packer>,
    sessions: Arc<Sessions>,
    idempotency: Idempotency,
    flights: SingleFlight<LoadResult>,
    index: Arc<EntityIndex>,
    fo: Arc<FailOpen>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("backends", &self.backends)
            .field("sessions", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Builds the engine from its parts.
    pub fn new(
        cache: Arc<dyn CacheStore>,
        backends: Registry,
        packer: Arc<dyn Packer>,
        sessions: Arc<Sessions>,
    ) -> Self {
        let index = Arc::new(EntityIndex::new(Arc::clone(&cache)));
        Engine {
            cache,
            backends,
            packer,
            sessions,
            idempotency: Idempotency::default(),
            flights: SingleFlight::new(),
            index,
            fo: Arc::new(FailOpen::default()),
        }
    }

    /// Replaces the component breaker policy (see [`crate::failopen`]). For tests and tuning.
    #[must_use]
    pub fn with_component_policy(mut self, policy: ComponentPolicy) -> Self {
        self.fo = Arc::new(FailOpen::new(policy));
        self
    }

    /// The fail-open guard: counters, component breakers, tenant bypass state.
    pub fn fail_open(&self) -> &FailOpen {
        &self.fo
    }

    /// The session tracker.
    pub fn sessions(&self) -> &Sessions {
        &self.sessions
    }

    fn backend(&self, t: &Tenant) -> Result<Arc<dyn Backend>, ApiError> {
        self.backends.get(&t.id).ok_or_else(|| {
            error!(tenant = %t.id, "no backend configured for an authenticated tenant");
            ApiError::Internal
        })
    }

    /// Answers a recall from the cache if it can, otherwise from the backend (and caches
    /// the result). Concurrent misses on one key share a single backend call. If the
    /// backend fails, a recently expired entry may answer instead (stale-if-error, G23).
    pub async fn recall(&self, t: &Tenant, req: RecallRequest) -> Result<Recall, ApiError> {
        req.validate()?;
        let now = clock_now();
        let backend = self.backend(t)?;
        let query = req.backend_query();
        let key = Key::derive(&t.id, &backend.id(), &query);
        let epoch = self.fo.epochs().get(&t.id);
        // The session's read-your-writes floor. If the tracker fails we do not know it, so
        // this request reads nothing from the cache.
        let floor = self.fo.run(Component::Sessions, || {
            self.sessions.touch(&t.id, &req.session, now);
            self.sessions.last_write(&t.id, &req.session)
        });
        let reads = match floor {
            Some(floor) if !req.bypasses_cache() && !TenantEpochs::is_bypass(epoch) => Some(floor),
            _ => None,
        };

        if let Some(floor) = reads {
            if let Some(e) = self.servable(&key, now, &req, floor).await {
                let age = now.saturating_duration_since(e.created_at);
                return Ok(self.packed(&e.items, &req, CacheOutcome::Hit, Some(age)));
            }
        }

        if TenantEpochs::is_bypass(epoch) {
            // Cache bypass (G24): no sharing either, since followers would have to re-read
            // through the cache. Straight to the backend.
            let result = self.load(t, &backend, &key, &query, now, epoch).run().await;
            return self.answer(t, &key, &req, reads, result).await;
        }

        let load = self.load(t, &backend, &key, &query, now, epoch);
        let (result, role) = self.flights.run(&key, move || load.run()).await;
        let result = match (result, role) {
            (Ok(_), Role::Follower) => {
                // We joined a load that may have started before something this request must
                // see: this session's own last write, or another session's acknowledged
                // write. Re-read through the cache, which applies every staleness check, and
                // load directly if it refuses.
                if let Some(floor) = reads {
                    if let Some(e) = self.servable(&key, now, &req, floor).await {
                        return Ok(self.packed(&e.items, &req, CacheOutcome::Miss, None));
                    }
                }
                self.load(t, &backend, &key, &query, now, epoch).run().await
            }
            (r, _) => r,
        };
        self.answer(t, &key, &req, reads, result).await
    }

    /// Packs a loaded result, or answers a backend failure from a stale entry if allowed.
    async fn answer(
        &self,
        t: &Tenant,
        key: &Key,
        req: &RecallRequest,
        reads: Option<WriteSeq>,
        result: LoadResult,
    ) -> Result<Recall, ApiError> {
        match result {
            Ok(items) => Ok(self.packed(&items, req, CacheOutcome::Miss, None)),
            Err(err) => match self.stale(t, key, req, reads, &err).await {
                Some(recall) => Ok(recall),
                None => Err(err.into()),
            },
        }
    }

    /// Stale-if-error (G23): an entry that expired less than `stale_if_error` ago, if the
    /// failure qualifies and every other check passes. `reads` is the session floor, or
    /// `None` when this request may not read the cache at all.
    async fn stale(
        &self,
        t: &Tenant,
        key: &Key,
        req: &RecallRequest,
        reads: Option<WriteSeq>,
        err: &BackendError,
    ) -> Option<Recall> {
        let floor = reads?;
        let grace = t.config.stale_if_error.get();
        if !stale_eligible(err) || grace.is_zero() {
            return None;
        }
        // Re-read: a write record may have failed while the load ran. In bypass the write
        // log is missing stamps, so its check cannot be trusted (G24).
        if TenantEpochs::is_bypass(self.fo.epochs().get(&t.id)) {
            return None;
        }
        let e = self
            .fo
            .run_async(Component::Cache, self.cache.get_stale(key, grace))
            .await
            .flatten()?;
        let now = clock_now();
        let age = now.saturating_duration_since(e.created_at);
        let within_max = req.max_staleness.is_none_or(|max| age <= max);
        if e.loaded_at < floor || !within_max {
            return None;
        }
        self.fo.counters().count_stale_served();
        warn!(tenant = %t.id, error = %err, age_ms = age.as_millis(), "backend failing; served a stale entry");
        Some(self.packed(&e.items, req, CacheOutcome::Stale, Some(age)))
    }

    /// The cached entry for `key`, if this request may be served from it: present, valid
    /// under the write log (checked by the store), within `max_staleness`, and loaded after
    /// this session's own last write. Promotes a speculative entry on use.
    async fn servable(
        &self,
        key: &Key,
        now: Instant,
        req: &RecallRequest,
        own_last_write: WriteSeq,
    ) -> Option<Arc<CacheEntry>> {
        let e = self
            .fo
            .run_async(Component::Cache, self.cache.get(key))
            .await
            .flatten()?;
        if !(e.is_fresh(now, req.max_staleness) && e.loaded_at >= own_last_write) {
            return None;
        }
        if e.speculative {
            self.fo.run(Component::Cache, || self.cache.promote(key));
        }
        Some(e)
    }

    fn packed(
        &self,
        items: &[MemoryItem],
        req: &RecallRequest,
        outcome: CacheOutcome,
        age: Option<Duration>,
    ) -> Recall {
        let packed = self
            .fo
            .run(Component::Packer, || self.packer.pack(items, req.budget))
            .unwrap_or_else(|| fallback_pack(items, req.budget));
        Recall {
            packed,
            outcome,
            age,
        }
    }

    fn load(
        &self,
        t: &Tenant,
        backend: &Arc<dyn Backend>,
        key: &Key,
        query: &BackendQuery,
        now: Instant,
        epoch: u64,
    ) -> Load {
        Load {
            cache: Arc::clone(&self.cache),
            index: Arc::clone(&self.index),
            fo: Arc::clone(&self.fo),
            epoch,
            backend: Arc::clone(backend),
            tenant: t.id.clone(),
            key: key.clone(),
            query: query.clone(),
            ttl: t.config.ttl.get(),
            deadline: backstop(t),
            now,
        }
    }

    /// Loads currently in flight (shared by concurrent misses).
    pub fn loads_in_flight(&self) -> usize {
        self.flights.in_flight()
    }

    /// Forwards a fact to the backend, then marks every entity it touched as written.
    pub async fn remember(
        &self,
        t: &Tenant,
        session: &SessionId,
        fact: Fact,
    ) -> Result<Remembered, ApiError> {
        fact.validate()?;
        let now = clock_now();
        if let Some(k) = &fact.idempotency_key {
            if k.is_empty() || k.len() > MAX_IDEMPOTENCY_KEY {
                return Err(ApiError::InvalidArgument(format!(
                    "idempotency_key must be 1 to {MAX_IDEMPOTENCY_KEY} bytes"
                )));
            }
            if let Some(mut prev) = self.idempotency.get(&t.id, k, now) {
                prev.replayed = true;
                return Ok(prev);
            }
        }
        let backend = self.backend(t)?;
        let result = call_backend(&self.fo, backstop(t), backend.write(&t.id, &fact)).await;

        let ack = match result {
            Ok(ack) => ack,
            Err(e) => {
                // A timeout, dropped connection or adapter panic may still have applied the
                // write, so treat its entities as written: that costs misses, never a wrong
                // answer. A rejection was definitely not applied.
                if !matches!(e, BackendError::Rejected(_)) {
                    self.record_write(t, session, &fact.entities, now).await;
                    warn!(tenant = %t.id, error = %e, "write failed; its entities are treated as written");
                }
                return Err(e.into());
            }
        };

        let mut entities = fact.entities.clone();
        entities.extend(ack.entities.iter().cloned());
        entities.sort_unstable();
        entities.dedup();
        self.record_write(t, session, &entities, now).await;

        let r = Remembered {
            node: ack.node,
            entities,
            version: ack.version,
            replayed: false,
        };
        if let Some(k) = fact.idempotency_key {
            self.idempotency.insert(t.id.clone(), k, r.clone(), now);
        }
        Ok(r)
    }

    /// Records a write that was acknowledged (or may have been applied): stamps its
    /// entities in the write log, raises the session's read-your-writes floor, and evicts
    /// dependent entries.
    ///
    /// The stamp and the floor are what keep the cache correct, so they run even while
    /// their component is bypassed. If either fails, the cache can no longer tell which
    /// entries this write made stale: the tenant goes into cache bypass, and leaves it only
    /// after a successful flush (G24). Eviction is only a memory optimisation.
    async fn record_write(
        &self,
        t: &Tenant,
        session: &SessionId,
        entities: &[EntityId],
        now: Instant,
    ) {
        let recorded = self
            .fo
            .always(Component::Cache, || {
                self.cache.mark_written(&t.id, entities)
            })
            .and_then(|seq| {
                self.fo.always(Component::Sessions, || {
                    self.sessions.note_write(&t.id, session, seq, now);
                })
            });
        if recorded.is_none() {
            self.fo.enter_bypass(&t.id);
            // Try to recover at once; housekeeping retries if the flush fails.
            self.leave_bypass(&t.id).await;
        }
        if let Some(r) = self
            .fo
            .run_async(Component::Index, self.index.on_write(&t.id, entities))
            .await
        {
            debug!(tenant = %t.id, entities = r.entities, keys_evicted = r.keys_evicted, "write invalidated");
        }
    }

    /// Flushes a bypassed tenant's cache and, if that succeeds, ends the bypass (G24).
    /// Returns true if the tenant is out of bypass afterwards.
    async fn leave_bypass(&self, tenant: &TenantId) -> bool {
        let from = self.fo.epochs().get(tenant);
        if !TenantEpochs::is_bypass(from) {
            return true;
        }
        let flushed = self
            .fo
            .always_async(
                Component::Cache,
                self.cache.flush(FlushScope::Tenant(tenant.clone())),
            )
            .await;
        if flushed.is_none() {
            self.fo.counters().count_bypass_flush_failure();
            warn!(tenant = %tenant, "flush failed; tenant stays in cache bypass");
            return false;
        }
        self.fo.leave_bypass(tenant, from)
    }

    /// Retries the flush for every tenant in cache bypass. Returns how many are still in
    /// bypass. Run periodically (housekeeping).
    pub async fn recover(&self) -> usize {
        let mut still = 0;
        for tenant in self.fo.epochs().bypassed() {
            if !self.leave_bypass(&tenant).await {
                still += 1;
            }
        }
        still
    }

    /// What is degraded right now: backends with an open or half-open breaker, tenants in
    /// cache bypass, bypassed internal components (G25).
    pub fn degraded(&self) -> Degraded {
        let bypassed = self.fo.epochs().bypassed();
        let mut tenants: Vec<(TenantId, Vec<DegradedReason>)> = self
            .backends
            .tenants()
            .filter_map(|tenant| {
                let mut why = Vec::new();
                match self.backends.breaker(tenant) {
                    Some(BreakerState::Open) => why.push(DegradedReason::CircuitOpen),
                    Some(BreakerState::HalfOpen) => why.push(DegradedReason::CircuitHalfOpen),
                    Some(BreakerState::Closed) | None => {}
                }
                if bypassed.contains(tenant) {
                    why.push(DegradedReason::CacheBypass);
                }
                (!why.is_empty()).then(|| (tenant.clone(), why))
            })
            .collect();
        tenants.sort_by(|a, b| a.0.cmp(&b.0));
        Degraded {
            tenants,
            components: self.fo.bypassed_components(),
        }
    }

    /// Forgets a session. Returns true if it existed.
    pub fn end_session(&self, t: &Tenant, session: &SessionId) -> bool {
        self.fo
            .run(Component::Sessions, || self.sessions.end(&t.id, session))
            .unwrap_or(false)
    }

    /// Periodic cleanup: idle sessions and old idempotency keys.
    pub fn housekeeping(&self, now: Instant, session_idle: Duration) -> Housekeeping {
        Housekeeping {
            sessions_expired: self
                .fo
                .run(Component::Sessions, || {
                    self.sessions.expire_idle(now, session_idle)
                })
                .unwrap_or(0),
            idempotency_keys_expired: self.idempotency.prune(now),
            index_refs_pruned: self
                .fo
                .run(Component::Index, || self.index.prune())
                .unwrap_or(0),
        }
    }

    /// The entity → key index (for inspection and tests).
    pub fn index(&self) -> &EntityIndex {
        &self.index
    }
}
