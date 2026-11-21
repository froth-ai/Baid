//! The five extension traits. The engine holds each as an `Arc<dyn Trait>`, so any of them
//! can be swapped or mocked without touching the engine.
//!
//! `async_trait` is used because native async trait methods cannot yet be called through
//! `dyn`. Revisit when that lands on stable Rust.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::backend::{BackendResult, Capabilities, Fact, Health, WriteAck};
use crate::error::BackendError;
use crate::item::{
    CacheEntry, FlushScope, InvalidationReport, MemoryItem, PackedContext, PutOutcome, WriteSeq,
};
use crate::key::Key;
use crate::predict::{Candidate, QueryClass, SessionView, TenantStats};
use crate::types::{AdapterId, BackendQuery, EntityId, TenantId, TokenBudget};

/// A memory backend: Neo4j over Bolt, S3, Zep, Mem0. The only thing the core knows about
/// backends (ADR-007).
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// This adapter's id; part of every cache key it produces.
    fn id(&self) -> AdapterId;

    /// What the backend can do.
    fn capabilities(&self) -> Capabilities;

    /// Runs several queries, in one round trip if `capabilities().native_batch`.
    /// Returns one result per query, in the same order.
    async fn query_batch(
        &self,
        tenant: &TenantId,
        queries: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError>;

    /// Stores a fact. Returns only after the backend has acknowledged it durably.
    async fn write(&self, tenant: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError>;

    /// Current health, for `/readyz` and the circuit breaker.
    async fn health(&self) -> Health;
}

/// Proposes the queries a session is likely to make next. Pure CPU, no I/O; must return in
/// microseconds because it runs after every recall.
pub trait Predictor: Send + Sync + 'static {
    /// Up to `k` candidates, best first.
    fn predict(&self, view: &SessionView, stats: &TenantStats, k: usize) -> Vec<Candidate>;

    /// Records that `next` followed `prev` in one of `tenant`'s sessions.
    fn observe(&self, tenant: &TenantId, prev: Option<&QueryClass>, next: &QueryClass);
}

/// The cache: tiers, TTLs, priorities, and the write log behind the staleness guard.
///
/// # The staleness guard
///
/// Each tenant has a write sequence number, and each entity remembers the sequence number
/// of the last write that touched it.
///
/// - A loader reads [`write_seq`](Self::write_seq) *before* calling the backend and stores
///   it in [`CacheEntry::loaded_at`].
/// - A writer, after the backend acknowledges, calls [`mark_written`](Self::mark_written)
///   with every entity it touched.
/// - An entry is valid only while none of its [`CacheEntry::entities`] (query-named *and*
///   result-mentioned) was written after `loaded_at`. [`put`](Self::put) refuses invalid
///   entries and [`get`](Self::get) never returns one.
///
/// So a slow read that started before a write can never serve pre-write data, even when
/// the write touched an entity that only showed up in the results.
#[async_trait]
pub trait CacheStore: Send + Sync + 'static {
    /// The entry under `key`, if present, unexpired and not invalidated by a write. Does
    /// not check `max_staleness`; see [`CacheEntry::is_fresh`].
    async fn get(&self, key: &Key) -> Option<Arc<CacheEntry>>;

    /// Like [`get`](Self::get), but also returns an entry that expired less than `grace`
    /// ago. Only the TTL check is relaxed: an entry invalidated by a write is never
    /// returned. For stale-if-error (G23), when the backend is failing.
    async fn get_stale(&self, key: &Key, grace: Duration) -> Option<Arc<CacheEntry>>;

    /// True if `key` is present. Cheap; used to skip prefetching what is already cached.
    /// May report an entry that `get` would refuse.
    fn contains(&self, key: &Key) -> bool;

    /// Inserts `entry` unless one of its entities was written after `entry.loaded_at`, in
    /// which case nothing is stored and [`PutOutcome::StaleVersion`] is returned.
    async fn put(&self, key: Key, entry: CacheEntry) -> PutOutcome;

    /// Marks a speculative entry as used (first hit). Returns true if it was speculative.
    fn promote(&self, key: &Key) -> bool;

    /// Removes one key. Returns true if it was present.
    fn remove(&self, key: &Key) -> bool;

    /// Removes everything in `scope`. Returns how many entries went.
    async fn flush(&self, scope: FlushScope) -> usize;

    /// The tenant's current write sequence number. Read it before a backend call.
    fn write_seq(&self, tenant: &TenantId) -> WriteSeq;

    /// Records an acknowledged write to `entities`: takes the tenant's next sequence number,
    /// stamps the entities with it, and returns it.
    fn mark_written(&self, tenant: &TenantId, entities: &[EntityId]) -> WriteSeq;
}

/// Turns recalled items into the context the agent sees. Pure CPU; under 3 ms for 200 items.
pub trait Packer: Send + Sync + 'static {
    /// Dedupes, ranks and trims `items` to `budget`, in a stable order.
    fn pack(&self, items: &[MemoryItem], budget: TokenBudget) -> PackedContext;
}

/// Keeps cached results consistent with writes.
#[async_trait]
pub trait Invalidator: Send + Sync + 'static {
    /// Records that `key`'s result depends on `entities`, so a later write can evict it.
    fn register(&self, key: &Key, entities: &[EntityId]);

    /// A write to `entities` was acknowledged and marked: evict every key that depends on
    /// them. This frees memory early; correctness already comes from the write log in
    /// [`CacheStore`].
    async fn on_write(&self, tenant: &TenantId, entities: &[EntityId]) -> InvalidationReport;
}
