//! Fail open (ADR-009): a bug in BAiD's own cache, index, packer or session tracker never
//! fails a request. The request carries on as if BAiD were not there.
//!
//! # How a call is guarded
//!
//! The engine runs each call into one of those components through [`FailOpen`]:
//!
//! - [`FailOpen::run`] / [`FailOpen::run_async`] for *optional* work (a cache read, a put,
//!   indexing, packing). A panic is caught, logged without its payload (it may contain
//!   request data), counted in `baid_internal_errors_total{component}`, and the caller
//!   falls back (a miss, no caching, the fallback packer).
//! - [`FailOpen::always`] / [`FailOpen::always_async`] for work correctness depends on
//!   (`mark_written`, the session's write record, the G24 flush). These run even while the
//!   component is bypassed; the caller reacts to a failure by putting the tenant into cache
//!   bypass.
//!
//! # Component breaker
//!
//! A component that panics [`ComponentPolicy::threshold`] times within
//! [`ComponentPolicy::window`] is bypassed for [`ComponentPolicy::cooldown`], so requests stop
//! paying for the panic. After the cooldown one probe call goes through: success closes the
//! breaker, a panic re-opens it at once. Trips are counted in
//! `baid_component_trips_total{component}`.
//!
//! # Cache bypass for a tenant (G24)
//!
//! [`TenantEpochs`] gives each tenant an epoch: even is normal, odd is in bypass. A failed
//! write record moves the tenant to odd; only a successful flush of its cache moves it to
//! the next even number. Loads remember the epoch they started in and only store a result
//! if the epoch is unchanged both before and after the put, so a load that started before
//! the bypass can never put pre-write data back after it.
//!
//! # State left behind by a panic
//!
//! - **Write log** (`cache::WriteLog`): a panic in `mark_written` may leave the sequence
//!   number bumped with some entities unstamped. That is exactly G24: the tenant goes into
//!   bypass, and the flush removes every entry the missing stamps would have refused.
//! - **Cache entries**: a panic between insert and the put's re-check can leave an entry
//!   behind. Every read re-checks the write log (ADR-011), so it is never served after a
//!   write that touched it; with the tenant in bypass the post-put epoch check removes it.
//! - **Locks**: the cache, index and session tracker use `dashmap` and foyer, whose guards
//!   release on unwind and do not poison; `Resilient`'s and this module's `std` mutexes are
//!   never held across code that can panic, and are opened with poison recovery.
//! - **Index and sessions**: a half-done `register` or `touch` loses memory or an idle time,
//!   never correctness. A failed `note_write` would lose read-your-writes, so it is treated
//!   like a failed `mark_written` (G24).
//! - **Anything else** (a component left inconsistent in a way that keeps panicking) is what
//!   the component breaker is for.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use baid_core::{MemoryItem, PackedContext, TenantId, TokenBudget};
use dashmap::DashMap;
use futures::FutureExt;
use tracing::{error, warn};

/// A part of BAiD whose failures are caught and counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Component {
    /// The cache store: reads, puts, the write log.
    Cache,
    /// The entity → key invalidation index.
    Index,
    /// The packer.
    Packer,
    /// The session tracker.
    Sessions,
    /// A backend adapter (caught here only when it is not wrapped in `Resilient`, which
    /// catches its own).
    Adapter,
    /// A panic no inner guard caught, stopped at the HTTP layer (answered with a 500).
    Request,
}

impl Component {
    /// Every component, in index order.
    pub const ALL: [Component; 6] = [
        Component::Cache,
        Component::Index,
        Component::Packer,
        Component::Sessions,
        Component::Adapter,
        Component::Request,
    ];

    /// Lower-case name, for metric labels and `/readyz`.
    pub fn as_str(self) -> &'static str {
        match self {
            Component::Cache => "cache",
            Component::Index => "index",
            Component::Packer => "packer",
            Component::Sessions => "sessions",
            Component::Adapter => "adapter",
            Component::Request => "request",
        }
    }

    fn idx(self) -> usize {
        self as usize
    }
}

/// When a component is bypassed and for how long.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ComponentPolicy {
    /// Panics within `window` that trip the breaker.
    pub threshold: u32,
    /// The window panics are counted over.
    pub window: Duration,
    /// How long the component is bypassed once tripped.
    pub cooldown: Duration,
}

impl Default for ComponentPolicy {
    /// 5 panics within a minute bypass the component for 30 seconds.
    fn default() -> Self {
        ComponentPolicy {
            threshold: 5,
            window: Duration::from_secs(60),
            cooldown: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default)]
struct BreakerInner {
    recent: VecDeque<Instant>,
    open_until: Option<Instant>,
    /// When the current half-open probe was let through, if one is out.
    probe_since: Option<Instant>,
}

/// One component's breaker. The hot path (closed) is a single atomic load.
#[derive(Debug)]
struct ComponentBreaker {
    policy: ComponentPolicy,
    tripped: AtomicBool,
    inner: Mutex<BreakerInner>,
}

impl ComponentBreaker {
    fn new(policy: ComponentPolicy) -> Self {
        ComponentBreaker {
            policy,
            tripped: AtomicBool::new(false),
            inner: Mutex::new(BreakerInner::default()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BreakerInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// May a call go through at `now`? Claims the probe slot once the cooldown is over.
    fn allow(&self, now: Instant) -> bool {
        if !self.tripped.load(Ordering::Acquire) {
            return true;
        }
        let mut b = self.lock();
        match b.open_until {
            None => true,
            Some(t) if now < t => false,
            Some(_) => match b.probe_since {
                // A probe is out. If it never reported back (its future was dropped), let
                // another one through after a cooldown rather than stay bypassed for good.
                Some(p) if now.saturating_duration_since(p) < self.policy.cooldown => false,
                _ => {
                    b.probe_since = Some(now);
                    true
                }
            },
        }
    }

    /// A call that was let through returned normally.
    fn on_ok(&self) {
        if !self.tripped.load(Ordering::Acquire) {
            return;
        }
        let mut b = self.lock();
        if b.probe_since.is_some() {
            *b = BreakerInner::default();
            self.tripped.store(false, Ordering::Release);
        }
    }

    /// A call panicked at `now`. Returns true if this tripped the breaker.
    fn on_panic(&self, now: Instant) -> bool {
        let mut b = self.lock();
        if b.probe_since.take().is_some() {
            b.open_until = Some(now + self.policy.cooldown);
            return true;
        }
        if b.open_until.is_some_and(|t| now < t) {
            return false; // already bypassed (a correctness-critical call that runs anyway)
        }
        b.recent.push_back(now);
        while b
            .recent
            .front()
            .is_some_and(|t| now.saturating_duration_since(*t) > self.policy.window)
        {
            b.recent.pop_front();
        }
        if b.recent.len() >= self.policy.threshold.max(1) as usize {
            b.recent.clear();
            b.open_until = Some(now + self.policy.cooldown);
            self.tripped.store(true, Ordering::Release);
            return true;
        }
        false
    }

    fn is_bypassed(&self, now: Instant) -> bool {
        self.tripped.load(Ordering::Acquire) && self.lock().open_until.is_some_and(|t| now < t)
    }
}

/// Counters for fail-open events. Plain atomics for now; plan T21 exports them as
/// Prometheus metrics under the names given on each accessor.
#[derive(Debug, Default)]
pub struct Counters {
    internal_errors: [AtomicU64; Component::ALL.len()],
    trips: [AtomicU64; Component::ALL.len()],
    stale_served: AtomicU64,
    bypass_entered: AtomicU64,
    bypass_flush_failures: AtomicU64,
    task_restarts: AtomicU64,
}

impl Counters {
    /// `baid_internal_errors_total{component}`: panics caught in `component`.
    pub fn internal_errors(&self, c: Component) -> u64 {
        self.internal_errors[c.idx()].load(Ordering::Relaxed)
    }

    /// `baid_component_trips_total{component}`: times `component` was bypassed.
    pub fn trips(&self, c: Component) -> u64 {
        self.trips[c.idx()].load(Ordering::Relaxed)
    }

    /// `baid_stale_served_total`: recalls answered from an expired entry (G23).
    pub fn stale_served(&self) -> u64 {
        self.stale_served.load(Ordering::Relaxed)
    }

    /// `baid_cache_bypass_entered_total`: times a tenant went into cache bypass (G24).
    pub fn bypass_entered(&self) -> u64 {
        self.bypass_entered.load(Ordering::Relaxed)
    }

    /// `baid_cache_bypass_flush_failures_total`: flushes that failed, keeping a tenant in
    /// bypass (G24).
    pub fn bypass_flush_failures(&self) -> u64 {
        self.bypass_flush_failures.load(Ordering::Relaxed)
    }

    /// `baid_task_restarts_total`: background tasks restarted after a panic.
    pub fn task_restarts(&self) -> u64 {
        self.task_restarts.load(Ordering::Relaxed)
    }

    pub(crate) fn count_stale_served(&self) {
        self.stale_served.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_bypass_flush_failure(&self) {
        self.bypass_flush_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_task_restart(&self) {
        self.task_restarts.fetch_add(1, Ordering::Relaxed);
    }
}

/// Per-tenant cache epochs (G24). Even: the cache is in use. Odd: the tenant is in cache
/// bypass, and nothing reads from or writes to its cache.
#[derive(Debug, Default)]
pub struct TenantEpochs {
    map: DashMap<TenantId, Arc<AtomicU64>>,
}

impl TenantEpochs {
    /// True if `epoch` means "in bypass".
    pub fn is_bypass(epoch: u64) -> bool {
        epoch % 2 == 1
    }

    fn slot(&self, tenant: &TenantId) -> Arc<AtomicU64> {
        if let Some(s) = self.map.get(tenant) {
            return Arc::clone(s.value());
        }
        Arc::clone(self.map.entry(tenant.clone()).or_default().value())
    }

    /// The tenant's current epoch.
    pub fn get(&self, tenant: &TenantId) -> u64 {
        self.map
            .get(tenant)
            .map_or(0, |s| s.value().load(Ordering::SeqCst))
    }

    /// Moves the tenant into bypass. Returns false if it already was.
    fn enter(&self, tenant: &TenantId) -> bool {
        let slot = self.slot(tenant);
        let mut cur = slot.load(Ordering::SeqCst);
        loop {
            if Self::is_bypass(cur) {
                return false;
            }
            match slot.compare_exchange(cur, cur + 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(now) => cur = now,
            }
        }
    }

    /// Leaves bypass if the epoch is still `from` (the odd value read before the flush).
    fn leave(&self, tenant: &TenantId, from: u64) -> bool {
        Self::is_bypass(from)
            && self
                .slot(tenant)
                .compare_exchange(from, from + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    /// Tenants currently in bypass.
    pub fn bypassed(&self) -> Vec<TenantId> {
        let mut v: Vec<TenantId> = self
            .map
            .iter()
            .filter(|e| Self::is_bypass(e.value().load(Ordering::SeqCst)))
            .map(|e| e.key().clone())
            .collect();
        v.sort();
        v
    }
}

/// The fail-open guard: component breakers, counters and tenant epochs.
#[derive(Debug)]
pub struct FailOpen {
    breakers: [ComponentBreaker; Component::ALL.len()],
    counters: Counters,
    epochs: TenantEpochs,
}

impl Default for FailOpen {
    fn default() -> Self {
        Self::new(ComponentPolicy::default())
    }
}

impl FailOpen {
    /// A guard whose component breakers all use `policy`.
    pub fn new(policy: ComponentPolicy) -> Self {
        FailOpen {
            breakers: std::array::from_fn(|_| ComponentBreaker::new(policy)),
            counters: Counters::default(),
            epochs: TenantEpochs::default(),
        }
    }

    /// The counters.
    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// The tenant epochs.
    pub fn epochs(&self) -> &TenantEpochs {
        &self.epochs
    }

    /// Components currently bypassed by their breaker.
    pub fn bypassed_components(&self) -> Vec<Component> {
        let now = Instant::now();
        Component::ALL
            .into_iter()
            .filter(|c| self.breakers[c.idx()].is_bypassed(now))
            .collect()
    }

    /// Records a panic caught in `c`: logged (without the payload), counted, and fed to the
    /// component's breaker.
    pub fn panicked(&self, c: Component) {
        self.counters.internal_errors[c.idx()].fetch_add(1, Ordering::Relaxed);
        error!(
            component = c.as_str(),
            "internal error; falling back to the backend (fail open)"
        );
        if self.breakers[c.idx()].on_panic(Instant::now()) {
            self.counters.trips[c.idx()].fetch_add(1, Ordering::Relaxed);
            warn!(
                component = c.as_str(),
                cooldown = ?self.breakers[c.idx()].policy.cooldown,
                "component bypassed after repeated panics"
            );
        }
    }

    /// Runs optional work in `c`. `None` if the component is bypassed or `f` panicked.
    pub fn run<T>(&self, c: Component, f: impl FnOnce() -> T) -> Option<T> {
        if !self.breakers[c.idx()].allow(Instant::now()) {
            return None;
        }
        let out = self.always(c, f);
        if out.is_some() {
            self.breakers[c.idx()].on_ok();
        }
        out
    }

    /// Runs correctness-critical work in `c`, even while the component is bypassed. `None`
    /// if `f` panicked.
    pub fn always<T>(&self, c: Component, f: impl FnOnce() -> T) -> Option<T> {
        match std::panic::catch_unwind(AssertUnwindSafe(f)) {
            Ok(v) => Some(v),
            Err(_payload) => {
                self.panicked(c);
                None
            }
        }
    }

    /// [`run`](Self::run) for a future.
    pub async fn run_async<F: Future>(&self, c: Component, fut: F) -> Option<F::Output> {
        if !self.breakers[c.idx()].allow(Instant::now()) {
            return None;
        }
        let out = self.always_async(c, fut).await;
        if out.is_some() {
            self.breakers[c.idx()].on_ok();
        }
        out
    }

    /// [`always`](Self::always) for a future.
    pub async fn always_async<F: Future>(&self, c: Component, fut: F) -> Option<F::Output> {
        match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(v) => Some(v),
            Err(_payload) => {
                self.panicked(c);
                None
            }
        }
    }

    /// Puts `tenant` into cache bypass (G24). Returns true if it was not already. The engine
    /// does this itself when a write record fails; public for tests and admin tooling.
    pub fn enter_bypass(&self, tenant: &TenantId) -> bool {
        let entered = self.epochs.enter(tenant);
        if entered {
            self.counters.bypass_entered.fetch_add(1, Ordering::Relaxed);
            error!(tenant = %tenant, "write record failed; tenant's cache bypassed until it is flushed");
        }
        entered
    }

    /// Takes `tenant` out of bypass after a successful flush, if no other failure moved its
    /// epoch since `from` was read. Call it only after the tenant's cache was flushed.
    pub fn leave_bypass(&self, tenant: &TenantId, from: u64) -> bool {
        let left = self.epochs.leave(tenant, from);
        if left {
            warn!(tenant = %tenant, "tenant's cache flushed; bypass over");
        }
        left
    }
}

/// What the engine packs with when the packer itself failed: input order, repeated ids
/// dropped, stopping at the budget. Deliberately minimal so it cannot fail the same way.
pub fn fallback_pack(items: &[MemoryItem], budget: TokenBudget) -> PackedContext {
    let limit = budget.get();
    let mut seen = HashSet::new();
    let mut out = PackedContext::default();
    for item in items {
        let tokens = if item.tokens > 0 {
            item.tokens
        } else {
            u32::try_from(item.text.len() / 4 + 1).unwrap_or(u32::MAX)
        };
        if !seen.insert(&item.id) || out.tokens.saturating_add(tokens) > limit {
            out.dropped = out.dropped.saturating_add(1);
            continue;
        }
        out.tokens += tokens;
        let mut kept = item.clone();
        kept.tokens = tokens;
        out.items.push(kept);
    }
    out
}

/// Logs panics without their message, which may contain request data (a query, a fact),
/// unless `BAID_LOG_PANIC_MESSAGES=1`. The location is always logged. The binary installs
/// this at start-up.
pub fn install_panic_hook() {
    let with_message = std::env::var("BAID_LOG_PANIC_MESSAGES").is_ok_and(|v| v == "1");
    std::panic::set_hook(Box::new(move |info| {
        let at = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_default();
        let thread = std::thread::current().name().unwrap_or("?").to_owned();
        if with_message {
            let msg = info
                .payload()
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| info.payload().downcast_ref::<String>().cloned())
                .unwrap_or_default();
            error!(location = %at, thread, message = %msg, "panic");
        } else {
            error!(location = %at, thread, "panic (message withheld; set BAID_LOG_PANIC_MESSAGES=1 to log it)");
        }
    }));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use baid_core::{AdapterId, NodeId};

    use super::*;

    fn policy() -> ComponentPolicy {
        ComponentPolicy {
            threshold: 3,
            window: Duration::from_secs(10),
            cooldown: Duration::from_secs(30),
        }
    }

    #[test]
    fn breaker_trips_after_threshold_panics_within_the_window() {
        let b = ComponentBreaker::new(policy());
        let t0 = Instant::now();
        assert!(!b.on_panic(t0));
        assert!(!b.on_panic(t0 + Duration::from_secs(1)));
        assert!(b.allow(t0 + Duration::from_secs(2)), "still closed");
        assert!(b.on_panic(t0 + Duration::from_secs(2)), "third trips");
        assert!(!b.allow(t0 + Duration::from_secs(3)), "bypassed");
        assert!(b.is_bypassed(t0 + Duration::from_secs(3)));
    }

    #[test]
    fn panics_outside_the_window_do_not_add_up() {
        let b = ComponentBreaker::new(policy());
        let t0 = Instant::now();
        for i in 0..10 {
            assert!(!b.on_panic(t0 + Duration::from_secs(i * 6)), "panic {i}");
        }
    }

    #[test]
    fn after_the_cooldown_one_probe_decides() {
        let b = ComponentBreaker::new(policy());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_panic(t0);
        }
        let after = t0 + Duration::from_secs(31);
        assert!(b.allow(after), "probe let through");
        assert!(!b.allow(after), "only one probe at a time");
        assert!(b.on_panic(after), "a panicking probe re-trips at once");
        assert!(!b.allow(after + Duration::from_secs(1)));
        let later = after + Duration::from_secs(31);
        assert!(b.allow(later));
        b.on_ok();
        assert!(b.allow(later) && b.allow(later), "closed again");
        assert!(!b.on_panic(later), "and the count starts afresh");
    }

    #[test]
    fn a_probe_that_never_reports_back_does_not_bypass_forever() {
        let b = ComponentBreaker::new(policy());
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_panic(t0);
        }
        let after = t0 + Duration::from_secs(31);
        assert!(b.allow(after)); // its future is then dropped: no on_ok, no on_panic
        assert!(!b.allow(after + Duration::from_secs(29)));
        assert!(b.allow(after + Duration::from_secs(30)), "another probe");
    }

    #[test]
    fn run_bypasses_a_tripped_component_and_counts() {
        let fo = FailOpen::new(policy());
        let calls = std::sync::atomic::AtomicU32::new(0);
        let boom = || {
            calls.fetch_add(1, Ordering::SeqCst);
            panic!("bug")
        };
        for _ in 0..3 {
            assert_eq!(fo.run(Component::Index, boom), None::<()>);
        }
        assert_eq!(fo.counters().internal_errors(Component::Index), 3);
        assert_eq!(fo.counters().trips(Component::Index), 1);
        assert_eq!(fo.bypassed_components(), [Component::Index]);
        assert_eq!(fo.run(Component::Index, boom), None);
        assert_eq!(calls.load(Ordering::SeqCst), 3, "not called while bypassed");
        assert_eq!(
            fo.run(Component::Packer, || 7),
            Some(7),
            "others unaffected"
        );
        assert_eq!(
            fo.always(Component::Index, || 1),
            Some(1),
            "critical work still runs"
        );
    }

    #[test]
    fn epochs_enter_and_leave_bypass() {
        let fo = FailOpen::default();
        let t = TenantId::new("acme").unwrap();
        assert_eq!(fo.epochs().get(&t), 0);
        assert!(fo.enter_bypass(&t));
        assert!(!fo.enter_bypass(&t), "already in");
        let e = fo.epochs().get(&t);
        assert!(TenantEpochs::is_bypass(e));
        assert_eq!(fo.epochs().bypassed(), std::slice::from_ref(&t));
        assert!(!fo.leave_bypass(&t, e + 2), "stale `from` is refused");
        assert!(fo.leave_bypass(&t, e));
        assert_eq!(
            fo.epochs().get(&t),
            2,
            "a new even epoch, never the old one"
        );
        assert!(!fo.leave_bypass(&t, 2), "even epochs cannot leave");
        assert_eq!(fo.counters().bypass_entered(), 1);
    }

    fn item(id: &str, tokens: u32) -> MemoryItem {
        MemoryItem {
            id: NodeId::new(id).unwrap(),
            text: Arc::from("x"),
            score: 1.0,
            tokens,
            entities: Default::default(),
            as_of: None,
            source: AdapterId::new("a").unwrap(),
        }
    }

    #[test]
    fn fallback_pack_respects_the_budget_and_drops_repeats() {
        let items = [item("a", 40), item("a", 40), item("b", 30), item("c", 10)];
        let p = fallback_pack(&items, TokenBudget::new(64).unwrap());
        let ids: Vec<&str> = p.items.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, ["a", "c"]);
        assert_eq!((p.tokens, p.dropped), (50, 2));
    }
}
