//! Retries and a circuit breaker around any [`Backend`].
//!
//! # Retries
//!
//! A read (`query_batch`) that fails with a retryable error (timeout, unavailable) is tried
//! again, up to [`RetryPolicy::max_attempts`] in total, with exponential backoff and jitter.
//! A write is retried only when it carries an idempotency key, because otherwise a retry
//! after a lost acknowledgement could store the fact twice. `Rejected` is never retried.
//! The engine's per-call deadline still bounds the whole sequence.
//!
//! # Circuit breaker
//!
//! - **Closed**: calls go through. Each retryable failure (after retries) counts; any
//!   success resets the count.
//! - **Open**: after [`BreakerPolicy::failure_threshold`] consecutive failures, calls fail at
//!   once with `CircuitOpen` for [`BreakerPolicy::open_for`]. The backend gets room to
//!   recover and callers stop waiting on timeouts.
//! - **Half-open**: after that, exactly one call is let through as a probe. Success closes
//!   the breaker; failure opens it again.
//!
//! `Rejected` errors do not count: a bad query says nothing about backend health.
//!
//! # Deadline
//!
//! Every call has one deadline, shared by its attempts and the backoff between them. When
//! it runs out the call fails with `Timeout` *and the failure is recorded*. That matters: if
//! only an outer timeout cut the call short, the future would be dropped, the breaker would
//! never hear about it, and a dead backend would hold every caller for the full timeout
//! forever.
//!
//! # Panics in the adapter
//!
//! A panic inside the wrapped adapter is caught and becomes `Unavailable`: recorded as a
//! failure, never retried (a panic usually repeats), and counted in [`Resilient::panics`].
//! Letting it unwind through here would leave a half-open breaker's probe slot claimed,
//! failing every later call with `CircuitOpen` for good (ADR-009, fail open).

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use baid_core::{
    AdapterId, Backend, BackendError, BackendQuery, BackendResult, Capabilities, Fact, Health,
    TenantId, WriteAck,
};
use futures::FutureExt;
use tokio::time::Instant;
use tracing::warn;

/// How often and how fast to retry.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Attempts in total, including the first (1 = no retries).
    pub max_attempts: u32,
    /// Delay before the first retry; doubles each time.
    pub base_delay: Duration,
    /// Upper bound on a single delay.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(25),
            max_delay: Duration::from_millis(250),
        }
    }
}

/// When the breaker opens and for how long.
#[derive(Clone, Copy, Debug)]
pub struct BreakerPolicy {
    /// Consecutive failures that open the breaker.
    pub failure_threshold: u32,
    /// How long it stays open before a probe is allowed.
    pub open_for: Duration,
}

impl Default for BreakerPolicy {
    fn default() -> Self {
        BreakerPolicy {
            failure_threshold: 5,
            open_for: Duration::from_secs(10),
        }
    }
}

/// The breaker's state, for health reporting and metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerState {
    /// Calls go through.
    Closed,
    /// Calls fail fast.
    Open,
    /// One probe call is in flight or allowed.
    HalfOpen,
}

#[derive(Debug)]
struct Breaker {
    failures: u32,
    open_until: Option<Instant>,
    probe_in_flight: bool,
}

/// A backend wrapped with retries and a circuit breaker.
pub struct Resilient<B> {
    inner: B,
    retry: RetryPolicy,
    policy: BreakerPolicy,
    deadline: Duration,
    breaker: Mutex<Breaker>,
    jitter_seed: AtomicU64,
    panics: AtomicU64,
}

impl<B: Backend> Resilient<B> {
    /// Wraps `inner` with the given policies.
    pub fn new(inner: B, retry: RetryPolicy, policy: BreakerPolicy, deadline: Duration) -> Self {
        Resilient {
            inner,
            retry,
            policy,
            deadline,
            breaker: Mutex::new(Breaker {
                failures: 0,
                open_until: None,
                probe_in_flight: false,
            }),
            jitter_seed: AtomicU64::new(0x9E37_79B9_7F4A_7C15),
            panics: AtomicU64::new(0),
        }
    }

    /// Wraps `inner` with the default policies and a per-call `deadline`.
    pub fn with_defaults(inner: B, deadline: Duration) -> Self {
        Self::new(
            inner,
            RetryPolicy::default(),
            BreakerPolicy::default(),
            deadline,
        )
    }

    /// The wrapped backend.
    pub fn inner(&self) -> &B {
        &self.inner
    }

    fn lock(&self) -> MutexGuard<'_, Breaker> {
        self.breaker.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Panics caught in the wrapped adapter since start.
    pub fn panics(&self) -> u64 {
        self.panics.load(Ordering::Relaxed)
    }

    /// Current breaker state.
    pub fn state(&self) -> BreakerState {
        let b = self.lock();
        match b.open_until {
            None => BreakerState::Closed,
            Some(t) if Instant::now() < t => BreakerState::Open,
            Some(_) => BreakerState::HalfOpen,
        }
    }

    /// May a call go through now? Claims the probe slot when half-open.
    fn admit(&self) -> Result<(), BackendError> {
        let mut b = self.lock();
        match b.open_until {
            None => Ok(()),
            Some(t) if Instant::now() < t => Err(BackendError::CircuitOpen),
            Some(_) if b.probe_in_flight => Err(BackendError::CircuitOpen),
            Some(_) => {
                b.probe_in_flight = true;
                Ok(())
            }
        }
    }

    fn on_success(&self) {
        let mut b = self.lock();
        b.failures = 0;
        b.open_until = None;
        b.probe_in_flight = false;
    }

    fn on_failure(&self, e: &BackendError) {
        if !e.is_retryable() {
            // Says nothing about health; release a probe slot without changing state.
            self.lock().probe_in_flight = false;
            return;
        }
        let mut b = self.lock();
        b.failures = b.failures.saturating_add(1);
        let was_probe = b.probe_in_flight;
        b.probe_in_flight = false;
        if was_probe || b.failures >= self.policy.failure_threshold {
            b.open_until = Some(Instant::now() + self.policy.open_for);
            drop(b);
            warn!(id = %self.inner.id(), error = %e, "circuit opened");
        }
    }

    /// Exponential backoff with full jitter, from a cheap xorshift.
    fn backoff(&self, attempt: u32) -> Duration {
        let cap = self
            .retry
            .base_delay
            .saturating_mul(1 << attempt.min(16))
            .min(self.retry.max_delay);
        let mut x = self.jitter_seed.fetch_add(0x9E37_79B9, Ordering::Relaxed) | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let frac = (x % 1_000) as u32;
        cap.mul_f64(f64::from(frac) / 1_000.0)
    }

    async fn call<T, F, Fut>(&self, retry: bool, mut f: F) -> Result<T, BackendError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, BackendError>>,
    {
        self.admit()?;
        let attempts = if retry {
            self.retry.max_attempts.max(1)
        } else {
            1
        };
        let start = Instant::now();
        let mut attempt = 0;
        loop {
            let remaining = self.deadline.saturating_sub(start.elapsed());
            let result = if remaining.is_zero() {
                Err(BackendError::Timeout(self.deadline))
            } else {
                match tokio::time::timeout(remaining, AssertUnwindSafe(f()).catch_unwind()).await {
                    Err(_) => Err(BackendError::Timeout(self.deadline)),
                    Ok(Ok(r)) => r,
                    Ok(Err(_panic)) => {
                        // The payload is not logged: it may contain request data.
                        self.panics.fetch_add(1, Ordering::Relaxed);
                        warn!(id = %self.inner.id(), "adapter panicked; treated as unavailable");
                        let e = BackendError::Unavailable("adapter panicked".into());
                        self.on_failure(&e);
                        return Err(e);
                    }
                }
            };
            let remaining = self.deadline.saturating_sub(start.elapsed());
            match result {
                Ok(v) => {
                    self.on_success();
                    return Ok(v);
                }
                Err(e) if e.is_retryable() && attempt + 1 < attempts && !remaining.is_zero() => {
                    tokio::time::sleep(self.backoff(attempt).min(remaining)).await;
                    attempt += 1;
                }
                Err(e) => {
                    self.on_failure(&e);
                    return Err(e);
                }
            }
        }
    }
}

#[async_trait]
impl<B: Backend> Backend for Resilient<B> {
    fn id(&self) -> AdapterId {
        self.inner.id()
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn query_batch(
        &self,
        tenant: &TenantId,
        queries: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        self.call(true, || self.inner.query_batch(tenant, queries))
            .await
    }

    async fn write(&self, tenant: &TenantId, fact: &Fact) -> Result<WriteAck, BackendError> {
        let retry = fact.idempotency_key.is_some();
        self.call(retry, || self.inner.write(tenant, fact)).await
    }

    async fn health(&self) -> Health {
        match (self.state(), self.inner.health().await) {
            (BreakerState::Closed, h) => h,
            (state, Health::Up) => Health::Degraded {
                reason: format!("circuit {state:?}"),
            },
            (_, h) => h,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use baid_core::mock::MockBackend;
    use baid_core::{EntityId, Filters};

    fn t() -> TenantId {
        TenantId::new("acme").unwrap()
    }
    fn q() -> Vec<BackendQuery> {
        vec![BackendQuery::Search {
            text: "x".into(),
            filters: Filters::default(),
            limit: 5,
        }]
    }
    fn fact(key: Option<&str>) -> Fact {
        Fact {
            text: "x".into(),
            entities: vec![EntityId::new("acme").unwrap()],
            labels: vec![],
            as_of: None,
            idempotency_key: key.map(String::from),
        }
    }
    fn wrap(threshold: u32) -> Resilient<MockBackend> {
        Resilient::new(
            MockBackend::new(),
            RetryPolicy::default(),
            BreakerPolicy {
                failure_threshold: threshold,
                open_for: Duration::from_secs(10),
            },
            Duration::from_secs(2),
        )
    }
    fn down() -> BackendError {
        BackendError::Unavailable("down".into())
    }

    #[tokio::test(start_paused = true)]
    async fn transient_read_failures_are_retried() {
        let r = wrap(5);
        r.inner().fail_next(down());
        r.inner().fail_next(down());
        assert!(r.query_batch(&t(), &q()).await.is_ok());
        assert_eq!(r.inner().query_calls(), 3);
        assert_eq!(r.state(), BreakerState::Closed);
    }

    #[tokio::test(start_paused = true)]
    async fn rejections_are_not_retried_and_do_not_trip() {
        let r = wrap(1);
        r.inner().fail_next(BackendError::Rejected("bad".into()));
        assert!(r.query_batch(&t(), &q()).await.is_err());
        assert_eq!(r.inner().query_calls(), 1);
        assert_eq!(r.state(), BreakerState::Closed);
    }

    #[tokio::test(start_paused = true)]
    async fn writes_retry_only_with_an_idempotency_key() {
        let r = wrap(5);
        r.inner().fail_next(down());
        assert!(r.write(&t(), &fact(None)).await.is_err());
        assert_eq!(
            r.inner().write_calls(),
            1,
            "no key: a retry could store twice"
        );
        r.inner().fail_next(down());
        assert!(r.write(&t(), &fact(Some("k"))).await.is_ok());
        assert_eq!(r.inner().write_calls(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn breaker_opens_fails_fast_then_probes() {
        let r = wrap(2);
        for _ in 0..6 {
            r.inner().fail_next(down()); // two calls x three attempts
        }
        assert!(r.query_batch(&t(), &q()).await.is_err());
        assert_eq!(r.state(), BreakerState::Closed);
        assert!(r.query_batch(&t(), &q()).await.is_err());
        assert_eq!(r.state(), BreakerState::Open);

        let calls = r.inner().query_calls();
        assert_eq!(
            r.query_batch(&t(), &q()).await,
            Err(BackendError::CircuitOpen)
        );
        assert_eq!(r.inner().query_calls(), calls, "open: backend not called");

        tokio::time::advance(Duration::from_secs(11)).await;
        assert_eq!(r.state(), BreakerState::HalfOpen);
        assert!(r.query_batch(&t(), &q()).await.is_ok(), "probe succeeds");
        assert_eq!(r.state(), BreakerState::Closed);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_probe_reopens() {
        let r = wrap(1);
        for _ in 0..3 {
            r.inner().fail_next(down());
        }
        assert!(r.query_batch(&t(), &q()).await.is_err());
        assert_eq!(r.state(), BreakerState::Open);
        tokio::time::advance(Duration::from_secs(11)).await;
        for _ in 0..3 {
            r.inner().fail_next(down());
        }
        assert!(r.query_batch(&t(), &q()).await.is_err());
        assert_eq!(r.state(), BreakerState::Open);
    }

    #[tokio::test(start_paused = true)]
    async fn health_reports_an_open_breaker() {
        let r = wrap(1);
        for _ in 0..3 {
            r.inner().fail_next(down());
        }
        let _ = r.query_batch(&t(), &q()).await;
        assert!(matches!(r.health().await, Health::Degraded { .. }));
    }

    /// A backend that hangs. Without the shared deadline the caller's outer timeout would
    /// drop the call and the breaker would never learn of the failure.
    struct Hang;

    #[async_trait]
    impl Backend for Hang {
        fn id(&self) -> AdapterId {
            AdapterId::new("hang").unwrap()
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }
        async fn query_batch(
            &self,
            _: &TenantId,
            _: &[BackendQuery],
        ) -> Result<Vec<BackendResult>, BackendError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(vec![])
        }
        async fn write(&self, _: &TenantId, _: &Fact) -> Result<WriteAck, BackendError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Err(BackendError::Unavailable("never".into()))
        }
        async fn health(&self) -> Health {
            Health::Up
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hanging_backend_times_out_and_trips_the_breaker() {
        let r = Resilient::new(
            Hang,
            RetryPolicy::default(),
            BreakerPolicy {
                failure_threshold: 2,
                open_for: Duration::from_secs(10),
            },
            Duration::from_millis(500),
        );
        let started = Instant::now();
        let e = r.query_batch(&t(), &q()).await.unwrap_err();
        assert!(matches!(e, BackendError::Timeout(_)), "{e:?}");
        assert!(
            started.elapsed() <= Duration::from_millis(600),
            "deadline is shared by attempts"
        );
        assert!(r.query_batch(&t(), &q()).await.is_err());
        assert_eq!(r.state(), BreakerState::Open, "timeouts count as failures");
        let before = Instant::now();
        assert_eq!(
            r.query_batch(&t(), &q()).await,
            Err(BackendError::CircuitOpen)
        );
        assert!(
            before.elapsed() < Duration::from_millis(1),
            "open breaker fails fast"
        );
    }

    /// Panics on its first `n` reads, then answers.
    struct Panicky {
        left: std::sync::atomic::AtomicU32,
        calls: std::sync::atomic::AtomicU32,
    }

    #[async_trait]
    impl Backend for Panicky {
        fn id(&self) -> AdapterId {
            AdapterId::new("panicky").unwrap()
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }
        async fn query_batch(
            &self,
            _: &TenantId,
            qs: &[BackendQuery],
        ) -> Result<Vec<BackendResult>, BackendError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let left = self.left.load(Ordering::SeqCst);
            if left > 0 {
                self.left.store(left - 1, Ordering::SeqCst);
                panic!("adapter bug");
            }
            Ok(qs.iter().map(|_| BackendResult::default()).collect())
        }
        async fn write(&self, _: &TenantId, _: &Fact) -> Result<WriteAck, BackendError> {
            panic!("adapter bug");
        }
        async fn health(&self) -> Health {
            Health::Up
        }
    }

    /// A panicking half-open probe must reopen the breaker, not leave the probe slot
    /// claimed forever.
    #[tokio::test(start_paused = true)]
    async fn adapter_panics_are_failures_not_crashes() {
        let r = Resilient::new(
            Panicky {
                left: 2.into(),
                calls: 0.into(),
            },
            RetryPolicy::default(),
            BreakerPolicy {
                failure_threshold: 1,
                open_for: Duration::from_secs(10),
            },
            Duration::from_secs(2),
        );
        let e = r.query_batch(&t(), &q()).await.unwrap_err();
        assert!(matches!(e, BackendError::Unavailable(_)), "{e:?}");
        assert_eq!(r.inner().calls.load(Ordering::SeqCst), 1, "not retried");
        assert_eq!((r.state(), r.panics()), (BreakerState::Open, 1));
        tokio::time::advance(Duration::from_secs(11)).await;
        assert!(r.query_batch(&t(), &q()).await.is_err(), "probe panics");
        assert_eq!(r.state(), BreakerState::Open, "probe failure reopens");
        tokio::time::advance(Duration::from_secs(11)).await;
        assert!(
            r.query_batch(&t(), &q()).await.is_ok(),
            "next probe allowed"
        );
        assert_eq!(r.state(), BreakerState::Closed);
        let e = r.write(&t(), &fact(None)).await.unwrap_err();
        assert!(matches!(e, BackendError::Unavailable(_)), "writes too");
    }

    #[test]
    fn backoff_is_bounded() {
        let r = wrap(5);
        for attempt in 0..20 {
            assert!(r.backoff(attempt) <= RetryPolicy::default().max_delay);
        }
    }
}
