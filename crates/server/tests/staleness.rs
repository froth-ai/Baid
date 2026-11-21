//! The staleness property (Code architecture section 6.2).
//!
//! Random traces interleave writes and reads across sessions against the real engine (RAM
//! cache, write log, single-flight, invalidator). A checker then asserts:
//!
//! 1. **No stale read after an acknowledged write.** A read filtered on entity `E` that
//!    *started* after a write to `E` was acknowledged returns that write.
//! 2. **Read-your-writes.** Any read by the writing session (filtered or not) that started
//!    after its own write was acknowledged returns that write.
//!
//! Unfiltered reads by *other* sessions are not held to rule 1: a write can add a result
//! to a search they cached, which is visible within the TTL (G14, documented).
//!
//! Race windows are widened on purpose: the backend reads its data at once and replies
//! after an injected delay (so a load can carry pre-write data), and writes are delayed
//! before they apply. "Before" and "after" come from one shared counter that both
//! acknowledgements and read starts increment, so the ordering is exact.
//!
//! The main property runs on a single-threaded runtime with a paused clock: every case is
//! deterministic and proptest can shrink a failure to a minimal trace. A smaller variant
//! runs on real threads and real time for genuine parallelism.
//!
//! **Outage profile (T17).** The same rules must hold while the backend fails. Here the TTL
//! is 10 ms and the stale-if-error window 10 ms, so entries expire within a trace, and a
//! generated schedule makes some backend reads fail. Failed recalls are left out; every
//! answer that came back, stale ones included, is held to rules 1 and 2, and additionally
//! (3) a hit is at most the TTL old and a stale answer at most TTL + window old.

#![allow(clippy::unwrap_used)] // test file

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adapters::Registry;
use async_trait::async_trait;
use baid_core::CacheOutcome;
use baid_core::{
    AdapterId, Backend, BackendError, BackendQuery, BackendResult, Capabilities, Config, EntityId,
    Fact, Filters, Health, MemoryBackend, RecallRequest, SessionId, TenantId, TokenBudget,
    WriteAck,
};
use cache::{Clock, RamStore, TierConfig};
use pack::TruncatingPacker;
use proptest::prelude::*;
use server::auth::Tenant;
use server::engine::Engine;
use session::Sessions;

/// Entity names: whole words, so a search for one never matches another.
const ENTITIES: [&str; 3] = ["alpha", "bravo", "charlie"];

/// The cache's clock: tokio's, so a paused runtime ages entries as the engine sees them.
struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> std::time::Instant {
        tokio::time::Instant::now().into_std()
    }
}

/// A memory backend that reads at once and replies late, with delays taken in order from a
/// generated list (so a paused-clock run is reproducible). Reads fail when the generated
/// failure schedule says so.
struct Delayed {
    inner: MemoryBackend,
    delays: Mutex<VecDeque<u8>>,
    failures: Mutex<VecDeque<bool>>,
}

impl Delayed {
    fn next_delay(&self) -> Duration {
        let ms = self.delays.lock().unwrap().pop_front().unwrap_or(0);
        Duration::from_millis(u64::from(ms))
    }
}

#[async_trait]
impl Backend for Delayed {
    fn id(&self) -> AdapterId {
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
        let answer = self.inner.query_now(t, qs); // data as of now ...
        let fail = self.failures.lock().unwrap().pop_front().unwrap_or(false);
        tokio::time::sleep(self.next_delay()).await; // ... delivered later
        if fail {
            return Err(BackendError::Unavailable("injected outage".into()));
        }
        Ok(answer)
    }
    async fn write(&self, t: &TenantId, f: &Fact) -> Result<WriteAck, BackendError> {
        tokio::time::sleep(self.next_delay()).await;
        Ok(self.inner.write_now(t, f))
    }
    async fn health(&self) -> Health {
        Health::Up
    }
}

#[derive(Clone, Debug)]
enum Act {
    Write { entity: usize },
    Read { entity: usize, filtered: bool },
}

#[derive(Clone, Debug)]
struct Step {
    act: Act,
    /// Think time before this step, in ms.
    pause_ms: u8,
}

#[derive(Clone, Debug)]
struct Trace {
    sessions: Vec<Vec<Step>>,
    backend_delays: Vec<u8>,
    /// Which backend reads fail, in call order (empty: none do).
    failures: Vec<bool>,
}

/// Tenant settings for a run.
#[derive(Clone, Copy, Debug)]
struct Profile {
    ttl: Duration,
    stale_if_error: Duration,
}

/// T16: entries never expire within a trace; the backend never fails.
const STEADY: Profile = Profile {
    ttl: Duration::from_secs(300),
    stale_if_error: Duration::from_secs(30),
};

/// T17: entries expire within a trace, and the backend fails on a schedule.
const OUTAGE: Profile = Profile {
    ttl: Duration::from_millis(10),
    stale_if_error: Duration::from_millis(10),
};

/// One step; `reads_per_write` sets the mix.
fn step_with(reads_per_write: u32) -> impl Strategy<Value = Step> {
    let act = prop_oneof![
        1 => (0..ENTITIES.len()).prop_map(|entity| Act::Write { entity }),
        reads_per_write => (0..ENTITIES.len(), any::<bool>())
            .prop_map(|(entity, filtered)| Act::Read { entity, filtered }),
    ];
    (act, 0u8..4).prop_map(|(act, pause_ms)| Step { act, pause_ms })
}

fn step() -> impl Strategy<Value = Step> {
    step_with(1)
}

fn trace() -> impl Strategy<Value = Trace> {
    (
        prop::collection::vec(prop::collection::vec(step(), 1..9), 2..5),
        prop::collection::vec(0u8..6, 0..80),
    )
        .prop_map(|(sessions, backend_delays)| Trace {
            sessions,
            backend_delays,
            failures: Vec::new(),
        })
}

/// A read-heavy trace (four reads per write, so entries live long enough to expire) with a
/// backend failure schedule in which about two reads in five fail.
fn outage_trace() -> impl Strategy<Value = Trace> {
    (
        prop::collection::vec(prop::collection::vec(step_with(4), 1..12), 2..5),
        prop::collection::vec(0u8..6, 0..80),
        prop::collection::vec(prop::bool::weighted(0.4), 0..80),
    )
        .prop_map(|(sessions, backend_delays, failures)| Trace {
            sessions,
            backend_delays,
            failures,
        })
}

#[derive(Debug)]
struct WriteRec {
    session: usize,
    entity: usize,
    value: u64,
    acked_at: u64,
}

#[derive(Debug)]
struct ReadRec {
    session: usize,
    entity: usize,
    filtered: bool,
    started_at: u64,
    values: HashSet<u64>,
    outcome: CacheOutcome,
    age: Option<Duration>,
}

/// The tenant, built without `validate()` so the outage profile may use a sub-second TTL.
fn tenant(p: Profile) -> Tenant {
    let config: Config = toml::from_str(&format!(
        "[[tenants]]\nid = \"acme\"\napi_key_env = \"K\"\nadapter = \"memory\"\nadapter_url = \"memory://\"\nbackend_timeout = \"30s\"\nttl = \"{}ms\"\nstale_if_error = \"{}ms\"\n",
        p.ttl.as_millis(),
        p.stale_if_error.as_millis(),
    ))
    .unwrap();
    Tenant {
        id: config.tenants[0].id.clone(),
        config: Arc::new(config.tenants[0].clone()),
    }
}

/// Runs the trace: sessions concurrently, each one's steps in order.
async fn run(trace: &Trace, profile: Profile) -> (Vec<WriteRec>, Vec<ReadRec>) {
    let t = tenant(profile);
    let store = RamStore::new(Arc::new(TokioClock));
    store.add_tenant(t.id.clone(), TierConfig::with_capacity(16 << 20));
    let backend = Arc::new(Delayed {
        inner: MemoryBackend::new(),
        delays: Mutex::new(trace.backend_delays.iter().copied().collect()),
        failures: Mutex::new(trace.failures.iter().copied().collect()),
    });
    let mut reg = Registry::new();
    reg.insert(t.id.clone(), backend);
    let engine = Arc::new(Engine::new(
        Arc::new(store),
        reg,
        Arc::new(TruncatingPacker),
        Arc::new(Sessions::new()),
    ));
    let clock = Arc::new(AtomicU64::new(0));
    let next_value = Arc::new(AtomicU64::new(1));

    let sessions = trace.sessions.iter().enumerate().map(|(si, steps)| {
        let (engine, clock, next_value, t) =
            (engine.clone(), clock.clone(), next_value.clone(), t.clone());
        async move {
            let sid = SessionId::new(format!("s{si}")).unwrap();
            let (mut writes, mut reads) = (Vec::new(), Vec::new());
            for st in steps {
                tokio::time::sleep(Duration::from_millis(u64::from(st.pause_ms))).await;
                match st.act {
                    Act::Write { entity } => {
                        let value = next_value.fetch_add(1, Ordering::SeqCst);
                        let fact = Fact {
                            text: format!("{} v{value}", ENTITIES[entity]),
                            entities: vec![EntityId::new(ENTITIES[entity]).unwrap()],
                            labels: vec![],
                            as_of: None,
                            idempotency_key: None,
                        };
                        engine.remember(&t, &sid, fact).await.unwrap();
                        let acked_at = clock.fetch_add(1, Ordering::SeqCst) + 1;
                        writes.push(WriteRec {
                            session: si,
                            entity,
                            value,
                            acked_at,
                        });
                    }
                    Act::Read { entity, filtered } => {
                        let started_at = clock.fetch_add(1, Ordering::SeqCst) + 1;
                        let filters = if filtered {
                            Filters {
                                entity_ids: vec![EntityId::new(ENTITIES[entity]).unwrap()],
                                ..Filters::default()
                            }
                        } else {
                            Filters::default()
                        };
                        let req = RecallRequest {
                            session: sid.clone(),
                            query: ENTITIES[entity].into(),
                            budget: TokenBudget::default(),
                            filters,
                            max_staleness: None,
                        };
                        let r = match engine.recall(&t, req).await {
                            Ok(r) => r,
                            // An outage with nothing servable: an error, not an answer.
                            Err(e) if e.code() == "backend_unavailable" => continue,
                            Err(e) => panic!("unexpected recall error: {e:?}"),
                        };
                        let values = r
                            .packed
                            .items
                            .iter()
                            .map(|i| {
                                let (name, v) = i.text.split_once(" v").unwrap();
                                assert_eq!(
                                    name, ENTITIES[entity],
                                    "a read returned another entity's fact"
                                );
                                v.parse::<u64>().unwrap()
                            })
                            .collect();
                        reads.push(ReadRec {
                            session: si,
                            entity,
                            filtered,
                            started_at,
                            values,
                            outcome: r.outcome,
                            age: r.age,
                        });
                    }
                }
            }
            (writes, reads)
        }
    });
    let results = futures::future::join_all(sessions).await;
    let (mut writes, mut reads) = (Vec::new(), Vec::new());
    for (w, r) in results {
        writes.extend(w);
        reads.extend(r);
    }
    (writes, reads)
}

/// Every violation of the rules, as readable messages. `slack` absorbs the gap between the
/// cache's and the engine's clock reads on real threads (zero on a paused clock).
fn violations(
    writes: &[WriteRec],
    reads: &[ReadRec],
    profile: Profile,
    slack: Duration,
) -> Vec<String> {
    let mut out = Vec::new();
    for r in reads {
        let limit = match r.outcome {
            CacheOutcome::Hit => Some(profile.ttl),
            CacheOutcome::Stale => Some(profile.ttl + profile.stale_if_error),
            _ => None,
        };
        if let Some(limit) = limit {
            match r.age {
                Some(age) if age <= limit + slack => {}
                age => out.push(format!(
                    "rule 3: s{} got a {:?} answer aged {age:?}, limit {limit:?}",
                    r.session, r.outcome
                )),
            }
        }
        for w in writes
            .iter()
            .filter(|w| w.entity == r.entity && w.acked_at < r.started_at)
        {
            if r.values.contains(&w.value) {
                continue;
            }
            if r.filtered {
                out.push(format!(
                    "rule 1: s{} read {} (filtered) at t={} without v{}, acked by s{} at t={}",
                    r.session, ENTITIES[r.entity], r.started_at, w.value, w.session, w.acked_at
                ));
            } else if w.session == r.session {
                out.push(format!(
                    "rule 2: s{} read {} at t={} without its own v{}, acked at t={}",
                    r.session, ENTITIES[r.entity], r.started_at, w.value, w.acked_at
                ));
            }
        }
    }
    out
}

/// Cases per property: `BAID_PROPTEST_CASES` if set (the nightly run uses a large value),
/// otherwise `default`.
fn cases(default: u32) -> u32 {
    std::env::var("BAID_PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn check(
    trace: &Trace,
    profile: Profile,
    rt: tokio::runtime::Runtime,
    slack: Duration,
) -> Result<(), TestCaseError> {
    let (writes, reads) = rt.block_on(run(trace, profile));
    let v = violations(&writes, &reads, profile, slack);
    prop_assert!(v.is_empty(), "{} violation(s):\n{}", v.len(), v.join("\n"));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(256), ..ProptestConfig::default() })]

    /// Single thread, paused clock: deterministic, so failures shrink to a minimal trace.
    #[test]
    fn no_stale_reads_deterministic(trace in trace()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        check(&trace, STEADY, rt, Duration::ZERO)?;
    }

    /// The outage profile, deterministic: stale answers obey the same rules.
    #[test]
    fn no_stale_reads_during_outages_deterministic(trace in outage_trace()) {
        check(&trace, OUTAGE, paused(), Duration::ZERO)?;
    }
}

fn paused() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

fn threaded() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(32), ..ProptestConfig::default() })]

    /// Four worker threads, real time: genuine parallelism, at a higher cost per case.
    #[test]
    fn no_stale_reads_parallel(trace in trace()) {
        check(&trace, STEADY, threaded(), Duration::ZERO)?;
    }

    /// The outage profile on real threads.
    #[test]
    fn no_stale_reads_during_outages_parallel(trace in outage_trace()) {
        check(&trace, OUTAGE, threaded(), Duration::from_millis(20))?;
    }
}

/// The outage profile must actually exercise what it claims to: across a fixed set of
/// generated traces, some answers are stale, some are hits, and some recalls fail.
#[test]
fn the_outage_profile_reaches_stale_answers() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let (mut stale, mut hits, mut reads) = (0, 0, 0);
    for _ in 0..64 {
        let trace = outage_trace().new_tree(&mut runner).unwrap().current();
        let (_, rs) = paused().block_on(run(&trace, OUTAGE));
        reads += rs.len();
        stale += rs
            .iter()
            .filter(|r| r.outcome == CacheOutcome::Stale)
            .count();
        hits += rs.iter().filter(|r| r.outcome == CacheOutcome::Hit).count();
    }
    eprintln!("outage profile: {reads} answers, {stale} stale, {hits} hits");
    assert!(
        stale > 0 && hits > 0,
        "stale {stale}, hits {hits} of {reads}"
    );
}

/// The checker itself must catch a stale read, or the properties above prove nothing.
#[test]
fn the_checker_flags_stale_reads() {
    let writes = [WriteRec {
        session: 0,
        entity: 0,
        value: 7,
        acked_at: 1,
    }];
    let stale_filtered = ReadRec {
        session: 1,
        entity: 0,
        filtered: true,
        started_at: 2,
        values: HashSet::new(),
        outcome: CacheOutcome::Miss,
        age: None,
    };
    let stale_own = ReadRec {
        session: 0,
        entity: 0,
        filtered: false,
        started_at: 2,
        values: HashSet::new(),
        outcome: CacheOutcome::Miss,
        age: None,
    };
    let concurrent = ReadRec {
        session: 1,
        entity: 0,
        filtered: true,
        started_at: 1,
        values: HashSet::new(),
        outcome: CacheOutcome::Miss,
        age: None,
    };
    let other_unfiltered = ReadRec {
        session: 1,
        entity: 0,
        filtered: false,
        started_at: 2,
        values: HashSet::new(),
        outcome: CacheOutcome::Miss,
        age: None,
    };
    let v = |r: ReadRec| violations(&writes, &[r], OUTAGE, Duration::ZERO).len();
    let aged = |outcome, ms| ReadRec {
        session: 1,
        entity: 1,
        filtered: true,
        started_at: 0,
        values: HashSet::new(),
        outcome,
        age: Some(Duration::from_millis(ms)),
    };
    assert_eq!(v(stale_filtered), 1);
    assert_eq!(v(stale_own), 1);
    assert_eq!(v(concurrent), 0, "read started before the ack");
    assert_eq!(v(other_unfiltered), 0, "G14 exception");
    assert_eq!(v(aged(CacheOutcome::Hit, 10)), 0);
    assert_eq!(v(aged(CacheOutcome::Hit, 11)), 1, "hit older than the ttl");
    assert_eq!(v(aged(CacheOutcome::Stale, 20)), 0);
    assert_eq!(v(aged(CacheOutcome::Stale, 21)), 1, "stale past the window");
}
