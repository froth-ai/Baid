# BAiD build plan

Sources: *Architecture and design* and *Code architecture* PDFs, plus the repo scaffolding as of
2026-11-17. One track: tasks run in order, and each ends
with `just check` green.

Status key: `[ ]` todo, `[~]` in progress, `[x]` done.

## Current status (2026-11-20)

- **Done:** T01–T18 (foundation, vertical slice, adapters, correctness, fail open, datasets).
  Live adapter and loader tests: `just test-live` and `.github/workflows/contract.yml`.
- **Blocking T19:** **G29**, a Bolt connection desync under load (owner decision). Replay in T19 drives the Bolt adapter under load, so G29 should be fixed first.
- **Next:** **T19**, tracegen and replay mode `off`. Not started.
- **Open follow-ups** (no action until the trigger happens):
  - neo4rs 0.9 stable: switch, then drop the `deny.toml` ignores it no longer needs (G18;
    see also G29).
  - foyer: on every upgrade run `cargo tree -i paste`; drop RUSTSEC-2024-0436 once gone (G12).
  - object_store: drop the `humantime` ignore RUSTSEC-2026-0542 once it stops depending on
    it (G20).
  - T27: bulk loading for `medium`/`large` (G26: ~31 min and ~5 h into Neo4j today).

## 1. Spec gaps and the decisions taken

| # | Issue | Decision |
|---|---|---|
| G1 | Race in the stale-put guard: put checks versions → writer bumps → writer evicts (key not there yet) → put inserts stale data. | Done in T06: `RamStore` inserts, then re-checks versions and removes its own entry if any moved. Covered by a deterministic test and a 2,000-round threaded race test; both fail if the re-check is removed. |
| G2 | The engine calls `cache.promote()` and `cache.contains()`; the trait lacks both. | Done: `CacheStore` has `contains`, `promote` and `remove`. `evict_entity` is dropped: the invalidator owns the entity→keys index and calls `remove`. The staleness API is now `write_seq` / `mark_written` (G13, ADR-011), replacing the first `snapshot_versions` / `bump_versions` design. |
| G3 | The `Predictor` trait uses `SessionView`, `TenantStats` and `QueryClass`. | Define them in baid-core. |
| G4 | `Key::derive` takes both `kind` and a query, but the query already implies the kind. | Derive the kind from the query. |
| G5 | baid-core needs `async-trait`, `time` and `unicode-normalization`. | Add them; none does I/O. |
| G6 | `entity_ver` lives in cache (`versions.rs`), and invalidate bumps it. | Superseded by G13: the cache owns the write log; invalidate only evicts through the `CacheStore` trait and needs no cache-crate dependency. |
| G7 | mcp must call the engine without depending on cache or exec. | Add a `MemoryService` trait in baid-core at T32 (with an ADR). |
| G8 | The project was renamed from GraphCache to BAiD. | Done: crate folders drop the `gc-` prefix, the core package is `baid-core` (`core` clashes with Rust's built-in crate), keys are `baid:v1:`, env vars will be `BAID_`, metrics `baid_`, the binary is `baid`. |
| G9 | Can foyer honour custom priority (ADR-005)? | Spike done: two levels only (`Hint::Low` is evicted first). One foyer LRU per tenant; speculative → Low, normal and hub → Normal. foyer admits oversized entries, so BAiD refuses entries over 1/4 of a shard. ADR-005 is Accepted. |
| G10 | The dependency table in the PDF is garbled. | The layer-check xtask (T05) becomes the source of truth. |
| G11 | The SRS is not used. | Targets come from the two spec tabs and this plan. |
| G12 | foyer pulls in `paste` (RUSTSEC-2024-0436: archived, unmaintained, no known vulnerability), which failed `cargo deny check`. | **Resolved** with one scoped ignore in `deny.toml` (owner-approved, 2026-11-17). Follow-up: recheck on every foyer upgrade (`cargo tree -i paste`) and remove the ignore once foyer drops `paste`. |
| G13 | **Hole in the spec's version guard:** versions are snapshotted only for entities the *query* names. A write to an entity that appears only in the *results* (search "renewal dates" returns acme items) passes the put check, and stale data is served until the TTL. | Done in T08: per-tenant **write sequence numbers**. A write takes the next number after the ack and stamps each entity it touched; an entry stores the number read before its load; it is refused on put **and on every get** if any of its entities (query or results) was stamped later. Correctness no longer depends on the invalidator, which becomes a memory optimisation (T14). |
| G14 | A write can *add* a result to a search another session cached (a new fact that now matches); no entity check can see that. | Policy, pinned by tests: the writing session never sees a cached entry older than its own last write (read-your-writes); other sessions see additions within the TTL. Documented in the README. |
| G15 | Ambiguous write failures (timeout, reset) may still have been applied. | The engine stamps the fact's entities as written on any failure except `Rejected`: costs misses, never a wrong answer. |
| G16 | MinIO no longer publishes container images (`minio/minio` is gone from Docker Hub and quay.io), so the local stack cannot pin it. | The local S3 store is **SeaweedFS** (`chrislusf/seaweedfs:3.97`, Apache-2.0, one container, S3 gateway on :8333). The S3 adapter speaks plain S3, so any compatible store works. |
| G17 | neo4rs 0.8 retries internally for up to 60 s, including on authentication failures, and cannot be configured; and an outer timeout that drops a call hides the failure from the circuit breaker. | The Bolt adapter uses explicit transactions (never retried by the driver), so errors come back at once and `Resilient` is the only retry layer. Every call has one deadline shared by attempts and backoff, recorded as a failure when it runs out. The engine's own timeout is a backstop at `backend_timeout + 250ms`. |
| G18 | neo4rs 0.8 pulls in six crates flagged *unmaintained* (backoff, instant, chrono, chrono-tz, chrono-tz-build, rustls-pemfile) and webpki-roots under CDLA-Permissive-2.0, which is not on the licence allow-list. No vulnerabilities. neo4rs 0.9.0-rc.11 drops backoff, instant, rustls-pemfile and webpki-roots but keeps chrono and chrono-tz. | **Resolved for 0.8** (owner-approved, 2026-11-17): one scoped ignore per advisory in `deny.toml` (RUSTSEC-2025-0012 backoff, RUSTSEC-2024-0384 instant, RUSTSEC-2026-0543 chrono, RUSTSEC-2026-0555 chrono-tz, RUSTSEC-2026-0559 chrono-tz-build, RUSTSEC-2025-0134 rustls-pemfile) and a `[[licenses.exceptions]]` entry allowing CDLA-Permissive-2.0 for webpki-roots only. Checked first: every one is transitive via neo4rs only, none is a BAiD dependency. Follow-up: switch to neo4rs 0.9 once it is a stable release, then drop the ignores it no longer needs (0.9.0-rc.11 already drops backoff, instant, rustls-pemfile and webpki-roots). |
| G19 | S3 has no query engine; object_store retries internally (10 retries, up to 3 min) by default. | Entity-scoped queries read hashed per-entity marker objects (cost grows with the entity, not the tenant). Unfiltered search scans only the newest 1,000 memories (`S3_SCAN_LIMIT`), documented in `s3.rs`. Client retries set to 0 and requests bounded by the tenant's backend timeout, so `Resilient` is the only retry layer (as G17). |
| G20 | object_store adds `humantime` (RUSTSEC-2026-0542, unmaintained, a non-optional object_store dependency) and `webpki-root-certs` (CDLA-Permissive-2.0, via reqwest → rustls-platform-verifier). No vulnerabilities. | **Resolved** (owner-approved, 2026-11-17): RUSTSEC-2026-0542 ignored with a scoped reason, and a `[[licenses.exceptions]]` entry allowing CDLA-Permissive-2.0 for webpki-root-certs only. Both proven transitive via object_store only. Follow-up: drop the humantime ignore when object_store stops depending on it. |
| G21 | Sharing a load between concurrent misses (spec 3.2) can hand a follower data loaded before a write it must see: its own last write, or another session's acknowledged write to an entity in the results. And a shared future abandoned by all waiters would be resumed later with an expired deadline. | Followers never use the shared result directly: they re-read through the cache (full write-log check plus the session floor) and load directly if refused. Flights count waiters; the last one to leave removes the entry and drops the load. Both are pinned by tests that fail under mutation. |
| G22 | CI failure (2026-11-18): on a fresh Neo4j, backends creating the schema at the same moment deadlock (`Neo.TransientError.Transaction.DeadlockDetected` on a label lock). Concurrent `MERGE`s of one entity can do the same. Hidden locally because the schema already existed. | The Bolt adapter retries a transaction (5 attempts, 10-160 ms jittered backoff, within the call timeout) when Neo4j reports `Neo.TransientError.*`, which means rolled back and safe to retry, even for writes without an idempotency key. Connection and auth errors still go to `Resilient` only. Regression test `tests/bolt_fresh_schema.rs` drops the schema and has 8 new backends write at once; it fails 3/3 without the retry. |
| G23 | Stale-if-error (spec §5, §12) must not weaken the staleness guarantees. | Owner-approved 2026-11-19. When the recall's own load fails with `Timeout`, `Unavailable` or `CircuitOpen` (never `Rejected`), serve an entry up to `stale_if_error` past its expiry through `CacheStore::get_stale`, which skips only the TTL check and still applies the write log; the engine still applies read-your-writes and the client's `max_staleness` (`0` never gets a stale answer). Default 30 s, validated `<= min(ttl, 120s)`. The response says `cache: "stale"` and gives the entry's age (`age_ms`). Bounds that grow from TTL to TTL + window while the backend fails: writes BAiD never sees, and additions to other sessions' cached searches (G14). |
| G24 | `mark_written` (or the session's write record) can fail *after* the backend acknowledged a write: the write log then lacks stamps, so cached pre-write data would pass every check. | Owner-approved 2026-11-19. Correctness beats fail-open: return the ack, then put the tenant into cache bypass (every recall goes straight to the backend). Bypass exits only after a successful flush of the tenant's cache **and** an engine-side epoch bump, so loads that started before the bypass cannot put their results back. While in bypass, stale-if-error is off for the tenant. A flush that keeps failing keeps the tenant in bypass (correct, slow), counted in a metric and reported by `/readyz`. |
| G25 | `/readyz` while a backend's breaker is open or a tenant is in G24 bypass. | Owner-approved 2026-11-19. The public `/readyz` returns 200 `{"status":"degraded"}` with no tenant ids or backend details (it is unauthenticated); 503 stays for draining only, so a backend outage does not pull every replica. The admin `/readyz` lists the affected tenants and why (`circuit_open`, `circuit_half_open`, `cache_bypass`) and any bypassed internal components. `/healthz` is unaffected. |
| G26 | The spec's dependency table has memharness write to Neo4j and S3 with neo4rs and object_store directly, which would duplicate each adapter's storage layout. | Owner-approved 2026-11-20. The loaders write through `adapters` (`Backend::write` inside `Resilient`), so a loaded dataset is exactly what BAiD reads and `Resilient` stays the only retry layer; memharness still never depends on `server` internals. Each fact carries the idempotency key `load:<dataset>:<n>`, so a re-run or resumed load adds nothing twice. One transaction per fact. **Measured** on `small` (local Docker on Windows, 16 in flight): Neo4j 108 rows/s, SeaweedFS 333 rows/s; extrapolated linearly, `medium` takes ~31 min (Neo4j) and ~10 min (S3), `large` ~5 h and ~1.7 h. More concurrency does not help Neo4j (64 in flight: ~117 rows/s, p99 8 s, writes queue on hub-entity locks). **Follow-up for T27:** bulk loading (Bolt `UNWIND` batches grouped to limit hub-lock contention, parallel S3 puts); not built in T18. Loading bypasses BAiD's write log, so it must happen with BAiD stopped (or its caches flushed); `memharness load` refuses while a BAiD server answers on the local ports unless told otherwise. |
| G27 | The dataset must stay identical over time and across platforms; `rand` does not promise stable output across versions, and libm float functions (`powf`, `ln`, `exp`) can differ between Windows and Linux. | Owner-approved 2026-11-20. memharness has its own SplitMix64, tested against the published reference vectors. Every sampling step is integer-only (Lemire's unbiased bounded integers, cumulative integer weight tables for the power laws). A test pins the blake3 hash of the `tiny` dataset; it runs locally and in Linux CI, so a platform difference fails at once. No new crates. |
| G28 | Datasets are large and derived from a seed. | Owner-approved 2026-11-20. `bench/datasets/*/memories.jsonl` is git-ignored; only each `manifest.json` is committed. `memharness load` checks the JSONL against the manifest's blake3 hash and refuses on mismatch, saying to regenerate. |
| G29 | **Found in T18 (2026-11-20), affects the Bolt adapter (T11) and so the server.** Under load a pooled neo4rs 0.8 connection can fall out of step with Neo4j's replies. When a call is dropped by its timeout while waiting for a `SUCCESS` (e.g. of `COMMIT`), the pool recycles the connection by sending `RESET` and reading exactly one reply: it consumes the stale `SUCCESS`, leaves `RESET`'s own reply unread, and every later reply on that connection arrives one message late. Reproduced in 2 of 2 runs of 3,000 raw writes at 64 in flight with a 2 s call timeout: `unexpected response for COMMIT: Record([<memory id>, 1])` (a `COMMIT` received a result row) and `write returned no row` (a `PULL` received a stale `SUCCESS`, so the transaction returned Ok with zero rows). A timeout while waiting on `PULL` was safe (the stale reply is a record, the recycle fails and the connection is discarded), and so is reuse after a failed statement. Risk for BAiD: a read on an out-of-step connection can return Ok with zero rows, which the engine would cache as the answer, and could in principle return the rows of an earlier query on that connection, possibly another tenant's (not observed). Related: a transient error (deadlock) reported during `PULL` surfaces as `UnexpectedMessage`, not as a Neo4j error, so the G22 transient retry does not see it. neo4rs 0.9.0-rc.11 drains pending replies before `RESET` and discards connections it cannot drain. | **Open: owner decision.** Options: (A) never abandon a Bolt exchange: each transaction runs to completion on its own task and the caller's timeout only stops waiting (orphans bounded by Neo4j's transaction timeout); (B) move to neo4rs 0.9.0-rc.11 now (a pre-release; API changes; would also retire four G18 ignores); (C) patch 0.8 through a fork. In any case (D): every Bolt read returns `m.tenant` and the adapter refuses rows of another tenant, and transient codes inside `UnexpectedMessage` are retried as G22. The loaders are unaffected at their default 16 in flight in the runs so far (none in 10,000 rows), and `--verify` plus idempotent re-runs would catch it. Reproduction probes kept outside the repo until the fix. |

## 2. Tasks

### Phase 0: foundation (baid-core)
- [x] **T01** IDs and cache keys: `TenantId`, `SessionId`, `EntityId`, `NodeId`, `AdapterId`; `BackendQuery`, `Filters`, `TokenBudget`, budget buckets; `normalise()`; `Key::derive` (blake3) and the `baid:v1:` string form.
- [x] **T02** Domain types: `RecallRequest`, `MemoryItem`, `CacheEntry`, `CacheOutcome`, `Candidate`, `Fact`, `WriteAck`, `BackendResult`, `Health`, `Capabilities`, `PutOutcome`, `FlushScope`, `PackedContext`, `InvalidationReport`, `SessionView`, `TenantStats`, `QueryClass`.
- [x] **T03** Errors and config structs (serde), with `validate()`.
- [x] **T04** The five traits (plus G2) and a `MockBackend` behind the `test-util` feature.
- [x] **T05** Layer-check xtask; a test that every `tokio::spawn` site takes a `CancellationToken`.

### Phase 1: vertical slice (cache-only, against a mock backend)
- [x] **T06** foyer spike, then the RAM tier: TTL, per-tenant capacity, priority, version-guarded put (G1), promote. Isolation proptest.
- [x] **T07** Server skeleton: CLI, figment config, tenant from API key, `/healthz` `/readyz`, ApiError→HTTP, graceful shutdown.
- [x] **T08** Engine: recall (hit/miss) and remember pass-through; read-your-writes and `max_staleness=0` bypass.
- [x] **T09** `api/openapi.yaml` v1, with a contract test against the live router (`crates/server/tests/contract.rs`).
- [x] **T10** docker-compose: Neo4j 5.26 LTS, SeaweedFS (S3, replaces MinIO: G16), Prometheus, Grafana; `just up` / `down` / `ps` / `logs`. Verified: all healthy, Bolt query, signed S3 put/get/range read, wrong key refused, Grafana datasource provisioned.
- [x] **T11** Bolt adapter (`adapters::BoltBackend`, namespaced `BaidMemory`/`BaidEntity` graph, escaped full-text search, neighbourhoods, idempotent writes), adapter registry with credentials, and `Resilient` (retries, circuit breaker, shared deadline). Live tests: `just test-live`. Dependency advisories handled in G18.
- [x] **T12** S3 adapter (`adapters::S3Backend` on object_store: per-tenant prefixes, hashed entity markers, newest-first ids, bounded scan for unfiltered search) and the `Backend` contract suite (`crates/adapters/tests/contract.rs`: memory and in-memory S3 always; live Neo4j and SeaweedFS via `just test-live` and `.github/workflows/contract.yml`).

### Phase 2: correctness
- [x] **T13** Single-flight loads (`exec::SingleFlight`): concurrent misses on a key share one backend call; waiter counting cancels and forgets abandoned loads; followers re-read through the cache so sharing keeps read-your-writes and write invalidation (G21).
- [x] **T14** Invalidator (`invalidate::EntityIndex`): entity → dependent keys, filled by each stored load; a write (and an ambiguous failed write) evicts the dependents at write time. Bounded at 1,024 keys per entity and pruned every minute; safe to forget keys because reads re-check the write log (ADR-011).
- [x] **T15** Write-through remember (ack, then stamp entities); done in T08, including ambiguous failures (G15).
- [x] **T16** Staleness property (`crates/server/tests/staleness.rs`): random multi-session traces against the real engine, backend replies delayed after reading, checked for (1) no stale filtered read after an acknowledged write and (2) read-your-writes. Paused-clock variant (256 cases, deterministic, shrinks) plus a real-thread variant (32 cases); `BAID_PROPTEST_CASES` scales both for nightly. Catches four distinct injected bugs.
- [x] **T17** Fail open (`server::failopen`): panics in the cache, index, packer, session tracker and adapters are caught; optional work falls back (miss, no caching, `fallback_pack`), write records failing put the tenant into cache bypass until flushed (G24); a per-component breaker bypasses a component after 5 panics in 60 s for 30 s; `Resilient` turns adapter panics into recorded failures; a supervisor restarts background tasks; the HTTP layer answers an uncaught panic with a 500; `compile_error!` under `panic = "abort"`. Stale-if-error (G23) via `CacheStore::get_stale`, `cache: "stale"` and `age_ms`. `/readyz` `degraded` (G25). Counters for `baid_internal_errors_total`, `baid_component_trips_total`, `baid_stale_served_total`, `baid_cache_bypass_*`, `baid_task_restarts_total` (exported in T21). Tests: `crates/server/tests/failopen.rs` (22) and an outage profile in the staleness proptest; every guard mutation-tested.

### Phase 3: measuring stick (memharness)
- [x] **T18** Datagen and the Neo4j/S3 loaders (`just load`; G26-G28). `memharness datagen`: accounts with people, projects and tickets, power-law hubs (products, teams), letters-only unique words; SplitMix64 with integer-only sampling, `tiny` hash pinned. `memharness load`: through the adapters and `Resilient`, idempotency keys, refuses on a manifest mismatch or a running local BAiD; `--verify` reports what it checked. Manifests for small, medium and large in `bench/datasets/` (see `bench/README.md` for rates). Tests: `crates/memharness/tests/{datagen,load,live}.rs` and unit tests; 16 mutations, all caught. Found G29.
- [ ] **T19** Tracegen with think time; replay mode `off`; report (memory wait, tokens per task).
- [ ] **T20** Replay mode `cache-only`.

### Phase 4: prefetch
- [ ] **T21** Telemetry: metrics, Prometheus `/metrics`, tracing, OTLP feature.
- [ ] **T22** Session tracker: window, think-time EWMA, cancel tokens. (Already there from T08: per-session last write for read-your-writes, idle expiry via housekeeping.)
- [ ] **T23** Query classes and the neighbour predictor.
- [ ] **T24** Executor: deadlines, cancellation, task state metrics.
- [ ] **T25** Budget governor: token buckets, global cap, auto-off.
- [ ] **T26** Speculative entries; precision and waste metrics.
- [ ] **T27** Batched UNWIND (Bolt) and coalesced reads (S3).
- [ ] **T28** Replay mode `prefetch`, targeting a memory-wait cut of at least 50%.

### Phase 5: packing and prediction
- [ ] **T29** Packer: dedupe, rank, trim, stable order, tiktoken counts.
- [ ] **T30** Transition and popularity predictors, and the ensemble.
- [ ] **T31** Partial hits through neighbourhood entries.

### Phase 6: surfaces
- [ ] **T32** MCP server (rmcp): Streamable HTTP at `/mcp` and the stdio bridge.
- [ ] **T33** Python SDK.

### Phase 7: operations and launch
- [ ] **T34** Admin API, SIGHUP reload, freshness probe.
- [ ] **T35** Grafana dashboards and the README quick start.
- [ ] **T36** NVMe tier (foyer hybrid, postcard with a format byte).
- [ ] **T37** nightly and release CI.
- [ ] **T38** Full benchmark and write-up.
