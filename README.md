# BAiD

**A memory accelerator layer for AI agents.** BAiD sits between agents and the memory backend they
already use (a Neo4j graph, an S3 bucket, Zep, Mem0). It serves repeated recalls from a cache,
prefetches the next recall while the LLM is still thinking, and trims what comes back to the
agent's token budget. Agent tasks get faster and cheaper with no change to the database.

BAiD is a layer, not a database. It never holds source-of-truth data: everything it caches can
be dropped and rebuilt from the backend.

> **Status: pre-alpha.** `recall` and `remember` work end to end over HTTP against Neo4j
> (the `bolt` adapter) or the in-process `memory` adapter, with the RAM cache, API-key auth,
> retries and a circuit breaker, and the staleness guarantees below. S3 (and S3-compatible
> stores) work through the `s3` adapter. Prefetch and MCP are next. See the [build plan](docs/build-plan.md).

## Why

An agent loop looks like *think → recall → think → recall*. Every recall waits on a backend
round trip, often tens to hundreds of milliseconds, and often for a question this or another
session asked a moment ago. Meanwhile the LLM spends seconds thinking, and the backend sits
idle. BAiD uses that idle time:

- **Hits are memory lookups.** Repeated and shared questions are answered from RAM.
- **Prefetch hides latency.** While the agent thinks, BAiD predicts its next recalls and loads
  them, so the next recall is a hit.
- **Packing saves tokens.** Results are deduplicated, ranked and trimmed to a budget in a
  stable order, so the agent pays for less context.

## How it works

```text
              recall / remember (HTTP, MCP, Python SDK)
 Agent  ───────────────────────────────────────────────▶  BAiD
                                                           │
   read path      tenant from API key → cache key → RAM tier ──hit──▶ packer ──▶ agent
                                                    │ miss
                                                    ▼
                                    single-flight load ──▶ backend (one call per key)

   prefetch path  after the response: session tracker → predictor → budget governor
                  → batched backend call → cached as low-priority "speculative" entries

   write path     remember → backend write → ack → stamp entities with a write number
```

- **One contract per side.** Agents see two calls, `recall` and `remember`. Backends implement
  one adapter trait with capability flags.
- **Tenant isolation by construction.** The tenant comes only from the API key, and a cache
  key cannot be built without one, so a cross-tenant hit is a type error, not a bug to catch.
- **Writes are never undone by the cache.** Writes go through BAiD to the backend. After the
  backend acknowledges, BAiD stamps every entity the write touched with the tenant's next
  write number. Every cached result remembers the write number from when its load began, and
  is refused (on put and on every read) if any entity it names or returns was stamped later.
  So a slow read that started before a write can never serve pre-write data.
- **A session always sees its own writes.** After a session writes, cached results loaded
  before that write are misses for that session.
- **What the TTL bounds.** A write can also *add* a result to someone else's cached search
  (a new fact that now matches). Other sessions see such additions within the tenant's TTL,
  the same bound as writes made outside BAiD.
- **Stale-if-error.** While the backend times out, is unavailable or its circuit breaker is
  open, BAiD may answer from an entry that expired up to `stale_if_error` ago (default 30 s,
  at most 120 s and at most the TTL). The response says `"cache": "stale"` and gives the
  entry's age in `age_ms`. Only the TTL is relaxed: an entry a write invalidated is never
  served, a session never gets one older than its own last write, and `max_staleness` still
  applies (`0` never gets one). While the backend is failing, the two TTL bounds above become
  TTL + `stale_if_error`.
- **Speculation is bounded.** Prefetch spends a fixed per-tenant budget, gets a deadline equal
  to the session's think time, is cancelled the moment it stops being useful, and is the first
  thing cut under load. Wrong guesses are evicted first.
- **Fail open.** Any internal error falls back to calling the backend directly. BAiD can make
  an agent faster, never less available. A cache, index or packer that keeps failing is
  bypassed for a cooldown. If BAiD cannot record a write (so it could no longer tell which
  cached results that write made stale), that tenant's cache is bypassed until it has been
  flushed: slower, never wrong. `/readyz` says `degraded` (still 200) while a backend's breaker
  is open or a tenant's cache is bypassed; the admin listener's `/readyz` says which tenants
  and why. Panic messages are not logged unless `BAID_LOG_PANIC_MESSAGES=1`, because they can
  contain request data.

The principles, in priority order: never wrong, then fast; fail open; bounded speculation; the
backend stays the source of truth; one contract per side; measure before claiming.

## Repository layout

```text
crates/
  core/         baid-core: ids, cache keys, domain types, the five traits, config, errors
  cache/        RAM and NVMe tiers, TTL, priorities, the version-guarded put
  session/      per-session window, think-time estimate, expiry
  predict/      next-query predictors: neighbour, transition, popularity, ensemble
  pack/         dedupe, rank and trim to a token budget
  invalidate/   entity → key index; bump-then-evict on writes
  adapters/     Bolt (Neo4j), S3, later Zep and Mem0; retries, timeouts, circuit breaker
  telemetry/    metrics, traces, structured logs
  exec/         single-flight loads, prefetch executor, budget governor
  mcp/          MCP server: recall and remember tools, stdio bridge
  server/       the `baid` binary: HTTP API, admin API, wiring
  memharness/   benchmark harness: datagen, trace replay, reports
xtask/          repository checks (crate layering, cancellation of spawned tasks)
api/            OpenAPI contract for the HTTP API (planned)
python/         Python SDK (planned)
deploy/         docker-compose, Helm charts, Grafana dashboards (planned)
bench/          benchmark datasets (manifests), traces and published reports
docs/adr/       architecture decision records
docs/build-plan.md  task list, status and design decisions
```

Crates depend only on lower layers: `baid-core` → building blocks → `exec` / `mcp` →
binaries. `cargo xtask layers` enforces this in CI.

## Getting started (contributors)

Requirements: Rust stable 1.91 or newer (picked up from `rust-toolchain.toml`) and
[just](https://github.com/casey/just). Docker and
[cargo-deny](https://github.com/EmbarkStudios/cargo-deny) are needed for later steps.

```bash
just check        # fmt, clippy -D warnings, layer and spawn rules, all tests
cargo xtask all   # just the repository rules
just deny         # licences, advisories, bans
just up           # local Neo4j, SeaweedFS (S3), Prometheus, Grafana
just test-live    # adapter and loader tests against the local stack
just load         # generate the small benchmark dataset and load it into Neo4j and S3

# run the server against a config (each tenant's API key comes from its api_key_env)
cargo run -p server -- -c baid.toml check-config
cargo run -p server -- -c baid.toml
```

`just up` starts the local stack (Neo4j, SeaweedFS for S3, Prometheus, Grafana; see
[deploy/](deploy/README.md)). `just load` writes a synthetic dataset straight into its
backends, so run it with BAiD stopped (see [bench/](bench/README.md)). `just bench-small`
comes with trace replay.

## Configuration

One TOML file, layered under `BAID_*` environment variables and command-line flags, validated
before the server binds a port. Secrets are never written in the file; `*_env` fields name the
variable that holds them.

```toml
[server]
listen = "0.0.0.0:7070"
admin_listen = "127.0.0.1:7071"

[cache]
ram_bytes = "2GiB"
# nvme_dir = "/var/lib/baid"   # set to turn on the disk tier
# nvme_bytes = "20GiB"

[[tenants]]
id = "acme"
api_key_env = "BAID_KEY_ACME"
adapter = "bolt"
adapter_url = "bolt://neo4j:7687"
adapter_user_env = "NEO4J_USER"
adapter_password_env = "NEO4J_PASSWORD"
ttl = "5m"              # hard upper bound on staleness
stale_if_error = "30s"  # serve expired entries this long while the backend fails (max 120s)

[tenants.prefetch]
enabled = true
k = 4                   # candidates per recall
min_confidence = 0.35
calls_per_second = 20   # speculative call budget
max_concurrent = 4
safety_margin = "150ms" # subtracted from the think-time estimate
```

## Deployment shapes

One binary, three topologies; only the config changes.

| Topology | When | Notes |
| --- | --- | --- |
| Sidecar | One agent app, strict latency | RAM tier only, one tenant |
| Shared service | Many apps or users on one memory | NVMe tier on, many tenants, router hashes tenant then session |
| Local dev | Demos, benchmarks, contributors | docker-compose runs Neo4j and SeaweedFS (S3) |

## Documentation

- [Build plan](docs/build-plan.md): tasks, status, and departures from the spec
- [Architecture decisions](docs/adr): why Rust, why fail open, why tenant-from-key, and more
- [Contributing](CONTRIBUTING.md): branches, pull requests, code rules
- [Security](SECURITY.md): report issues privately; tenant isolation bugs are security bugs

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
