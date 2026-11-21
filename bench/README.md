# bench/

Benchmark datasets, traces and published memharness reports. Raw run output goes in
`bench/raw/`, which is not committed.

## Datasets

`memharness datagen` writes a synthetic agent memory into `bench/datasets/<name>/`:

- `memories.jsonl`: one memory per line (tenant, text, entities, label, `as_of`, and a
  letters-only word unique to that memory). **Not committed**: it is regenerated from its
  seed.
- `manifest.json`: scale, seed, generator version, counts, and the blake3 hash of the JSONL.
  **Committed.** `datagen` refuses to overwrite a manifest with different output, and
  `memharness load` refuses a JSONL that does not match its manifest.

The name is `<scale>-s<seed>-v<generator version>`, and tenant ids are `<name>-<n>`, so
datasets never share tenants. Generation is a pure function of scale and seed on every
platform (SplitMix64, integer-only sampling); `crates/memharness/tests/datagen.rs` pins the
hash of the `tiny` dataset.

| Scale | Tenants | Memories per tenant | Entities | JSONL size | Generation |
|---|---|---|---|---|---|
| tiny | 2 | 100 | 160 | 52 KB | instant (tests) |
| small | 2 | 5,000 | 3,693 | 2.6 MB | 0.1 s |
| medium | 4 | 50,000 | 76,674 | 53 MB | 1.3 s |
| large | 8 | 250,000 | 788,480 | 526 MB | 18 s |

What the data looks like, and why, is documented in `crates/memharness/src/datagen.rs`.

## Loading

With the local stack up (`just up`) and **BAiD stopped**:

```sh
just load               # small, seed 42, into Neo4j and SeaweedFS, then --verify
just load medium 7      # another scale or seed
```

Loading writes straight to the backends through the adapters, bypassing BAiD's write log,
so a running BAiD could keep serving results it cached before the load. `memharness load`
refuses while anything listens on BAiD's ports (127.0.0.1:7070 and :7071); after flushing
BAiD's caches for the dataset's tenants, pass `--allow-running-baid`.

Each fact carries the idempotency key `load:<dataset>:<n>`: re-running a load, or resuming
one that failed, stores nothing twice. `--verify` samples rows and entities per tenant and
checks neighbourhoods, entity lookups, unique-word searches and tenant isolation against the
dataset, then prints what it checked.

Measured load rates (local Docker on Windows, 16 writes in flight, one transaction per fact):

| Backend | small (10,000) | medium (200,000), extrapolated | large (2,000,000), extrapolated |
|---|---|---|---|
| Neo4j | 108 rows/s, 93 s | ~31 min | ~5 h |
| SeaweedFS | 333 rows/s, 30 s | ~10 min | ~1.7 h |

Extrapolations are linear and therefore optimistic for Neo4j. More writes in flight do not
help Neo4j (64 in flight gave ~117 rows/s with a p99 of 8 s: writes queue on the locks of hub
entities). Bulk loading is a T27 follow-up (plan G26).
