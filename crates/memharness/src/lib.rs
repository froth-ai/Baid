//! Benchmark harness: datagen, load, tracegen, replay and report. Talks to BAiD over HTTP
//! only; loads backends directly through the adapters crate (G26).
//!
//! - [`datagen`]: a synthetic memory graph, a pure function of scale and seed (G27).
//! - [`dataset`]: the files (`manifest.json`, `memories.jsonl`) and their hash check (G28).
//! - [`load`]: writes a dataset into a backend, idempotently.
//! - [`verify`]: checks what a backend returns against the dataset.
//! - [`probe`]: refuses to load while a local BAiD is running.

pub mod datagen;
pub mod dataset;
pub mod load;
pub mod probe;
pub mod rng;
pub mod verify;
