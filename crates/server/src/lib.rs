//! The BAiD service: HTTP API, admin API and the wiring of the engine crates.
//!
//! The `baid` binary (`main.rs`) is a thin shell over this library, so the pieces can be
//! tested directly.

// Fail open (ADR-009) catches panics in the cache, index, packer and adapters and falls back
// to the backend. With `panic = "abort"` there is nothing to catch: every such bug would
// kill the process instead.
#[cfg(panic = "abort")]
compile_error!("BAiD's fail-open design requires panic = unwind");

pub mod auth;
pub mod cli;
pub mod config;
pub mod engine;
pub mod failopen;
pub mod http;
pub mod run;
pub mod state;
