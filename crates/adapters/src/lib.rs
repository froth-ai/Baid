//! Backend adapters and the registry that maps each tenant to its backend.
//!
//! - [`BoltBackend`]: Neo4j (or any Bolt server).
//! - [`MemoryBackend`]: in-process, non-durable (re-exported from `baid-core`).
//! - [`S3Backend`]: S3 and S3-compatible object stores.
//! - [`Resilient`]: retries and a circuit breaker; the registry wraps every backend in it.
//!
//! Zep and Mem0 come after launch.

mod bolt;
mod registry;
mod resilient;
mod s3;

pub use baid_core::MemoryBackend;
pub use bolt::{lucene_query, BoltBackend, BoltConfig};
pub use registry::{BreakerProbe, BuildError, Credentials, Registry};
pub use resilient::{BreakerPolicy, BreakerState, Resilient, RetryPolicy};
pub use s3::{S3Backend, S3Config};
