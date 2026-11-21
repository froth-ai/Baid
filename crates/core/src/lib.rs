//! Core types, the five extension traits, error enums and config structs; no I/O and no
//! async runtime. Every other BAiD crate depends on this one.
#![deny(missing_docs)]

pub mod backend;
pub mod config;
pub mod error;
pub mod item;
pub mod key;
pub mod memory;
pub mod predict;
pub mod request;
pub mod traits;
pub mod types;
pub mod units;

#[cfg(any(test, feature = "test-util"))]
pub mod mock;

pub use backend::{BackendResult, Capabilities, Fact, Health, WriteAck};
pub use config::{
    AdapterKind, CacheConfig, Config, ConfigError, ConfigIssue, PrefetchConfig, ServerConfig,
    TelemetryConfig, TenantConfig, MAX_STALE_IF_ERROR,
};
pub use error::{BackendError, IdError, RequestError, TokenBudgetError};
pub use item::{
    CacheEntry, CacheOutcome, FlushScope, InvalidationReport, LoadInfo, MemoryItem, PackedContext,
    Priority, PutOutcome, WriteSeq,
};
pub use key::{normalise, Key, KEY_PREFIX, SCHEMA_VERSION};
pub use memory::{text_score, MemoryBackend};
pub use predict::{
    Candidate, PredictionSource, QueryClass, RecallRecord, ScoredClass, SessionView, TenantStats,
};
pub use request::RecallRequest;
pub use traits::{Backend, CacheStore, Invalidator, Packer, Predictor};
pub use types::{
    AdapterId, BackendQuery, BudgetBucket, EntityId, Filters, KeyKind, NodeId, SessionId, TenantId,
    TokenBudget,
};
pub use units::{ByteSize, HumanDuration, UnitError};
