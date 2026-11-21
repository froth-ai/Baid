//! Loading a dataset into a backend (G26).
//!
//! Every fact goes through the adapters crate's `Backend::write`, wrapped in `Resilient` by
//! the caller, so the stored shape is exactly what BAiD reads and `Resilient` is the only
//! retry layer. Each fact carries the idempotency key `load:<dataset>:<n>`: a re-run, or a
//! run resumed after a failure, stores nothing twice.
//!
//! Rows are written in file order (oldest first within each tenant), a bounded number at a
//! time, so backends that order by insertion time (S3 ids, Neo4j `created_at`) roughly agree
//! with `as_of`.
//!
//! **Loading bypasses BAiD.** BAiD's write log never sees these writes, so a running BAiD
//! could keep serving cached results from before the load. Load with BAiD stopped (or flush
//! its caches for the dataset's tenants afterwards); `memharness load` checks the local
//! ports first (see [`crate::probe`]).

use std::time::{Duration, Instant};

use baid_core::{Backend, BackendError, WriteAck};
use futures::{stream, StreamExt};

use crate::dataset::{Dataset, DatasetError};

/// How to load.
#[derive(Clone, Debug)]
pub struct LoadOptions {
    /// Writes in flight at once.
    pub concurrency: usize,
    /// How often to print a progress line (to stderr); `None` prints nothing.
    pub progress_every: Option<Duration>,
}

impl Default for LoadOptions {
    fn default() -> Self {
        LoadOptions {
            concurrency: 16,
            progress_every: Some(Duration::from_secs(5)),
        }
    }
}

/// What a load did.
#[derive(Clone, Debug)]
pub struct LoadReport {
    /// Rows written (acknowledged, including idempotent repeats).
    pub written: u64,
    /// Wall time.
    pub elapsed: Duration,
    /// An order-independent digest of every `(row, node id)` acknowledgement. Two loads of
    /// the same dataset into the same backend give the same digest only if every row was
    /// acknowledged with the same node, i.e. nothing was stored twice.
    pub acks: [u8; 32],
}

impl LoadReport {
    /// Rows per second.
    pub fn rate(&self) -> f64 {
        self.written as f64 / self.elapsed.as_secs_f64().max(1e-9)
    }
}

/// Why a load stopped.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The dataset could not be read.
    #[error(transparent)]
    Dataset(#[from] DatasetError),
    /// A row is not a valid fact.
    #[error("row {n} is not a valid fact: {reason}")]
    BadFact {
        /// The row.
        n: u64,
        /// Why.
        reason: String,
    },
    /// A write failed after `Resilient`'s retries.
    #[error(
        "writing row {n} failed after {written} rows were written: {error}. Re-run to \
         resume: writes are idempotent"
    )]
    Write {
        /// The row.
        n: u64,
        /// Rows acknowledged before the failure.
        written: u64,
        /// The backend error.
        error: BackendError,
    },
}

/// Loads every row of `ds` into `backend`.
pub async fn load<B: Backend + ?Sized>(
    backend: &B,
    ds: &Dataset,
    opts: &LoadOptions,
    label: &str,
) -> Result<LoadReport, LoadError> {
    let name = ds.name().to_owned();
    let total = ds.manifest().memories;
    let start = Instant::now();
    let mut last_print = start;
    let mut written = 0u64;
    let mut acks = [0u8; 32];

    let rows = ds.rows()?;
    let mut writes =
        stream::iter(rows)
            .map(|row| {
                let name = &name;
                async move {
                    let row = row?;
                    let fact = row.fact(name);
                    fact.validate().map_err(|e| LoadError::BadFact {
                        n: row.n,
                        reason: e.to_string(),
                    })?;
                    let ack = backend.write(&row.tenant, &fact).await.map_err(|error| {
                        LoadError::Write {
                            n: row.n,
                            written: 0,
                            error,
                        }
                    })?;
                    Ok::<(u64, WriteAck), LoadError>((row.n, ack))
                }
            })
            .buffer_unordered(opts.concurrency.max(1));

    while let Some(done) = writes.next().await {
        let (n, ack) = done.map_err(|e| match e {
            LoadError::Write { n, error, .. } => LoadError::Write { n, written, error },
            other => other,
        })?;
        written += 1;
        let mut h = blake3::Hasher::new();
        h.update(&n.to_le_bytes());
        h.update(ack.node.as_str().as_bytes());
        for (a, b) in acks.iter_mut().zip(h.finalize().as_bytes()) {
            *a ^= b;
        }
        if let Some(every) = opts.progress_every {
            if last_print.elapsed() >= every {
                last_print = Instant::now();
                let secs = start.elapsed().as_secs_f64().max(1e-9);
                eprintln!(
                    "{label}: {written}/{total} rows, {:.0} rows/s",
                    written as f64 / secs
                );
            }
        }
    }
    Ok(LoadReport {
        written,
        elapsed: start.elapsed(),
        acks,
    })
}
