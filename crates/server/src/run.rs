//! Serving both listeners and shutting down cleanly.

use std::future::{Future, IntoFuture};
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::FutureExt;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::failopen::Counters;

use crate::http::{admin_router, public_router};
use crate::state::AppState;

/// Serves the public and admin APIs until `shutdown` is cancelled, then stops accepting
/// connections and waits for in-flight requests to finish.
pub async fn serve(
    state: AppState,
    public: TcpListener,
    admin: TcpListener,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let public_srv = axum::serve(public, public_router(state.clone()))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    let admin_srv = axum::serve(admin, admin_router(state.clone()))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    let chores = async {
        let counters = state.engine.fail_open();
        supervise("housekeeping", counters.counters(), &shutdown, || {
            housekeeping(state.clone(), shutdown.clone())
        })
        .await;
        Ok(())
    };
    tokio::try_join!(public_srv, admin_srv, chores)?;
    Ok(())
}

/// Sessions idle this long are forgotten.
pub const SESSION_IDLE: Duration = Duration::from_secs(30 * 60);
/// How often housekeeping runs.
pub const HOUSEKEEPING_EVERY: Duration = Duration::from_secs(60);

/// First delay before restarting a background task that panicked; doubles per restart.
pub const RESTART_BACKOFF_MIN: Duration = Duration::from_secs(1);
/// Longest delay between restarts.
pub const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(60);
/// A task that ran this long before panicking restarts at the minimum delay again.
pub const RESTART_BACKOFF_RESET: Duration = Duration::from_secs(300);

/// Runs a background task, restarting it with backoff whenever it panics, until it returns
/// or `shutdown` is cancelled. Restarts are counted (`baid_task_restarts_total`). The panic
/// payload is not logged: it may contain request data.
pub async fn supervise<F, Fut>(
    name: &'static str,
    counters: &Counters,
    shutdown: &CancellationToken,
    mut task: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut backoff = RESTART_BACKOFF_MIN;
    loop {
        let started = tokio::time::Instant::now();
        if AssertUnwindSafe(task()).catch_unwind().await.is_ok() {
            return;
        }
        counters.count_task_restart();
        if started.elapsed() >= RESTART_BACKOFF_RESET {
            backoff = RESTART_BACKOFF_MIN;
        }
        error!(task = name, restart_in = ?backoff, "background task panicked; restarting");
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
    }
}

/// Expires idle sessions and old idempotency keys, prunes the invalidation index, and
/// retries the flush for tenants in cache bypass (G24), every minute until `shutdown`.
pub async fn housekeeping(state: AppState, shutdown: CancellationToken) {
    let mut tick = tokio::time::interval(HOUSEKEEPING_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tick.tick() => {
                let still_bypassed = state.engine.recover().await;
                if still_bypassed > 0 {
                    warn!(tenants = still_bypassed, "tenants still in cache bypass");
                }
                let done = state.engine.housekeeping(crate::engine::clock_now(), SESSION_IDLE);
                if !done.is_empty() {
                    debug!(
                        sessions = done.sessions_expired,
                        idempotency_keys = done.idempotency_keys_expired,
                        index_refs = done.index_refs_pruned,
                        "housekeeping"
                    );
                }
            }
        }
    }
}

/// Waits for Ctrl-C (or SIGTERM on Unix), then marks the service not ready and cancels
/// `shutdown`.
pub async fn shutdown_on_signal(state: AppState, shutdown: CancellationToken) {
    wait_for_signal().await;
    info!("shutdown requested; draining");
    state.set_ready(false);
    shutdown.cancel();
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
