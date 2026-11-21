//! HTTP routers: the public API and the admin API, each on its own listener.

pub mod api;
pub mod error;

use std::any::Any;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tower_http::catch_panic::{CatchPanicLayer, ResponseForPanic};
use tower_http::trace::TraceLayer;

use crate::auth::Tenant;
use crate::failopen::Component;
use crate::state::AppState;
use error::ApiError;

/// Every `(method, path)` the public router serves, in `api/openapi.yaml` notation. Keep it
/// in step with [`public_router`]. `tests/contract.rs` checks that it equals the spec's
/// routes and that the router answers each one with a documented status.
pub const ROUTES: &[(&str, &str)] = &[
    ("GET", "/healthz"),
    ("GET", "/readyz"),
    ("GET", "/v1/whoami"),
    ("POST", "/v1/recall"),
    ("POST", "/v1/remember"),
    ("DELETE", "/v1/sessions/{session_id}"),
];

/// Public API: health probes (no auth) and `/v1/*` (API key required).
pub fn public_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/whoami", get(whoami))
        .route("/v1/recall", post(api::recall))
        .route("/v1/remember", post(api::remember))
        .route("/v1/sessions/{session_id}", delete(api::end_session))
        .fallback(not_found)
        .layer(CatchPanicLayer::custom(PanicTo500(state.clone())))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Last line of fail open: a panic no inner guard caught answers with the standard
/// `internal` error (500, retryable) instead of dropping the connection, and is counted.
#[derive(Clone)]
struct PanicTo500(AppState);

impl ResponseForPanic for PanicTo500 {
    type ResponseBody = Body;

    fn response_for_panic(&mut self, _payload: Box<dyn Any + Send + 'static>) -> Response<Body> {
        self.0.engine.fail_open().panicked(Component::Request);
        ApiError::Internal.into_response()
    }
}

/// Admin API: health probes now; metrics, flush and reload come later. Bind it to loopback
/// or a private network.
pub fn admin_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(admin_readyz))
        .fallback(not_found)
        .layer(CatchPanicLayer::custom(PanicTo500(state.clone())))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Liveness: the process is up and serving HTTP.
async fn healthz() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// Readiness: 503 once shutdown has begun, so traffic drains away. While a backend is failing
/// or a tenant's cache is bypassed it stays 200 with `degraded`: this replica still serves
/// (stale answers, or errors no worse than the backend's), and a 503 would pull every replica
/// during a backend outage (G25). Unauthenticated, so it names no tenants or backends.
async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if !state.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "draining" })),
        );
    }
    let status = if state.engine.degraded().is_empty() {
        "ready"
    } else {
        "degraded"
    };
    (StatusCode::OK, Json(json!({ "status": status })))
}

/// The admin listener's readiness: the same status, plus which tenants are degraded and why,
/// and which internal components are bypassed.
async fn admin_readyz(State(state): State<AppState>) -> impl IntoResponse {
    let d = state.engine.degraded();
    let (code, status) = if !state.is_ready() {
        (StatusCode::SERVICE_UNAVAILABLE, "draining")
    } else if d.is_empty() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::OK, "degraded")
    };
    let tenants: Vec<Value> = d
        .tenants
        .iter()
        .map(|(t, why)| {
            json!({
                "tenant": t,
                "reasons": why.iter().map(|r| r.as_str()).collect::<Vec<_>>(),
            })
        })
        .collect();
    let components: Vec<&str> = d.components.iter().map(|c| c.as_str()).collect();
    (
        code,
        Json(json!({ "status": status, "tenants": tenants, "components": components })),
    )
}

/// Which tenant the caller's API key belongs to. Lets clients check a key.
async fn whoami(tenant: Tenant) -> impl IntoResponse {
    Json(json!({ "tenant": tenant.id }))
}

async fn not_found() -> ApiError {
    ApiError::NotFound
}
