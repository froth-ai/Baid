//! `/v1` request and response bodies, and their handlers.

use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use baid_core::{
    EntityId, Fact, Filters, MemoryItem, NodeId, RecallRequest, SessionId, TokenBudget,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::auth::Tenant;
use crate::http::error::ApiError;
use crate::state::AppState;

/// `POST /v1/recall`
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecallBody {
    /// The agent session.
    pub session_id: SessionId,
    /// The question.
    pub query: String,
    /// Tokens of context wanted (64 to 32000; default 2000).
    #[serde(default)]
    pub token_budget: Option<u32>,
    /// Only items mentioning one of these entities.
    #[serde(default)]
    pub entity_ids: Vec<EntityId>,
    /// Only items with one of these labels.
    #[serde(default)]
    pub labels: Vec<String>,
    /// Only items recorded at or after this time (RFC 3339).
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub since: Option<OffsetDateTime>,
    /// Oldest cached answer accepted, in ms; 0 skips the cache.
    #[serde(default)]
    pub max_staleness_ms: Option<u64>,
}

/// Response to `POST /v1/recall`.
#[derive(Debug, Serialize)]
pub struct RecallResponse {
    /// The packed items, in reading order.
    pub items: Vec<MemoryItem>,
    /// Tokens used by `items`.
    pub tokens: u32,
    /// Items dropped as duplicates or to fit the budget.
    pub dropped: u32,
    /// `hit`, `miss` or `stale` (backend failing, expired entry served; G23).
    pub cache: &'static str,
    /// Age of the cached entry served, in ms: present for `hit` and `stale`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
}

/// `POST /v1/remember`
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RememberBody {
    /// The agent session.
    pub session_id: SessionId,
    /// The fact.
    pub text: String,
    /// Entities the fact is about.
    #[serde(default)]
    pub entities: Vec<EntityId>,
    /// Free-form labels.
    #[serde(default)]
    pub labels: Vec<String>,
    /// When the fact became true (RFC 3339).
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub as_of: Option<OffsetDateTime>,
    /// Retrying with the same key applies the write once.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

/// Response to `POST /v1/remember`.
#[derive(Debug, Serialize)]
pub struct RememberResponse {
    /// The backend's id for the stored fact.
    pub id: NodeId,
    /// Every entity the write touched.
    pub entities: Vec<EntityId>,
    /// The item's version, if the backend tracks them.
    pub version: Option<u64>,
    /// True if this is the stored answer to an earlier request with the same key.
    pub replayed: bool,
}

fn body<T>(b: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    b.map(|Json(v)| v)
        .map_err(|e| ApiError::InvalidArgument(e.body_text()))
}

/// `POST /v1/recall`
pub async fn recall(
    State(state): State<AppState>,
    tenant: Tenant,
    b: Result<Json<RecallBody>, JsonRejection>,
) -> Result<Json<RecallResponse>, ApiError> {
    let b = body(b)?;
    let budget = match b.token_budget {
        Some(n) => TokenBudget::new(n)?,
        None => TokenBudget::default(),
    };
    let req = RecallRequest {
        session: b.session_id,
        query: b.query,
        budget,
        filters: Filters {
            entity_ids: b.entity_ids,
            since: b.since,
            labels: b.labels,
        },
        max_staleness: b.max_staleness_ms.map(Duration::from_millis),
    };
    let r = state.engine.recall(&tenant, req).await?;
    Ok(Json(RecallResponse {
        items: r.packed.items,
        tokens: r.packed.tokens,
        dropped: r.packed.dropped,
        cache: r.outcome.as_str(),
        age_ms: r
            .age
            .map(|a| u64::try_from(a.as_millis()).unwrap_or(u64::MAX)),
    }))
}

/// `POST /v1/remember`
pub async fn remember(
    State(state): State<AppState>,
    tenant: Tenant,
    b: Result<Json<RememberBody>, JsonRejection>,
) -> Result<Json<RememberResponse>, ApiError> {
    let b = body(b)?;
    let fact = Fact {
        text: b.text,
        entities: b.entities,
        labels: b.labels,
        as_of: b.as_of,
        idempotency_key: b.idempotency_key,
    };
    let r = state.engine.remember(&tenant, &b.session_id, fact).await?;
    Ok(Json(RememberResponse {
        id: r.node,
        entities: r.entities,
        version: r.version,
        replayed: r.replayed,
    }))
}

/// `DELETE /v1/sessions/{id}`: forget a session. Idempotent; always 204.
pub async fn end_session(
    State(state): State<AppState>,
    tenant: Tenant,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let session = SessionId::new(&id).map_err(|e| ApiError::InvalidArgument(e.to_string()))?;
    state.engine.end_session(&tenant, &session);
    Ok(StatusCode::NO_CONTENT)
}
