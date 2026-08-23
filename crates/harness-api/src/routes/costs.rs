//! `GET /api/v1/costs` — the 5.9 cost ledger (ADR-0037), consumed by
//! the 5.10 Costs dashboard. Session-auth like every read endpoint.
//! The payload echoes its window: this is a LOCAL, time-bounded view,
//! never an all-time mesh total.

use axum::{extract::State, http::HeaderMap, http::StatusCode, response::IntoResponse, Json};

use crate::auth::{is_authenticated, unauthorized};
use crate::state::ApiState;

pub async fn get_costs(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> axum::response::Response {
    if !is_authenticated(&state.auth, &headers) {
        return unauthorized();
    }
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    match harness_cost::CostLedger::totals(store, now_ms) {
        Ok(totals) => (StatusCode::OK, Json(totals)).into_response(),
        Err(err) => {
            tracing::warn!(target: "harness.api", ?err, "cost ledger query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "ledger_failed" })),
            )
                .into_response()
        }
    }
}
