//! HTTP and WebSocket route handlers.

pub mod events;
pub mod health;
pub mod peers;
pub mod status;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde_json::json;

use crate::state::ApiState;

/// Build the API sub-router. Mount under `/api/v1`.
///
/// The sub-router carries its own `fallback` so unknown paths under
/// `/api/v1/*` always return a JSON `{ "error": "not_found" }` instead
/// of falling through to the outer router's UI fallback. Without this,
/// a typo'd API URL would silently return `index.html`, masking bugs
/// from any test that only inspects status codes.
pub fn api_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health::get_health))
        .route("/status", get(status::get_status))
        .route("/peers", get(peers::get_peers))
        .route("/events", get(events::ws_events))
        .fallback(api_not_found)
        .with_state(state)
}

async fn api_not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "not_found", "scope": "api" })),
    )
}
