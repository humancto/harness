//! `GET /api/v1/health` — liveness check. Always returns 200.

use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub service: &'static str,
}

pub async fn get_health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "harness-api",
    })
}
