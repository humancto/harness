//! `GET /api/v1/capabilities` — list every capability registered locally.
//! Used by the Submit page's capability dropdown and (eventually) by
//! 3.x's `harness run` flag completion.

use axum::{extract::State, Json};
use serde::Serialize;

use crate::state::ApiState;

#[derive(Debug, Serialize)]
pub struct CapabilityDto {
    pub id: String,
    pub version: String,
    pub cardinality: String,
    pub cost_hint: String,
    pub tags: Vec<String>,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct CapabilitiesResponse {
    pub capabilities: Vec<CapabilityDto>,
}

pub async fn get_capabilities(State(state): State<ApiState>) -> Json<CapabilitiesResponse> {
    let caps: Vec<CapabilityDto> = state
        .local_status
        .read()
        .capabilities
        .iter()
        .map(|id| CapabilityDto {
            id: id.clone(),
            version: "0.1.0".to_string(),
            cardinality: "anyone".to_string(),
            cost_hint: "local_fast".to_string(),
            tags: vec![],
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
        })
        .collect();
    Json(CapabilitiesResponse { capabilities: caps })
}
