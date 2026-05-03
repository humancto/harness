//! `GET /api/v1/status` — local node detail.

use axum::{extract::State, Json};

use crate::dto::{system_time_to_ms, StatusDto};
use crate::state::ApiState;

pub async fn get_status(State(state): State<ApiState>) -> Json<StatusDto> {
    let status = state.local_status.read().clone();
    let pubkey = state.local_pubkey();
    Json(StatusDto {
        node_id: format!("{}", state.local_node_id),
        pubkey_fp: pubkey.fingerprint_hex(),
        mesh_name: status.mesh_name,
        brain_score: status.brain_score,
        leader_belief: status.leader_belief.map(|id| format!("{id}")),
        started_at_ms: system_time_to_ms(status.started_at),
        ui_version: env!("CARGO_PKG_VERSION").to_owned(),
    })
}
