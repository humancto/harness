//! `GET /api/v1/peers` — full peer-table snapshot, deterministic order.

use axum::{extract::State, Json};

use crate::dto::{LocalDto, PeerDto, PeersSnapshot};
use crate::state::ApiState;

pub async fn get_peers(State(state): State<ApiState>) -> Json<PeersSnapshot> {
    // Snapshot the local status first so we don't race with a concurrent
    // election update — the sequence below interleaves cleanly because
    // each read is its own RwLock acquisition.
    let local_status = state.local_status.read().clone();
    let pubkey = state.local_pubkey();

    let local_peer = PeerDto::local(
        state.local_node_id,
        pubkey,
        &local_status.mesh_name,
        local_status.brain_score,
        local_status.leader_belief,
        local_status.seq,
        local_status.capabilities.clone(),
    );
    let local = LocalDto::new(local_peer);

    let peers: Vec<PeerDto> = state
        .peers
        .snapshot()
        .iter()
        .map(|entry| PeerDto::from_entry(entry, &local_status.mesh_name))
        .collect();

    Json(PeersSnapshot {
        local,
        peers,
        leader_belief: local_status.leader_belief.map(|id| format!("{id}")),
        fetched_at_ms: PeersSnapshot::now_ms(),
    })
}
