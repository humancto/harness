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

    let mut local_peer = PeerDto::local(
        state.local_node_id,
        pubkey,
        &local_status.mesh_name,
        local_status.brain_score,
        local_status.leader_belief,
        local_status.seq,
        local_status.capabilities.clone(),
    );
    local_peer.node_name = Some(local_status.node_name.clone());
    let local = LocalDto::new(local_peer);

    // 3.3-fanout: enrich each peer from its announced manifest (name,
    // os, real capability list) when the store has one.
    let peers: Vec<PeerDto> = state
        .peers
        .snapshot()
        .iter()
        .map(|entry| {
            let mut dto = PeerDto::from_entry(entry, &local_status.mesh_name);
            if let Some(store) = &state.store {
                if let Ok(Some(manifest)) = store.load_manifest(entry.heartbeat.node_id) {
                    dto.enrich_from_manifest(&manifest);
                }
            }
            dto
        })
        .collect();

    Json(PeersSnapshot {
        local,
        peers,
        leader_belief: local_status.leader_belief.map(|id| format!("{id}")),
        fetched_at_ms: PeersSnapshot::now_ms(),
    })
}
