//! `GET /api/v1/peers` integration tests — empty + populated + concurrent.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use axum::{body::Body, http::Request};
use harness_api::{router, ApiStateBuilder, PeersSnapshot};
use harness_core::{Heartbeat, Identity, NodeId, SemVer, Signature};
use harness_mesh::heartbeat::PeerTable;
use http_body_util::BodyExt;
use tower::ServiceExt;

fn synthetic_hb(node: [u8; 16], seq: u64, brain: i32) -> Heartbeat {
    Heartbeat {
        node_id: NodeId::from_bytes(node),
        seq,
        timestamp: 0,
        queue_depth: 0,
        cpu_busy_pct: 10,
        cpu_pinned_count: 0,
        ram_used_mb: 1024,
        ram_total_mb: 8192,
        gpu_used_mb: 0,
        gpu_total_mb: 0,
        capabilities_hash: [0; 16],
        replica_head: [0u8; 32],
        in_flight: vec![],
        leader_belief: NodeId::from_bytes([0u8; 16]),
        brain_score: brain,
        on_battery: false,
        paused: false,
        version: SemVer {
            major: 0,
            minor: 1,
            patch: 0,
        },
        sig: Signature::from_bytes([0u8; 64]),
    }
}

async fn fetch_snapshot(app: axum::Router) -> PeersSnapshot {
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/peers")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), 200);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("snapshot json")
}

#[tokio::test]
async fn peers_endpoint_empty_returns_local_only() {
    let id = Arc::new(Identity::generate());
    let state = ApiStateBuilder::new(id, "lan-test").build();
    let app = router(state);
    let snap = fetch_snapshot(app).await;
    assert_eq!(snap.peers.len(), 0);
    assert!(snap.local.is_local);
    assert_eq!(snap.local.peer.mesh_name, "lan-test");
}

#[tokio::test]
async fn peers_endpoint_populated_returns_sorted_peers() {
    let peers = PeerTable::new();
    peers.record(synthetic_hb([0xAA; 16], 1, 100));
    peers.record(synthetic_hb([0x11; 16], 2, 250));

    let id = Arc::new(Identity::generate());
    let state = ApiStateBuilder::new(id, "lan-test")
        .with_peers(peers)
        .build();
    let app = router(state);
    let snap = fetch_snapshot(app).await;

    assert_eq!(snap.peers.len(), 2);
    // PeerTable::snapshot sorts by node_id ascending.
    assert!(snap.peers[0].node_id < snap.peers[1].node_id);
    assert_eq!(snap.peers[0].node_id, "11111111111111111111111111111111");
}

#[tokio::test]
async fn peers_endpoint_concurrent_writes_does_not_panic() {
    let peers = PeerTable::new();
    let id = Arc::new(Identity::generate());
    let state = ApiStateBuilder::new(id, "lan-test")
        .with_peers(peers.clone())
        .build();
    let app = router(state);

    let writer = tokio::spawn(async move {
        let mut byte = 1u8;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
        while tokio::time::Instant::now() < deadline {
            peers.record(synthetic_hb([byte; 16], u64::from(byte), i32::from(byte)));
            byte = byte.wrapping_add(1);
            tokio::task::yield_now().await;
        }
    });

    for _ in 0..50 {
        let snap = fetch_snapshot(app.clone()).await;
        // Number of peers monotonically grows OR resets if a NodeId
        // collision happened, but the response always parses.
        assert!(snap.peers.len() <= 256);
    }

    writer.await.expect("writer joined");
}
