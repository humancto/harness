//! `WS /api/v1/events` integration tests.
//!
//! Bind a real loopback socket per test; connect with `tokio-tungstenite`;
//! drive `TableEvent`s through the broadcast bridge and assert the JSON
//! frames the server sends.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::items_after_statements,
    clippy::doc_markdown
)]

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use harness_api::{serve, ApiStateBuilder, MeshEvent, TableEventBridge};
use harness_core::{Heartbeat, Identity, NodeId, SemVer, Signature};
use harness_mesh::heartbeat::{PeerTable, TableEvent};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

fn synthetic_hb(node: [u8; 16], seq: u64) -> Heartbeat {
    Heartbeat {
        node_id: NodeId::from_bytes(node),
        seq,
        timestamp: 0,
        queue_depth: 0,
        cpu_busy_pct: 25,
        cpu_pinned_count: 0,
        ram_used_mb: 4096,
        ram_total_mb: 16384,
        gpu_used_mb: 0,
        gpu_total_mb: 0,
        capabilities_hash: [0; 16],
        replica_head: [0u8; 32],
        in_flight: vec![],
        leader_belief: NodeId::from_bytes([0u8; 16]),
        brain_score: 100,
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

/// Build a wired-up state + bridge: TableEvent source → MeshEvent sink.
async fn boot() -> (
    std::net::SocketAddr,
    broadcast::Sender<TableEvent>,
    PeerTable,
    harness_api::ServerHandle,
    TableEventBridge,
) {
    let id = Arc::new(Identity::generate());
    let peers = PeerTable::new();
    let (table_tx, table_rx) = broadcast::channel::<TableEvent>(64);
    let state = ApiStateBuilder::new(id, "ws-test")
        .with_peers(peers.clone())
        .build();

    let mesh_sink = state.events.clone();
    let bridge = TableEventBridge::spawn(table_rx, mesh_sink, peers.clone(), "ws-test".to_string());

    let server = serve("127.0.0.1:0".parse().unwrap(), state)
        .await
        .expect("bind");
    let addr = server.local_addr();
    (addr, table_tx, peers, server, bridge)
}

async fn connect(
    addr: std::net::SocketAddr,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://{addr}/api/v1/events");
    let (ws, _resp) = tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect");
    ws
}

async fn next_text(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> String {
    let timeout = Duration::from_millis(1500);
    let frame = tokio::time::timeout(timeout, ws.next())
        .await
        .expect("ws frame timeout")
        .expect("ws frame")
        .expect("ws frame ok");
    match frame {
        Message::Text(t) => t,
        other => panic!("expected text frame, got {other:?}"),
    }
}

#[tokio::test]
async fn ws_subscribes_and_receives_peer_added() {
    let (addr, table_tx, peers, server, bridge) = boot().await;
    let mut ws = connect(addr).await;

    // Give the server a tick to attach our subscriber before we publish.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let hb = synthetic_hb([0xAA; 16], 1);
    peers.record(hb.clone());
    table_tx
        .send(TableEvent::Recorded {
            node_id: hb.node_id,
            was_new: true,
        })
        .expect("send TableEvent");

    let text = next_text(&mut ws).await;
    let event: MeshEvent = serde_json::from_str(&text).expect("MeshEvent json");
    match event {
        MeshEvent::PeerAdded { peer } => {
            assert_eq!(peer.node_id, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        }
        other => panic!("unexpected: {other:?}"),
    }

    ws.close(None).await.ok();
    bridge.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn ws_translates_evicted_to_peer_evicted() {
    let (addr, table_tx, _peers, server, bridge) = boot().await;
    let mut ws = connect(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    table_tx
        .send(TableEvent::Evicted {
            node_id: NodeId::from_bytes([0x55; 16]),
        })
        .expect("send TableEvent");

    let text = next_text(&mut ws).await;
    let event: MeshEvent = serde_json::from_str(&text).expect("MeshEvent json");
    match event {
        MeshEvent::PeerEvicted { node_id } => {
            assert_eq!(node_id, "55555555555555555555555555555555");
        }
        other => panic!("unexpected: {other:?}"),
    }

    ws.close(None).await.ok();
    bridge.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn ws_origin_check_rejects_foreign_origin() {
    let (addr, _tx, _peers, server, bridge) = boot().await;
    let url = format!("ws://{addr}/api/v1/events");
    let mut req = url.as_str().into_client_request().expect("build req");
    req.headers_mut()
        .insert("Origin", "https://evil.example".parse().unwrap());
    let result = tokio_tungstenite::connect_async(req).await;
    assert!(result.is_err(), "foreign origin must be rejected");
    bridge.shutdown().await;
    server.shutdown().await;
}

/// 4.7 (ADR-0029): a subscriber lagging past `MESH_EVENT_CAPACITY` is
/// closed with 1011 "lagged" — the documented resync story (reconnect,
/// refetch snapshot). Capacity-1 channel makes the lag deterministic.
#[tokio::test(flavor = "current_thread")]
async fn ws_lagged_subscriber_closed_1011_for_resync() {
    let id = Arc::new(Identity::generate());
    let (events_tx, _keepalive) = broadcast::channel::<MeshEvent>(1);
    let state = ApiStateBuilder::new(id, "ws-lag-test")
        .with_events(events_tx.clone())
        .build();
    let server = harness_api::serve("127.0.0.1:0".parse().unwrap(), state)
        .await
        .expect("bind");
    let addr = server.local_addr();

    let mut ws = connect(addr).await;
    // Give the server a tick to attach the subscriber.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Burst past the capacity-1 channel before the forwarder task can
    // poll (current-thread runtime: no await between sends) — its next
    // recv() is Lagged.
    for score in 0..3 {
        let _ = events_tx.send(MeshEvent::LeaderChanged {
            leader: None,
            brain_score: score,
        });
    }

    let frame = tokio::time::timeout(Duration::from_millis(1500), ws.next())
        .await
        .expect("frame timeout")
        .expect("frame")
        .expect("frame ok");
    match frame {
        Message::Close(Some(cf)) => {
            assert_eq!(u16::from(cf.code), 1011, "close code");
            assert_eq!(cf.reason, "lagged");
        }
        other => panic!("expected 1011 close, got {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test]
async fn ws_leader_changed_pushes_frame() {
    let id = Arc::new(Identity::generate());
    let state = ApiStateBuilder::new(id, "ws-leader-test").build();
    let server = serve("127.0.0.1:0".parse().unwrap(), state.clone())
        .await
        .expect("bind");
    let addr = server.local_addr();

    let mut ws = connect(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_leader = NodeId::from_bytes([0xCC; 16]);
    state.set_local_status(|s| {
        s.leader_belief = Some(new_leader);
        s.brain_score = 333;
    });

    let text = next_text(&mut ws).await;
    let event: MeshEvent = serde_json::from_str(&text).expect("MeshEvent json");
    match event {
        MeshEvent::LeaderChanged {
            leader,
            brain_score,
        } => {
            assert_eq!(brain_score, 333);
            assert_eq!(leader.as_deref(), Some("cccccccccccccccccccccccccccccccc"));
        }
        other => panic!("unexpected: {other:?}"),
    }

    ws.close(None).await.ok();
    server.shutdown().await;
}
