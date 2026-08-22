//! End-to-end heartbeat exchange over the loopback QUIC transport.
//!
//! Wire two `HeartbeatService`s through a real `Transport`, send a few
//! heartbeats from one side, watch them appear in the other side's
//! `PeerTable`. This is the "1.5 actually works on the wire" gate.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use harness_core::{Identity, Signable};
use harness_mesh::transport::{Transport, TrustStore};
use harness_mesh::{HeartbeatPublisherConfig, HeartbeatService};

fn loopback() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
}

fn build_transport() -> (Transport, Arc<Identity>) {
    let id = Arc::new(Identity::generate());
    let t = Transport::bind(loopback(), id.clone(), TrustStore::new()).expect("bind");
    (t, id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_once_lands_in_peer_table() {
    // server = receiver, client = sender (heartbeat broadcaster).
    let (server, server_id) = build_transport();
    let (client, client_id) = build_transport();
    let server_addr = server.local_addr().expect("local_addr");
    let server_pubkey = *server_id.public_key();

    let server_handle = tokio::spawn(async move {
        let inc = server.accept_one().await.expect("accept_one");
        inc.accept(|_pk| true).expect("accept conn")
    });

    let conn_to_server = Arc::new(
        client
            .dial(server_addr, &server_pubkey)
            .await
            .expect("dial"),
    );
    let server_conn = Arc::new(server_handle.await.expect("join"));

    // Server side: register the client connection so incoming
    // heartbeats land in the table.
    let server_svc = HeartbeatService::new(server_id.clone());
    let _listener = server_svc.register_peer(server_conn);

    // Client side: build + send a heartbeat directly.
    let client_svc = HeartbeatService::new(client_id.clone());
    client_svc
        .broadcast_once(
            &HeartbeatPublisherConfig {
                queue_depth: 7,
                ..Default::default()
            },
            client_id.node_id(),
            42,
            &[conn_to_server.clone()],
        )
        .await;

    // Give the listener task a moment to drain the recv.
    let table = server_svc.peers();
    let recorded = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(entry) = table.get(&client_id.node_id()) {
                break entry;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timeout");
    assert_eq!(recorded.heartbeat.node_id, client_id.node_id());
    assert_eq!(recorded.heartbeat.brain_score, 42);
    assert_eq!(recorded.heartbeat.queue_depth, 7);
    assert_eq!(recorded.heartbeat.seq, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_heartbeats_advance_seq_in_peer_table() {
    let (server, server_id) = build_transport();
    let (client, client_id) = build_transport();
    let server_addr = server.local_addr().expect("local_addr");
    let server_pubkey = *server_id.public_key();

    let server_handle = tokio::spawn(async move {
        let inc = server.accept_one().await.expect("accept_one");
        inc.accept(|_pk| true).expect("accept conn")
    });

    let conn_to_server = Arc::new(
        client
            .dial(server_addr, &server_pubkey)
            .await
            .expect("dial"),
    );
    let server_conn = Arc::new(server_handle.await.expect("join"));

    let server_svc = HeartbeatService::new(server_id.clone());
    let _listener = server_svc.register_peer(server_conn);
    let client_svc = HeartbeatService::new(client_id.clone());

    let snap = HeartbeatPublisherConfig::default();
    for _ in 0..3 {
        client_svc
            .broadcast_once(&snap, client_id.node_id(), 0, &[conn_to_server.clone()])
            .await;
    }

    // Wait until seq >= 3 lands.
    let table = server_svc.peers();
    let final_entry = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(entry) = table.get(&client_id.node_id()) {
                if entry.heartbeat.seq >= 3 {
                    break entry;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timeout");
    assert_eq!(final_entry.heartbeat.seq, 3);
}

#[tokio::test]
async fn evict_stale_drops_silent_peers() {
    let id = Arc::new(Identity::generate());
    let svc = HeartbeatService::new(id.clone());
    let table = svc.peers();
    // Inject a peer directly via the table (no transport in this test).
    let other = Identity::from_secret_bytes(&[0xAA; 32]);
    let mut hb = harness_core::Heartbeat {
        node_id: other.node_id(),
        seq: 1,
        timestamp: 0,
        queue_depth: 0,
        cpu_busy_pct: 0,
        cpu_pinned_count: 0,
        ram_used_mb: 0,
        ram_total_mb: 0,
        gpu_used_mb: 0,
        gpu_total_mb: 0,
        capabilities_hash: [0u8; 16],
        replica_head: [0u8; 32],
        in_flight: vec![],
        leader_belief: other.node_id(),
        brain_score: 0,
        on_battery: false,
        paused: false,
        version: harness_core::SemVer::new(0, 1, 0),
        sig: harness_core::Signature::from_bytes([0u8; 64]),
    };
    hb.sign(&other).expect("sign");
    table.record(hb);
    assert!(table.get(&other.node_id()).is_some());

    // Sleep beyond an artificially-tiny timeout, then evict.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let dropped = table.evict_stale(Duration::from_millis(10));
    assert_eq!(dropped, vec![other.node_id()]);
    assert!(table.get(&other.node_id()).is_none());
}
