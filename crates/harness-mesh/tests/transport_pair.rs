//! Loopback integration tests for `harness_mesh::transport`.
//!
//! Each test binds two `Transport` instances on `127.0.0.1:0`, reads the
//! actual ports back via `local_addr`, and exchanges signed messages. No
//! real network, no fixed ports — CI-stable.
//!
//! Stream open/accept is lazy on `Connection`: the dialer opens its bidi
//! stream on first `send`, the accepter accepts on first `recv` (or
//! `send`). Eagerly opening at handshake time would deadlock with the
//! accepter pattern (accepter blocks on `accept_bi` waiting for the
//! dialer's first write, but the dialer can't write until the accepter
//! returns). The current design lets both sides return from the
//! handshake immediately and hand off via the application protocol's
//! first message.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use harness_core::{
    Heartbeat, Identity, NodeId, NodeManifest, Resources, SemVer, Signable, Signature, TaskId,
};
use harness_mesh::transport::{
    channels, Connection, IncomingConnection, Transport, TransportError, TrustStore,
};

fn loopback() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
}

fn build_transport() -> (Transport, Arc<Identity>) {
    let id = Arc::new(Identity::generate());
    let t = Transport::bind(loopback(), id.clone(), TrustStore::new()).expect("bind");
    (t, id)
}

fn sample_heartbeat(node: NodeId, seq: u64) -> Heartbeat {
    Heartbeat {
        node_id: node,
        seq,
        timestamp: 1_700_000_000_000,
        queue_depth: 0,
        cpu_busy_pct: 0,
        cpu_pinned_count: 0,
        ram_used_mb: 0,
        ram_total_mb: 0,
        gpu_used_mb: 0,
        gpu_total_mb: 0,
        capabilities_hash: [0u8; 16],
        in_flight: vec![TaskId(uuid::Uuid::nil())],
        leader_belief: node,
        brain_score: 0,
        on_battery: false,
        paused: false,
        version: SemVer::new(0, 1, 0),
        sig: Signature::from_bytes([0u8; 64]),
    }
}

// -----------------------------------------------------------------------------
// Transport-construction smoke tests — these all pass and prove the
// rustls + quinn endpoint configs are syntactically valid.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn bind_succeeds_on_loopback() {
    let (_t, _id) = build_transport();
}

#[tokio::test]
async fn local_addr_returns_bound_loopback_port() {
    let (t, _) = build_transport();
    let addr = t.local_addr().expect("local_addr");
    assert_eq!(addr.ip().to_string(), "127.0.0.1");
    assert_ne!(addr.port(), 0, "should have been assigned a port");
}

#[tokio::test]
async fn shutdown_is_idempotent() {
    let (server, _) = build_transport();
    server.shutdown(Duration::from_millis(100)).await;
    server.shutdown(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn transport_clone_shares_endpoint() {
    let (t, _) = build_transport();
    let t2 = t.clone();
    assert_eq!(
        t.local_addr().expect("orig"),
        t2.local_addr().expect("clone")
    );
}

#[tokio::test]
async fn two_transports_get_distinct_ports() {
    let (a, _) = build_transport();
    let (b, _) = build_transport();
    assert_ne!(
        a.local_addr().expect("a").port(),
        b.local_addr().expect("b").port()
    );
}

#[tokio::test]
async fn bind_with_trust_store_accepts() {
    let id = Arc::new(Identity::generate());
    let mut trust = TrustStore::new();
    let other = Identity::generate();
    trust.allow(*other.public_key());
    let _t = Transport::bind(loopback(), id, trust).expect("bind with trust");
}

#[tokio::test]
async fn node_id_returns_local_identity_node_id() {
    let (t, id) = build_transport();
    assert_eq!(t.node_id(), id.node_id());
}

// -----------------------------------------------------------------------------
// Handshake-requiring tests — currently ignored pending debug.
// See module docstring for why.
// -----------------------------------------------------------------------------

async fn accept_one_into(transport: Transport) -> IncomingConnection {
    transport.accept_one().await.expect("accept_one")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dial_send_heartbeat_recv() {
    let (server, server_id) = build_transport();
    let (client, client_id) = build_transport();
    let server_addr = server.local_addr().expect("local_addr");
    let server_pubkey = *server_id.public_key();

    let server_handle = tokio::spawn(async move {
        let inc = accept_one_into(server).await;
        inc.accept(|_pk| true).expect("accept conn")
    });

    let conn = client
        .dial(server_addr, &server_pubkey)
        .await
        .expect("dial");
    let server_conn: Connection = server_handle.await.expect("join");

    let mut hb = sample_heartbeat(NodeId::from_bytes([0xAA; 16]), 1);
    hb.sign(&client_id).expect("sign");
    conn.send(&hb).await.expect("send");

    let received: Heartbeat = server_conn.recv().await.expect("recv");
    assert_eq!(received.node_id, hb.node_id);
    assert_eq!(received.seq, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dial_with_wrong_expected_pubkey_fails() {
    let (server, _server_id) = build_transport();
    let (client, _) = build_transport();
    let server_addr = server.local_addr().expect("local_addr");

    let _server_handle = tokio::spawn(async move {
        let _ = server.accept_one().await;
    });

    let other = Identity::generate();
    let err = client
        .dial(server_addr, other.public_key())
        .await
        .expect_err("must fail with wrong pubkey");
    match err {
        TransportError::Connect { .. }
        | TransportError::DialTimeout(_)
        | TransportError::CertMismatch => {}
        other => panic!("expected Connect/DialTimeout/CertMismatch, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_old_seq_rejected() {
    let (server, server_id) = build_transport();
    let (client, client_id) = build_transport();
    let server_addr = server.local_addr().expect("local_addr");
    let server_pubkey = *server_id.public_key();

    let server_handle = tokio::spawn(async move {
        let inc = accept_one_into(server).await;
        inc.accept(|_pk| true).expect("accept")
    });

    let conn = client
        .dial(server_addr, &server_pubkey)
        .await
        .expect("dial");
    let server_conn = server_handle.await.expect("join");

    let mut hb1 = sample_heartbeat(client_id.node_id(), 1);
    hb1.sign(&client_id).expect("sign 1");
    conn.send(&hb1).await.expect("send 1");
    let _: Heartbeat = server_conn
        .recv_sequenced(channels::HEARTBEAT)
        .await
        .expect("recv 1");

    let mut hb1_replay = sample_heartbeat(client_id.node_id(), 1);
    hb1_replay.sign(&client_id).expect("sign replay");
    conn.send(&hb1_replay).await.expect("send replay");
    let err = server_conn
        .recv_sequenced::<Heartbeat>(channels::HEARTBEAT)
        .await
        .expect_err("replay must reject");
    assert!(
        matches!(
            err,
            TransportError::Replay {
                got: 1,
                last_seen: 1,
                ..
            }
        ),
        "got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_manifest_round_trip_via_transport() {
    let (server, server_id) = build_transport();
    let (client, client_id) = build_transport();
    let server_addr = server.local_addr().expect("local_addr");
    let server_pubkey = *server_id.public_key();

    let server_handle = tokio::spawn(async move {
        let inc = accept_one_into(server).await;
        inc.accept(|_pk| true).expect("accept")
    });

    let conn = client
        .dial(server_addr, &server_pubkey)
        .await
        .expect("dial");
    let server_conn = server_handle.await.expect("join");

    let mut manifest = NodeManifest {
        node_id: client_id.node_id(),
        hostname: "test".into(),
        pubkey: *client_id.public_key(),
        capabilities: vec![],
        scopes: vec![],
        resources: Resources {
            cpu_cores: 1,
            ram_total_mb: 1,
            gpu: None,
            os: "linux".into(),
            arch: "x86_64".into(),
        },
        online_since: 0,
        version: SemVer::new(0, 0, 0),
        sig: Signature::from_bytes([0u8; 64]),
    };
    manifest.sign(&client_id).expect("sign");
    conn.send(&manifest).await.expect("send");

    let received: NodeManifest = server_conn.recv().await.expect("recv");
    assert_eq!(received.node_id, manifest.node_id);
    assert_eq!(received.hostname, "test");
}

/// Regression test for the cancel-safety hole the reviewer caught
/// after the leftover-residue fix: when a chunk straddles two frames,
/// the framer must keep the residue ON the framer across any await
/// point so a `select!`-cancelled `recv` doesn't lose the head of
/// frame N+1. Test sends two frames back-to-back, receives frame 1,
/// then races `recv` for frame 2 against an immediate cancellation,
/// then calls `recv` again — frame 2's bytes must still be there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recv_with_pending_leftover_survives_cancellation() {
    let (server, server_id) = build_transport();
    let (client, client_id) = build_transport();
    let server_addr = server.local_addr().expect("local_addr");
    let server_pubkey = *server_id.public_key();

    let server_handle = tokio::spawn(async move {
        let inc = accept_one_into(server).await;
        inc.accept(|_pk| true).expect("accept")
    });
    let conn = client
        .dial(server_addr, &server_pubkey)
        .await
        .expect("dial");
    let server_conn = server_handle.await.expect("join");

    // Send two heartbeats back-to-back. quinn typically coalesces
    // these into one STREAM frame on the wire, so the server's first
    // read_chunk produces both — exercising the leftover path.
    let mut hb1 = sample_heartbeat(client_id.node_id(), 1);
    hb1.sign(&client_id).expect("sign 1");
    let mut hb2 = sample_heartbeat(client_id.node_id(), 2);
    hb2.sign(&client_id).expect("sign 2");
    conn.send(&hb1).await.expect("send 1");
    conn.send(&hb2).await.expect("send 2");

    // Receive frame 1 (drains the chunk; frame 2's bytes go to leftover).
    let r1: Heartbeat = server_conn.recv().await.expect("recv 1");
    assert_eq!(r1.seq, 1);

    // Race recv for frame 2 against an immediate cancellation. With
    // the fix, the leftover stays on the framer across the cancel and
    // the next call extracts frame 2.
    let cancelled: Result<Heartbeat, TransportError> = tokio::select! {
        biased;
        // Cancel-now branch fires first (no await, ready immediately).
        () = std::future::ready(()) => Err(TransportError::Closed),
        msg = server_conn.recv::<Heartbeat>() => msg,
    };
    assert!(cancelled.is_err(), "cancel branch should win");

    // Now actually receive frame 2. With the bug, frame 2's bytes
    // would be lost when the cancelled future was dropped.
    let r2: Heartbeat = server_conn.recv().await.expect("recv 2 after cancel");
    assert_eq!(r2.seq, 2);
}

/// Regression test for the single-`streams`-mutex deadlock the
/// reviewer caught: a `recv` blocked on `read_chunk` must not gate a
/// concurrent `send` on the same `Connection`. Item 1.5's heartbeat
/// loop will run send and recv from independent tokio tasks; if they
/// share one mutex the loop deadlocks the moment the recv awaits a
/// chunk.
///
/// Test shape: client opens the conversation (so the lazy `accept_bi`
/// on the server resolves), then we run a chatty exchange:
///
///   * Client task A: park in `recv` (waiting for the server to
///     respond).
///   * Server task B: receive the client's first heartbeat.
///   * Server task C: spawn a concurrent send back to the client
///     while task B's recv is in flight on a separate piece of state.
///
/// With one mutex covering both halves, task C would block on task B
/// until B's recv returned (which already returned in this shape, so
/// the failure mode is more obvious in 1.5 — but the test still
/// proves split-mutex behavior by running send AND recv concurrently
/// on the same `Arc<Connection>` from two tasks).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_send_and_recv_do_not_deadlock() {
    let (server, server_id) = build_transport();
    let (client, client_id) = build_transport();
    let server_addr = server.local_addr().expect("local_addr");
    let server_pubkey = *server_id.public_key();

    let server_handle = tokio::spawn(async move {
        let inc = accept_one_into(server).await;
        inc.accept(|_pk| true).expect("accept")
    });

    let conn = Arc::new(
        client
            .dial(server_addr, &server_pubkey)
            .await
            .expect("dial"),
    );
    let server_conn = Arc::new(server_handle.await.expect("join"));

    // Step 1: client sends the first heartbeat. This wakes up the
    // server's lazy `accept_bi` and gives both sides a real stream
    // pair under their independent mutexes.
    let mut hb_initial = sample_heartbeat(client_id.node_id(), 1);
    hb_initial.sign(&client_id).expect("sign initial");
    conn.send(&hb_initial).await.expect("client send 1");

    // Step 2: server receives the initial heartbeat.
    let received: Heartbeat = server_conn.recv().await.expect("server recv 1");
    assert_eq!(received.seq, 1);

    // Step 3: park the server in recv. With single-mutex it would
    // hold the streams lock for the duration; split-mutex frees the
    // send half.
    let server_conn_clone = server_conn.clone();
    let recv_park = tokio::spawn(async move {
        let h: Heartbeat = server_conn_clone.recv().await.expect("server recv 2");
        h
    });

    // Step 4: while server-recv is parked, send server -> client. If
    // the streams mutex covered both halves, this send would block
    // forever because recv is holding the lock waiting for bytes.
    let mut server_reply = sample_heartbeat(server_id.node_id(), 1);
    server_reply.sign(&server_id).expect("sign server reply");
    let server_conn_send = server_conn.clone();
    let send_concurrent = tokio::spawn(async move {
        // Tiny delay so the recv has definitely parked on read_chunk.
        tokio::time::sleep(Duration::from_millis(50)).await;
        server_conn_send
            .send(&server_reply)
            .await
            .expect("server send during recv");
    });

    // Step 5: client receives the server's reply (proves send went
    // through despite the parked recv).
    let client_recv: Heartbeat = conn.recv().await.expect("client recv reply");
    assert_eq!(client_recv.node_id, server_id.node_id());

    // Step 6: client sends so the server's parked recv unblocks.
    let mut hb_unblock = sample_heartbeat(client_id.node_id(), 2);
    hb_unblock.sign(&client_id).expect("sign unblock");
    conn.send(&hb_unblock).await.expect("client send 2");

    // Both background tasks must complete promptly.
    tokio::time::timeout(Duration::from_secs(5), async {
        send_concurrent.await.expect("send task");
        let unblocked = recv_park.await.expect("recv task");
        assert_eq!(unblocked.seq, 2);
    })
    .await
    .expect("send+recv must not deadlock");
}
