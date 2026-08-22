//! 3.3-fanout — named channel streams over loopback QUIC (ADR-0017).
//!
//! Same harness shape as `transport_pair.rs`: two `Transport`s on
//! `127.0.0.1:0`, real handshakes, no fixed ports.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use harness_core::{
    Constraints, Cost, ExecutionPolicy, FinalResult, Heartbeat, Identity, LeaseId, NodeId,
    ResourceHints, RetryPolicy, SemVer, Signable, Signature, Status, Task, TaskAssign, TaskClaim,
    TaskId, TaskResultMsg, TraceContext,
};
use harness_mesh::transport::{channels, Connection, Transport, TransportError, TrustStore};

fn loopback() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
}

fn build_transport() -> (Transport, Arc<Identity>) {
    let id = Arc::new(Identity::generate());
    let t = Transport::bind(loopback(), id.clone(), TrustStore::new()).expect("bind");
    (t, id)
}

/// Dial b from a; return both `Connection` ends (dialer, accepter).
async fn connect_pair(
    a: &Transport,
    b: &Transport,
    b_id: &Identity,
) -> (Arc<Connection>, Arc<Connection>) {
    let b_accept = tokio::spawn({
        let b = b.clone();
        async move {
            let inc = b.accept_one().await.expect("accept_one");
            inc.accept(|_| true).expect("accept")
        }
    });
    let dialed = a
        .dial(b.local_addr().expect("addr"), b_id.public_key())
        .await
        .expect("dial");
    let accepted = b_accept.await.expect("join");
    (Arc::new(dialed), Arc::new(accepted))
}

fn empty_hints() -> ResourceHints {
    ResourceHints {
        cpu_class: harness_core::protocol::CpuClass::Light,
        memory_mb: None,
        gpu_required: false,
        gpu_memory_mb: None,
        network_class: harness_core::protocol::NetworkClass::None,
        disk_io_class: harness_core::protocol::DiskIoClass::None,
        estimated_duration_ms: None,
    }
}

fn signed_task(identity: &Identity, input: serde_json::Value) -> Task {
    let mut t = Task {
        id: TaskId::new_v7(),
        parent: None,
        plan_id: None,
        capability: "echo".into(),
        input,
        constraints: Constraints::default(),
        retry: RetryPolicy::default(),
        execution: ExecutionPolicy::default(),
        resource_hints: empty_hints(),
        trace_ctx: TraceContext::default(),
        issued_by: identity.public_key().node_id(),
        issued_at: 1_700_000_000_000,
        tags: Vec::new(),
        sig: Signature::from_bytes([0u8; 64]),
    };
    t.sign(identity).expect("sign task");
    t
}

fn signed_assign(identity: &Identity, seq: u64, task: Task) -> TaskAssign {
    let mut a = TaskAssign {
        seq,
        lease_id: LeaseId::new_v7(),
        task,
        assigned_by: identity.public_key().node_id(),
        lease_expires_at: 1_700_000_060_000,
        sig: Signature::from_bytes([0u8; 64]),
    };
    a.sign(identity).expect("sign assign");
    a
}

fn sample_heartbeat(identity: &Identity, seq: u64) -> Heartbeat {
    let node: NodeId = identity.public_key().node_id();
    let mut hb = Heartbeat {
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
        replica_head: [0u8; 32],
        in_flight: vec![],
        leader_belief: node,
        brain_score: 0,
        on_battery: false,
        paused: false,
        version: SemVer::new(0, 1, 0),
        sig: Signature::from_bytes([0u8; 64]),
    };
    hb.sign(identity).expect("sign hb");
    hb
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t01_named_channel_round_trip() {
    let (a, a_id) = build_transport();
    let (b, b_id) = build_transport();
    let (dialer, accepter) = connect_pair(&a, &b, &b_id).await;

    let router = tokio::spawn(async move {
        let ch = accepter.accept_channel().await.expect("accept channel");
        assert_eq!(ch.name(), channels::TASK_ASSIGN);
        let assign: TaskAssign = ch.recv_sequenced().await.expect("recv assign");
        assign
    });

    let ch = dialer
        .channel(channels::TASK_ASSIGN)
        .await
        .expect("open channel");
    let assign = signed_assign(
        &a_id,
        ch.next_seq(),
        signed_task(&a_id, serde_json::json!({})),
    );
    ch.send(&assign).await.expect("send");

    let got = router.await.expect("join");
    assert_eq!(got, assign);
    // The inner task's issuer signature survives the trip.
    got.task
        .verify_signature(a_id.public_key())
        .expect("inner task sig");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t02_channel_replay_rejected_and_fresh_stream_seq0_accepted() {
    let (a, a_id) = build_transport();
    let (b, b_id) = build_transport();
    let (dialer, accepter) = connect_pair(&a, &b, &b_id).await;

    let router = tokio::spawn(async move {
        let ch = accepter.accept_channel().await.expect("accept channel");
        // seq 0 accepted exactly once on a fresh stream…
        let first: TaskClaim = ch.recv_sequenced().await.expect("first seq0");
        assert_eq!(first.seq, 0);
        // …the replayed copy is rejected.
        let err = ch.recv_sequenced::<TaskClaim>().await.expect_err("replay");
        assert!(matches!(err, TransportError::Replay { got: 0, .. }));
    });

    let ch = dialer
        .channel(channels::TASK_CLAIM)
        .await
        .expect("open channel");
    let mut claim = TaskClaim {
        seq: 0,
        lease_id: LeaseId::new_v7(),
        task_id: TaskId::new_v7(),
        worker: a_id.public_key().node_id(),
        sig: Signature::from_bytes([0u8; 64]),
    };
    claim.sign(&a_id).expect("sign");
    ch.send(&claim).await.expect("send 1");
    ch.send(&claim).await.expect("send 2 (replay)");
    router.await.expect("join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t03_unknown_channel_reset_does_not_kill_connection() {
    let (a, _a_id) = build_transport();
    let (b, b_id) = build_transport();
    let (dialer, accepter) = connect_pair(&a, &b, &b_id).await;

    let router = tokio::spawn(async move {
        // The router must skip the bogus stream and deliver the next
        // valid one, on the same still-alive connection.
        let ch = accepter.accept_channel().await.expect("accept channel");
        assert_eq!(ch.name(), channels::ANNOUNCE);
    });

    dialer
        .raw_open_named_stream_for_test("harness.evil")
        .await
        .expect("open bogus");
    dialer
        .channel(channels::ANNOUNCE)
        .await
        .expect("open good channel");
    router.await.expect("join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t04_duplicate_channel_name_second_stream_reset() {
    let (a, _a_id) = build_transport();
    let (b, b_id) = build_transport();
    let (dialer, accepter) = connect_pair(&a, &b, &b_id).await;

    let router = tokio::spawn(async move {
        let first = accepter.accept_channel().await.expect("first");
        assert_eq!(first.name(), channels::TASK_CLAIM);
        // Second stream with the same name is skipped; the next distinct
        // name comes through instead.
        let second = accepter.accept_channel().await.expect("second");
        assert_eq!(second.name(), channels::TASK_RESULT);
    });

    dialer
        .raw_open_named_stream_for_test(channels::TASK_CLAIM)
        .await
        .expect("open 1");
    dialer
        .raw_open_named_stream_for_test(channels::TASK_CLAIM)
        .await
        .expect("open dup");
    dialer
        .raw_open_named_stream_for_test(channels::TASK_RESULT)
        .await
        .expect("open distinct");
    router.await.expect("join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t05_per_channel_frame_cap_enforced_on_send() {
    let (a, a_id) = build_transport();
    let (b, b_id) = build_transport();
    let (dialer, _accepter) = connect_pair(&a, &b, &b_id).await;

    let ch = dialer
        .channel(channels::TASK_ASSIGN)
        .await
        .expect("open assign");
    // The assign channel caps at 1 MiB; a >1 MiB input must be refused
    // locally without poisoning the stream.
    let big = "x".repeat(1024 * 1024 + 512);
    let assign = signed_assign(
        &a_id,
        ch.next_seq(),
        signed_task(&a_id, serde_json::json!({ "blob": big })),
    );
    let err = ch.send(&assign).await.expect_err("must exceed cap");
    assert!(matches!(err, TransportError::FrameTooLarge { .. }));

    // The stream is still usable for a normal-sized frame.
    let ok = signed_assign(
        &a_id,
        ch.next_seq(),
        signed_task(&a_id, serde_json::json!({})),
    );
    ch.send(&ok).await.expect("normal frame after refusal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t06_result_channel_carries_multi_megabyte_frame() {
    let (a, a_id) = build_transport();
    let (b, b_id) = build_transport();
    let (dialer, accepter) = connect_pair(&a, &b, &b_id).await;

    let worker = Identity::generate();
    let worker_node = worker.public_key().node_id();

    let router = tokio::spawn(async move {
        let ch = accepter.accept_channel().await.expect("accept");
        assert_eq!(ch.name(), channels::TASK_RESULT);
        let msg: TaskResultMsg = ch.recv_sequenced().await.expect("recv big result");
        msg
    });

    let ch = dialer
        .channel(channels::TASK_RESULT)
        .await
        .expect("open result");
    let mut result = FinalResult {
        task_id: TaskId::new_v7(),
        node_id: worker_node,
        started_at: 0,
        finished_at: 1,
        status: Status::Ok,
        output: serde_json::json!({ "stdout": "y".repeat(3 * 1024 * 1024) }),
        cost: Cost {
            tokens_in: 0,
            tokens_out: 0,
            usd: 0.0,
            wall_ms: 1,
            node_id: worker_node,
        },
        logs: vec![],
        provenance: vec![],
        sig: Signature::from_bytes([0u8; 64]),
    };
    // Inner signed by the executing node, outer by the channel peer —
    // the real 3.3 layering (both are verified independently).
    result.sign(&worker).expect("sign inner");
    let mut msg = TaskResultMsg {
        seq: ch.next_seq(),
        lease_id: LeaseId::new_v7(),
        result,
        sig: Signature::from_bytes([0u8; 64]),
    };
    msg.sign(&a_id).expect("sign outer");
    ch.send(&msg).await.expect("send big frame");

    let got = router.await.expect("join");
    got.result
        .verify_signature(worker.public_key())
        .expect("inner result sig");
    assert_eq!(
        got.result.output["stdout"].as_str().map(str::len),
        Some(3 * 1024 * 1024)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t07_concurrent_channels_interleave_heartbeats_and_assigns() {
    // The daemon-shape invariant (review R1): every stream is named —
    // heartbeats included — and ONE router per connection accepts them
    // all, dispatching per-channel recv tasks. Legacy unnamed streams and
    // `accept_channel` must never share a connection's accepter side.
    let (a, a_id) = build_transport();
    let (b, b_id) = build_transport();
    let (dialer, accepter) = connect_pair(&a, &b, &b_id).await;

    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<&'static str>(4);
    let router = tokio::spawn({
        let accepter = accepter.clone();
        async move {
            let mut assign_id = None;
            for _ in 0..2 {
                let ch = accepter.accept_channel().await.expect("accept channel");
                match ch.name() {
                    n if n == channels::HEARTBEAT => {
                        let done = done_tx.clone();
                        tokio::spawn(async move {
                            for _ in 0..3 {
                                let _hb: Heartbeat =
                                    ch.recv_sequenced().await.expect("hb over channel");
                            }
                            let _ = done.send("heartbeats").await;
                        });
                    }
                    n if n == channels::TASK_ASSIGN => {
                        let assign: TaskAssign = ch.recv_sequenced().await.expect("assign");
                        assign_id = Some(assign.task.id);
                        let _ = done_tx.send("assign").await;
                    }
                    other => panic!("unexpected channel {other}"),
                }
            }
            assign_id
        }
    });

    // Dialer side: interleave heartbeat-channel sends with assign traffic.
    let hb_ch = dialer
        .channel(channels::HEARTBEAT)
        .await
        .expect("open hb channel");
    hb_ch.send(&sample_heartbeat(&a_id, 1)).await.expect("hb1");
    let ch = dialer
        .channel(channels::TASK_ASSIGN)
        .await
        .expect("open assign");
    hb_ch.send(&sample_heartbeat(&a_id, 2)).await.expect("hb2");
    let assign = signed_assign(
        &a_id,
        ch.next_seq(),
        signed_task(&a_id, serde_json::json!({})),
    );
    ch.send(&assign).await.expect("assign send");
    hb_ch.send(&sample_heartbeat(&a_id, 3)).await.expect("hb3");

    let mut seen = Vec::new();
    for _ in 0..2 {
        seen.push(done_rx.recv().await.expect("done signal"));
    }
    seen.sort_unstable();
    assert_eq!(seen, vec!["assign", "heartbeats"]);
    let got_id = router.await.expect("router join").expect("assign id");
    assert_eq!(got_id, assign.task.id);
}
