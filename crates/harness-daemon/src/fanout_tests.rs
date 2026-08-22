//! 3.3-fanout money tests — two full daemons in one process over
//! loopback QUIC (static peers, mDNS off), exercising the whole path:
//! submit → dispatch → QUIC assign → remote execute → result home.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use harness_core::{
    Constraints, ExecutionPolicy, Identity, NodeId, ResourceHints, RetryPolicy, Signable,
    Signature, Task, TaskId, TraceContext,
};
use harness_mesh::{AddedVia, Peer, TrustStore, TrustTier};
use harness_store::{Store, TaskState};
use tokio::sync::watch;

use crate::lifecycle::{DaemonOrchestrator, DaemonRuntimeConfig};

struct TestDaemon {
    identity: Arc<Identity>,
    store: Store,
    mesh_addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    root: tempfile::TempDir,
}

impl TestDaemon {
    fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }

    async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.handle).await;
    }
}

/// Generate two identities + cross-trusted roots, then boot daemons.
/// `b` dials `a` via a static peer hint.
async fn boot_pair(policy_toml: Option<&str>) -> (TestDaemon, TestDaemon) {
    let root_a = tempfile::tempdir().expect("root a");
    let root_b = tempfile::tempdir().expect("root b");
    let id_a = Arc::new(harness_mesh::identity::init_or_load(root_a.path()).expect("id a"));
    let id_b = Arc::new(harness_mesh::identity::init_or_load(root_b.path()).expect("id b"));

    let trust_a = TrustStore::open(root_a.path(), id_a.node_id()).expect("trust a");
    let trust_b = TrustStore::open(root_b.path(), id_b.node_id()).expect("trust b");
    trust_a.add(peer_of(&id_b, "node-b")).expect("a trusts b");
    trust_b.add(peer_of(&id_a, "node-a")).expect("b trusts a");

    if let Some(policy) = policy_toml {
        std::fs::write(root_a.path().join("policy.toml"), policy).expect("policy a");
        std::fs::write(root_b.path().join("policy.toml"), policy).expect("policy b");
    }

    let a = boot_one(root_a, id_a, trust_a, "node-a", vec![]).await;
    let b = boot_one(root_b, id_b, trust_b, "node-b", vec![a.mesh_addr]).await;
    (a, b)
}

fn peer_of(id: &Identity, hostname: &str) -> Peer {
    Peer {
        node_id: id.node_id(),
        pubkey: *id.public_key(),
        hostname: hostname.into(),
        tier: TrustTier::Default,
        added_at: 0,
        added_via: AddedVia::Static,
    }
}

async fn boot_one(
    root: tempfile::TempDir,
    identity: Arc<Identity>,
    trust: TrustStore,
    node_name: &str,
    static_peers: Vec<SocketAddr>,
) -> TestDaemon {
    let cfg = DaemonRuntimeConfig {
        mesh_name: "fanout-test".into(),
        node_name: node_name.into(),
        api_bind: SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), 0),
        mesh_bind: SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), 0),
        mdns_enabled: false,
        static_peers,
        harness_root: root.path().to_path_buf(),
    };
    let orch = DaemonOrchestrator::build(identity.clone(), trust, cfg)
        .await
        .expect("build daemon");
    let mesh_addr = orch.mesh_addr();
    let store = orch.store();
    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = orch.run_until(stop_rx).await;
    });
    TestDaemon {
        identity,
        store,
        mesh_addr,
        stop_tx,
        handle,
        root,
    }
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

fn submit_task(
    daemon: &TestDaemon,
    capability: &str,
    input: serde_json::Value,
    pin_to: Option<NodeId>,
    timeout_ms: u32,
) -> TaskId {
    let mut task = Task {
        id: TaskId::new_v7(),
        parent: None,
        plan_id: None,
        capability: capability.into(),
        input,
        constraints: Constraints {
            pin_to_node: pin_to,
            ..Constraints::default()
        },
        retry: RetryPolicy::default(),
        execution: ExecutionPolicy {
            timeout_ms,
            // Small lease on purpose: the dispatcher must size the TTL
            // from timeout_ms (ADR-0017 / R2).
            lease_ms: 100,
            ..ExecutionPolicy::default()
        },
        resource_hints: empty_hints(),
        trace_ctx: TraceContext::default(),
        issued_by: daemon.node_id(),
        issued_at: now_ms(),
        tags: vec![],
        sig: Signature::from_bytes([0u8; 64]),
    };
    task.sign(&daemon.identity).expect("sign");
    daemon.store.insert_task(&task).expect("insert");
    task.id
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

async fn wait_for_state(
    store: &Store,
    id: TaskId,
    accept: &[TaskState],
    budget: Duration,
) -> TaskState {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Ok(Some(state)) = store.task_state(id) {
            if accept.contains(&state) {
                return state;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let last = store.task_state(id).ok().flatten();
            panic!("task {id:?} never reached {accept:?}; last state {last:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until `a`'s capability index (via its store manifests) knows `b`.
async fn wait_for_mesh(a: &TestDaemon, b: &TestDaemon) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = a.store.load_manifest(b.node_id()).ok().flatten().is_some()
            && b.store.load_manifest(a.node_id()).ok().flatten().is_some();
        if seen {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemons never exchanged manifests"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m01_pinned_task_executes_on_remote_node_end_to_end() {
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    let id = submit_task(
        &a,
        "echo",
        serde_json::json!({"msg": "over the wire"}),
        Some(b.node_id()),
        5_000,
    );

    let state = wait_for_state(&a.store, id, &[TaskState::Done], Duration::from_secs(15)).await;
    assert_eq!(state, TaskState::Done);

    // Result on the issuer names the worker.
    let row = a.store.load_task_result(id).expect("load").expect("row");
    assert_eq!(row.completed_by, b.node_id());
    assert_eq!(
        row.output,
        Some(serde_json::json!({"echoed": {"msg": "over the wire"}}))
    );
    // Worker's own store also holds the terminal row + result.
    assert_eq!(
        b.store.task_state(id).expect("state"),
        Some(TaskState::Done)
    );
    assert!(b.store.load_task_result(id).expect("load").is_some());

    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m02_long_task_survives_short_lease_exactly_once() {
    // R2: runtime (~1.2 s) >> lease_ms (100 ms). The TTL must be sized
    // from timeout_ms so the lease survives and the command runs once.
    let policy = r#"
[shell]
allow = [{ cmd = "sh", any_args = true }]
"#;
    let (a, b) = boot_pair(Some(policy)).await;
    wait_for_mesh(&a, &b).await;

    let marker = b.root.path().join("ran.txt");
    let script = format!("sleep 1.2 && echo ran >> {}", marker.display());
    let id = submit_task(
        &a,
        "shell.exec",
        serde_json::json!({"cmd": "sh", "args": ["-c", script], "timeout_ms": 5_000}),
        Some(b.node_id()),
        5_000,
    );

    let state = wait_for_state(&a.store, id, &[TaskState::Done], Duration::from_secs(20)).await;
    assert_eq!(state, TaskState::Done);
    // Give any (buggy) duplicate execution a moment to also land…
    tokio::time::sleep(Duration::from_millis(500)).await;
    let content = std::fs::read_to_string(&marker).expect("marker written on b");
    assert_eq!(
        content.lines().count(),
        1,
        "command must run exactly once, got: {content:?}"
    );

    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m03_worker_death_pinned_task_fails_terminally_with_reason() {
    // Worker dies mid-lease. The lease expires, the task resets, the
    // pinned node never comes back → after the eligibility window the
    // task fails terminally with an actionable error (R6), instead of
    // spinning forever.
    let policy = r#"
[shell]
allow = [{ cmd = "sleep", any_args = true }]
"#;
    let (a, b) = boot_pair(Some(policy)).await;
    wait_for_mesh(&a, &b).await;

    let id = submit_task(
        &a,
        "shell.exec",
        serde_json::json!({"cmd": "sleep", "args": ["30"], "timeout_ms": 30_000}),
        Some(b.node_id()),
        1_000, // small execution timeout → small lease TTL (test builds)
    );

    // Wait until the worker has actually claimed it, then kill the worker.
    wait_for_state(
        &a.store,
        id,
        &[TaskState::Dispatched, TaskState::Claimed],
        Duration::from_secs(10),
    )
    .await;
    b.stop().await;

    // Lease TTL (~1.7 s in test builds) + peer timeout (6 s) + the
    // 2 s test eligibility window + margins.
    let state = wait_for_state(&a.store, id, &[TaskState::Failed], Duration::from_secs(30)).await;
    assert_eq!(state, TaskState::Failed);
    let row = a.store.load_task_result(id).expect("load").expect("row");
    let err = row.error.expect("error text");
    assert!(
        err.contains("undispatchable"),
        "error must carry the routing reason, got: {err}"
    );

    a.stop().await;
}
