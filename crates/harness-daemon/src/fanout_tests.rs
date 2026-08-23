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

pub(crate) struct TestDaemon {
    pub(crate) identity: Arc<Identity>,
    pub(crate) store: Store,
    pub(crate) peers: harness_mesh::heartbeat::PeerTable,
    pub(crate) mesh_addr: SocketAddr,
    pub(crate) partials: Arc<harness_api::PartialBuffers>,
    pub(crate) stop_tx: watch::Sender<bool>,
    pub(crate) handle: tokio::task::JoinHandle<()>,
    pub(crate) root: tempfile::TempDir,
}

impl TestDaemon {
    pub(crate) fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }

    pub(crate) async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.handle).await;
    }
}

/// Generate two identities + cross-trusted roots, then boot daemons.
/// `b` dials `a` via a static peer hint.
pub(crate) async fn boot_pair(policy_toml: Option<&str>) -> (TestDaemon, TestDaemon) {
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
    let peers = orch.peer_table();
    let partials = orch.partial_buffers();
    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = orch.run_until(stop_rx).await;
    });
    TestDaemon {
        identity,
        store,
        peers,
        mesh_addr,
        partials,
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

pub(crate) fn now_ms() -> u64 {
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

/// Wait until both nodes hold each other's manifest AND at least one
/// heartbeat (the `mesh_meta` target filter requires peer liveness).
pub(crate) async fn wait_for_mesh(a: &TestDaemon, b: &TestDaemon) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = a.store.load_manifest(b.node_id()).ok().flatten().is_some()
            && b.store.load_manifest(a.node_id()).ok().flatten().is_some()
            && a.peers.get(&b.node_id()).is_some()
            && b.peers.get(&a.node_id()).is_some();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m04_mesh_grep_federates_across_both_nodes() {
    // 3.11: a single mesh.grep task fans out to fs.grep on every
    // node/scope and concat-merges the results with provenance.
    let root_a = tempfile::tempdir().expect("root a");
    let root_b = tempfile::tempdir().expect("root b");
    let data_a = tempfile::tempdir().expect("data a");
    let data_b = tempfile::tempdir().expect("data b");
    std::fs::write(data_a.path().join("alpha.txt"), "the needle in a\n").expect("write a");
    std::fs::write(
        data_b.path().join("beta.txt"),
        "another needle in b\nplain line\n",
    )
    .expect("write b");
    for (root, data, id) in [(&root_a, &data_a, "docs-a"), (&root_b, &data_b, "docs-b")] {
        std::fs::write(
            root.path().join("scopes.toml"),
            format!(
                "[[scope]]\nid=\"{id}\"\nkind=\"directory\"\nlabel=\"L\"\nroot=\"{}\"\n",
                data.path().display()
            ),
        )
        .expect("scopes.toml");
    }

    let id_a = Arc::new(harness_mesh::identity::init_or_load(root_a.path()).expect("id a"));
    let id_b = Arc::new(harness_mesh::identity::init_or_load(root_b.path()).expect("id b"));
    let trust_a = TrustStore::open(root_a.path(), id_a.node_id()).expect("trust a");
    let trust_b = TrustStore::open(root_b.path(), id_b.node_id()).expect("trust b");
    trust_a.add(peer_of(&id_b, "node-b")).expect("a trusts b");
    trust_b.add(peer_of(&id_a, "node-a")).expect("b trusts a");
    let a = boot_one(root_a, id_a, trust_a, "node-a", vec![]).await;
    let b = boot_one(root_b, id_b, trust_b, "node-b", vec![a.mesh_addr]).await;
    wait_for_mesh(&a, &b).await;

    let id = submit_task(
        &a,
        "mesh.grep",
        serde_json::json!({ "pattern": "needle", "timeout_ms": 15_000 }),
        None, // Anyone — either node may coordinate
        20_000,
    );
    let state = wait_for_state(&a.store, id, &[TaskState::Done], Duration::from_secs(30)).await;
    assert_eq!(state, TaskState::Done);

    let row = a.store.load_task_result(id).expect("load").expect("row");
    let output = row.output.expect("output");
    let results = output["results"].as_array().expect("results array");
    assert_eq!(results.len(), 2, "both scopes must contribute: {output:#}");
    let scopes: Vec<&str> = results
        .iter()
        .map(|r| r["scope"].as_str().unwrap())
        .collect();
    assert!(scopes.contains(&"docs-a") && scopes.contains(&"docs-b"));
    // Each block's matches carry the file + line.
    for block in results {
        let matches = block["result"]["matches"].as_array().expect("matches");
        assert_eq!(matches.len(), 1, "one needle per scope: {block:#}");
        // Pin the real fs.grep MatchDto field names the CLI renders.
        assert!(matches[0]["path"].as_str().is_some());
        assert!(matches[0]["line_number"].as_u64().is_some());
        assert!(matches[0]["line"].as_str().unwrap().contains("needle"));
    }
    assert_eq!(output["provenance"]["targets_ok"], 2);
    assert_eq!(output["failures"].as_array().expect("failures").len(), 0);

    // 4.2 (ADR-0024): the coordinator streamed per-target progress into
    // the issuer's partial ring — 2 per-target frames + 1 summary. If
    // the wrapper ran on node-b, frames rode harness.task.partial; the
    // final 50 ms flush can trail the result, so poll briefly.
    let progress = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let frames: Vec<serde_json::Value> = a
                .partials
                .frames(id)
                .iter()
                .filter(|f| f.stream == "progress")
                .map(|f| serde_json::from_str(&f.line).expect("progress line is JSON"))
                .collect();
            if frames.len() >= 3 || tokio::time::Instant::now() >= deadline {
                break frames;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    assert_eq!(progress.len(), 3, "2 per-target + 1 summary: {progress:#?}");
    let target_scopes: Vec<&str> = progress
        .iter()
        .filter_map(|c| c["target"]["scope"].as_str())
        .collect();
    assert!(target_scopes.contains(&"docs-a") && target_scopes.contains(&"docs-b"));
    for c in progress.iter().filter(|c| !c["target"].is_null()) {
        assert_eq!(c["outcome"], "ok");
        assert_eq!(c["items"], 1, "one needle per scope: {c:#}");
        assert_eq!(c["total"], 2);
    }
    let summary = progress
        .iter()
        .find(|c| !c["summary"].is_null())
        .expect("summary frame");
    assert_eq!(summary["summary"]["total"], 2);
    assert_eq!(summary["summary"]["ok"], 2);
    assert_eq!(summary["summary"]["failed"], 0);

    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m05_plan_execute_chains_steps_with_output_threading() {
    // 4.3 (ADR-0025): a two-step echo chain where step 2's input embeds
    // step 1's output via $task_output — executed end-to-end through a
    // real daemon (plan.execute → step rows → dispatch → executor).
    let root = tempfile::tempdir().expect("root");
    let identity = Arc::new(harness_mesh::identity::init_or_load(root.path()).expect("id"));
    let trust = TrustStore::open(root.path(), identity.node_id()).expect("trust");
    let a = boot_one(root, identity, trust, "solo", vec![]).await;

    let step_a = TaskId::new_v7();
    let step_b = TaskId::new_v7();
    let plan = serde_json::json!({
        "id": harness_core::PlanId::new_v7().0.to_string(),
        "name": "chain",
        "tasks": {
            step_a.0.to_string(): {
                "id": step_a.0.to_string(),
                "capability": "echo",
                "input": { "msg": "from-step-one" },
                "resource_hints": {
                    "cpu_class": "light", "memory_mb": null, "gpu_required": false,
                    "gpu_memory_mb": null, "network_class": "none",
                    "disk_io_class": "none", "estimated_duration_ms": null
                },
                "timeout_ms": null
            },
            step_b.0.to_string(): {
                "id": step_b.0.to_string(),
                "capability": "echo",
                "input": { "carried": { "$task_output": step_a.0.to_string(), "pointer": "/echoed/msg" } },
                "resource_hints": {
                    "cpu_class": "light", "memory_mb": null, "gpu_required": false,
                    "gpu_memory_mb": null, "network_class": "none",
                    "disk_io_class": "none", "estimated_duration_ms": null
                },
                "timeout_ms": null
            }
        },
        "edges": [[step_b.0.to_string(), step_a.0.to_string()]],
        "budget": null,
        "checkpoint": null,
        "issued_by": identity_bytes(&a),
        "sig": vec![0u8; 64],
    });
    let id = submit_task(
        &a,
        "plan.execute",
        serde_json::json!({ "plan": plan, "timeout_ms": 30_000 }),
        None,
        40_000,
    );
    let state = wait_for_state(&a.store, id, &[TaskState::Done], Duration::from_secs(30)).await;
    assert_eq!(state, TaskState::Done);

    let row = a.store.load_task_result(id).expect("load").expect("row");
    let output = row.output.expect("output");
    assert_eq!(output["ok"], 2, "both steps done: {output:#}");
    let steps = output["steps"].as_object().expect("steps");
    let b_entry = &steps[&step_b.0.to_string()];
    assert_eq!(b_entry["state"], "done");
    assert_eq!(
        b_entry["output"]["echoed"]["carried"], "from-step-one",
        "step 1's output threaded into step 2: {b_entry:#}"
    );

    // Step rows exist with parent + plan_id set.
    let plan_id: harness_core::PlanId =
        serde_json::from_value(output["plan_id"].clone()).expect("plan id");
    let rows = a.store.list_tasks_by_plan(plan_id).expect("by plan");
    assert_eq!(rows.len(), 2, "one row per step");
    assert!(rows.iter().all(|(_, cap, state, parent)| cap == "echo"
        && *state == TaskState::Done
        && *parent == Some(id)));
    // Row ids in the aggregate map back to real rows.
    let row_ids: Vec<&str> = steps
        .values()
        .filter_map(|s| s["task_id"].as_str())
        .collect();
    assert_eq!(row_ids.len(), 2);

    // Progress frames: 2 step frames + 1 plan summary in the ring.
    let frames: Vec<serde_json::Value> = a
        .partials
        .frames(id)
        .iter()
        .filter(|f| f.stream == "progress")
        .map(|f| serde_json::from_str(&f.line).expect("json"))
        .collect();
    let step_frames = frames.iter().filter(|c| !c["step"].is_null()).count();
    let summaries = frames
        .iter()
        .filter(|c| !c["plan_summary"].is_null())
        .count();
    assert_eq!((step_frames, summaries), (2, 1), "{frames:#?}");

    a.stop().await;
}

fn identity_bytes(d: &TestDaemon) -> Vec<u8> {
    d.node_id().as_bytes().to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m06_mesh_info_federates_with_provenance_and_frames() {
    // 4.5 (ADR-0027): the first real Cardinality::Federated capability
    // end-to-end over QUIC — submit mesh.info on A, the coordinator
    // fans pinned sub-tasks to both nodes, Concat-merges the items, and
    // persists 2×Ok provenance; stage frames ride the partial ring.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    let id = submit_task(&a, "mesh.info", serde_json::json!({}), None, 20_000);
    let state = wait_for_state(&a.store, id, &[TaskState::Done], Duration::from_secs(30)).await;
    assert_eq!(state, TaskState::Done);

    let row = a.store.load_task_result(id).expect("load").expect("row");
    let output = row.output.expect("output");
    let items = output["items"].as_array().expect("items");
    assert_eq!(items.len(), 2, "one inventory item per node: {output:#}");
    let names: Vec<&str> = items
        .iter()
        .map(|i| i["node_name"].as_str().expect("node_name"))
        .collect();
    assert!(
        names.contains(&"node-a") && names.contains(&"node-b"),
        "both node names present: {names:?}"
    );
    assert_eq!(output["merge"]["strategy"], "concat");
    assert!(
        output["merge"].get("failures").is_none(),
        "clean run has no failures block"
    );

    // Persisted provenance: both nodes Ok, one item each, node ids match
    // the DispatchPlan's sorted order.
    let provenance = row.provenance.expect("provenance");
    assert_eq!(provenance.len(), 2);
    let mut expected: Vec<NodeId> = vec![a.node_id(), b.node_id()];
    expected.sort();
    for (contribution, node) in provenance.iter().zip(&expected) {
        assert_eq!(contribution.node_id, *node);
        assert_eq!(
            contribution.status,
            harness_core::protocol::NodeStatus::Ok,
            "provenance: {provenance:?}"
        );
        assert_eq!(contribution.item_count, 1);
    }

    // 4.2 pipe: discovered + 2 streaming + merging stage frames in the
    // issuer's ring (coordinator is local to the issuer).
    let stages: Vec<String> = a
        .partials
        .frames(id)
        .iter()
        .filter(|f| f.stream == "progress")
        .filter_map(|f| {
            serde_json::from_str::<serde_json::Value>(&f.line)
                .ok()
                .and_then(|v| {
                    v.pointer("/federated/stage")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
        })
        .collect();
    assert_eq!(
        stages.first().map(String::as_str),
        Some("discovered"),
        "stages: {stages:?}"
    );
    assert_eq!(stages.iter().filter(|s| *s == "streaming").count(), 2);
    assert!(stages.iter().any(|s| s == "merging"));

    // Mixed-version / pin rule end-to-end: a PINNED federated-cardinality
    // task must execute on that node (DispatchPlan::Single), not
    // re-coordinate — the old-issuer wire path.
    let pinned = submit_task(
        &a,
        "mesh.info",
        serde_json::json!({}),
        Some(b.node_id()),
        10_000,
    );
    let state = wait_for_state(
        &a.store,
        pinned,
        &[TaskState::Done],
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(state, TaskState::Done);
    let row = a
        .store
        .load_task_result(pinned)
        .expect("load")
        .expect("row");
    let output = row.output.expect("output");
    let items = output["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "pinned: executes on B alone: {output:#}");
    assert_eq!(items[0]["node_name"], "node-b");
    assert!(
        row.provenance.is_none(),
        "single-node execution carries no federated provenance"
    );

    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m07_mesh_info_node_death_returns_partial_with_failed_provenance() {
    // 4.5: kill B while it still looks live, then federate. The B
    // sub-task dies (send failure → lease/eligibility machinery →
    // Failed), ReturnPartial merges A's item alone, and provenance
    // records B as non-Ok.
    let (a, b) = boot_pair(None).await;
    wait_for_mesh(&a, &b).await;

    // Stop B; within the peer-timeout window A still counts it live and
    // fans out to it.
    let b_id = b.node_id();
    b.stop().await;

    let id = submit_task(&a, "mesh.info", serde_json::json!({}), None, 30_000);
    let state = wait_for_state(&a.store, id, &[TaskState::Done], Duration::from_secs(45)).await;
    assert_eq!(
        state,
        TaskState::Done,
        "ReturnPartial: A's item still merges"
    );

    let row = a.store.load_task_result(id).expect("load").expect("row");
    let output = row.output.expect("output");
    let items = output["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "only A contributed: {output:#}");
    assert_eq!(items[0]["node_name"], "node-a");
    let failures = output["merge"]["failures"].as_array().expect("failures");
    assert_eq!(failures.len(), 1, "B reported in merge.failures");
    assert_eq!(failures[0]["node"], b_id.to_string());

    let provenance = row.provenance.expect("provenance");
    assert_eq!(provenance.len(), 2);
    let b_contribution = provenance
        .iter()
        .find(|c| c.node_id == b_id)
        .expect("B in provenance");
    assert!(
        !matches!(
            b_contribution.status,
            harness_core::protocol::NodeStatus::Ok
        ),
        "B must be non-Ok: {provenance:?}"
    );
    let a_contribution = provenance
        .iter()
        .find(|c| c.node_id == a.node_id())
        .expect("A in provenance");
    assert_eq!(
        a_contribution.status,
        harness_core::protocol::NodeStatus::Ok
    );

    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m08_lease_extension_shrinks_lease_and_detects_dead_worker_fast() {
    // 4.6 (ADR-0028): worker-side LeaseExtend over QUIC. Phase 1: a
    // live worker's extensions SHRINK a 60 s lease to the rolling
    // 2 s (test) horizon and the task still completes exactly once.
    // Phase 2: a killed worker's task fails FAST (~horizon + expire
    // tick), not after the full 60 s timeout bound.
    let policy = r#"
[shell]
allow = [{ cmd = "sleep", any_args = true }]
"#;
    let (a, b) = boot_pair(Some(policy)).await;
    wait_for_mesh(&a, &b).await;

    // ---- Phase 1: live worker, long budget, short real runtime.
    let id = submit_task(
        &a,
        "shell.exec",
        serde_json::json!({"cmd": "sleep", "args": ["2"], "timeout_ms": 60_000}),
        Some(b.node_id()),
        60_000,
    );
    // Original lease ≈ now + 60.7 s. Wait for the first extension to
    // shrink it below now + 10 s (test horizon 2 s + margins).
    let lease = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let leases = a.store.list_leases_for_task(id).expect("leases");
            if let Some(lease) = leases.first() {
                if lease.expires_at < now_ms() + 10_000 {
                    break lease.clone();
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "lease never shrank: {leases:?}",
                leases = a.store.list_leases_for_task(id).expect("leases")
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    assert_eq!(lease.attempt, 1, "still the first attempt");
    // The rolling horizon keeps the live worker alive to completion.
    let state = wait_for_state(&a.store, id, &[TaskState::Done], Duration::from_secs(20)).await;
    assert_eq!(state, TaskState::Done, "extensions kept the lease alive");
    assert_eq!(
        a.store.list_leases_for_task(id).expect("leases").len(),
        1,
        "exactly one attempt — extensions never triggered a re-dispatch"
    );

    // ---- Phase 2: dead worker on a long-budget task.
    let id2 = submit_task(
        &a,
        "shell.exec",
        serde_json::json!({"cmd": "sleep", "args": ["50"], "timeout_ms": 60_000}),
        Some(b.node_id()),
        60_000,
    );
    // Wait until the extensions have taken over the lease horizon —
    // the kill must land AFTER the first shrink (plan review NIT-15).
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let leases = a.store.list_leases_for_task(id2).expect("leases");
            if leases
                .first()
                .is_some_and(|l| l.expires_at < now_ms() + 10_000)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "second lease never shrank"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let killed_at = tokio::time::Instant::now();
    b.stop().await;

    // Dead-worker detection: the shrunken lease expires within ~2 s,
    // the reset task backs off briefly, the pinned node goes un-live,
    // and the eligibility window (2 s test) terminalizes. All of that
    // is bounded FAR below the 60 s budget the old TTL would have
    // waited out.
    let state = wait_for_state(&a.store, id2, &[TaskState::Failed], Duration::from_secs(25)).await;
    assert_eq!(state, TaskState::Failed);
    let elapsed = killed_at.elapsed();
    assert!(
        elapsed < Duration::from_secs(25),
        "fast detection: took {elapsed:?}, budget was 60 s"
    );
    let row = a.store.load_task_result(id2).expect("load").expect("row");
    let err = row.error.expect("error");
    assert!(
        err.contains("undispatchable") || err.contains("expired"),
        "actionable reason: {err}"
    );

    a.stop().await;
}
