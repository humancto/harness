//! Phase 3.3a — `task_results` CRUD tests.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::doc_markdown
)]

use harness_core::{
    Constraints, ExecutionPolicy, NodeId, ResourceHints, RetryPolicy, Signable, Signature, Task,
    TaskId, TraceContext,
};
use harness_store::Store;
use serde_json::json;

fn fresh_store() -> Store {
    Store::open_memory().expect("open memory store")
}

fn dummy_task(id: TaskId, capability: &str) -> Task {
    let mut t = Task {
        id,
        parent: None,
        plan_id: None,
        capability: capability.to_string(),
        input: json!({}),
        constraints: Constraints::default(),
        retry: RetryPolicy::default(),
        execution: ExecutionPolicy::default(),
        resource_hints: ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        },
        trace_ctx: TraceContext::default(),
        issued_by: NodeId::from_bytes([1; 16]),
        issued_at: 1_700_000_000_000,
        tags: Vec::new(),
        sig: Signature::from_bytes([0u8; 64]),
    };
    let id_priv = harness_core::Identity::generate();
    t.sign(&id_priv).expect("sign");
    t
}

#[test]
fn t01_write_done_then_load_round_trips_json() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id, "echo")).expect("insert");

    let output = json!({ "echoed": { "msg": "hello" }, "count": 42 });
    s.write_task_result_done(id, &output, 1_700_000_000_500, NodeId::from_bytes([2; 16]))
        .expect("write done");

    let loaded = s.load_task_result(id).expect("load").expect("present");
    assert_eq!(loaded.output, Some(output));
    assert!(loaded.error.is_none());
    assert_eq!(loaded.completed_at_ms, 1_700_000_000_500);
    assert_eq!(loaded.completed_by, NodeId::from_bytes([2; 16]));
}

#[test]
fn t02_write_failed_then_load_round_trips_error() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id, "shell.exec"))
        .expect("insert");

    s.write_task_result_failed(
        id,
        "policy denied: no allow rule matched cat",
        1_700_000_000_900,
        NodeId::from_bytes([3; 16]),
    )
    .expect("write failed");

    let loaded = s.load_task_result(id).expect("load").expect("present");
    assert!(loaded.output.is_none());
    assert_eq!(
        loaded.error.as_deref(),
        Some("policy denied: no allow rule matched cat")
    );
}

#[test]
fn t03_replace_on_retry_via_on_conflict() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id, "echo")).expect("insert");

    // First write — Failed.
    s.write_task_result_failed(
        id,
        "first error",
        1_700_000_000_000,
        NodeId::from_bytes([1; 16]),
    )
    .expect("first");
    // Second write — Done. ON CONFLICT DO UPDATE replaces output, clears error.
    s.write_task_result_done(
        id,
        &json!({"final": true}),
        1_700_000_000_500,
        NodeId::from_bytes([1; 16]),
    )
    .expect("second");

    let loaded = s.load_task_result(id).expect("load").expect("present");
    assert_eq!(loaded.output, Some(json!({"final": true})));
    assert!(
        loaded.error.is_none(),
        "error must be cleared by ON CONFLICT DO UPDATE"
    );
}

#[test]
fn t04_load_missing_returns_none() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    assert!(s.load_task_result(id).expect("load").is_none());
}

// ---- 4.5 (ADR-0027): provenance column ---------------------------------

fn contributions() -> Vec<harness_core::NodeContribution> {
    use harness_core::protocol::NodeStatus;
    vec![
        harness_core::NodeContribution {
            node_id: NodeId::from_bytes([7; 16]),
            status: NodeStatus::Ok,
            duration_ms: 120,
            item_count: 5,
        },
        harness_core::NodeContribution {
            node_id: NodeId::from_bytes([8; 16]),
            status: NodeStatus::Failed,
            duration_ms: 45,
            item_count: 0,
        },
    ]
}

#[test]
fn t05_provenance_round_trips_on_done_and_failed() {
    let s = fresh_store();
    let done_id = TaskId::new_v7();
    let failed_id = TaskId::new_v7();
    s.insert_task(&dummy_task(done_id, "mesh.grep"))
        .expect("insert");
    s.insert_task(&dummy_task(failed_id, "mesh.grep"))
        .expect("insert");

    let prov = contributions();
    s.write_task_result_done_with_provenance(
        done_id,
        &json!({"items": []}),
        1_700_000_001_000,
        NodeId::from_bytes([9; 16]),
        &prov,
    )
    .expect("write done");
    s.write_task_result_failed_with_provenance(
        failed_id,
        "node [8] failed: boom",
        1_700_000_001_000,
        NodeId::from_bytes([9; 16]),
        &prov,
    )
    .expect("write failed");

    let done = s.load_task_result(done_id).expect("load").expect("present");
    assert_eq!(done.provenance.as_deref(), Some(prov.as_slice()));
    let failed = s
        .load_task_result(failed_id)
        .expect("load")
        .expect("present");
    assert_eq!(failed.provenance.as_deref(), Some(prov.as_slice()));
}

#[test]
fn t06_plain_writes_leave_provenance_none_and_replace_clears_it() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id, "echo")).expect("insert");

    // Federated write first…
    s.write_task_result_done_with_provenance(
        id,
        &json!({"v": 1}),
        1_700_000_000_100,
        NodeId::from_bytes([2; 16]),
        &contributions(),
    )
    .expect("federated write");
    assert!(s
        .load_task_result(id)
        .expect("load")
        .expect("present")
        .provenance
        .is_some());

    // …then a plain retry write must clear it (excluded.provenance = NULL).
    s.write_task_result_done(
        id,
        &json!({"v": 2}),
        1_700_000_000_200,
        NodeId::from_bytes([2; 16]),
    )
    .expect("plain write");
    let loaded = s.load_task_result(id).expect("load").expect("present");
    assert_eq!(loaded.output, Some(json!({"v": 2})));
    assert!(loaded.provenance.is_none());
}

/// V0006 applies cleanly over a populated V0005-era database: rows written
/// before the column existed load back with `provenance: None`.
#[test]
fn t07_v0006_migrates_populated_v0005_database() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("old.db");

    // Build a V0005-era database by applying the shipped migration files
    // 1..=5 directly, then populate a task_results row with pre-4.5 SQL.
    {
        let conn = rusqlite::Connection::open(&path).expect("raw open");
        conn.execute_batch(
            "CREATE TABLE _migrations (
                version    INTEGER PRIMARY KEY NOT NULL,
                name       TEXT    NOT NULL,
                applied_at INTEGER NOT NULL
            ) WITHOUT ROWID;",
        )
        .expect("migrations table");
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        for (version, file) in [
            (1, "V0001__initial_schema.sql"),
            (2, "V0002__leases.sql"),
            (3, "V0003__replica.sql"),
            (4, "V0004__task_results.sql"),
            (5, "V0005__assigned_node.sql"),
        ] {
            let sql = std::fs::read_to_string(base.join(file)).expect("read migration");
            conn.execute_batch(&sql).expect("apply migration");
            conn.execute(
                "INSERT INTO _migrations(version, name, applied_at) VALUES (?, ?, 0)",
                rusqlite::params![version, file],
            )
            .expect("record migration");
        }
        conn.execute(
            "INSERT INTO tasks (id, capability, state, issued_by, issued_at,
                                canonical_cbor, signature)
             VALUES (?1, 'echo', 'done', ?2, 0, x'00', ?3)",
            rusqlite::params![
                uuid::Uuid::from_bytes([5; 16]).as_bytes(),
                NodeId::from_bytes([6; 16]).as_bytes(),
                vec![0u8; 64],
            ],
        )
        .expect("old task row");
        conn.execute(
            "INSERT INTO task_results (task_id, output, error, completed_at_ms, completed_by)
             VALUES (?1, ?2, NULL, 42, ?3)",
            rusqlite::params![
                uuid::Uuid::from_bytes([5; 16]).as_bytes(),
                "{\"old\":true}",
                NodeId::from_bytes([6; 16]).as_bytes(),
            ],
        )
        .expect("old row");
    }

    // Store::open applies V0006+V0007 on top; the old row must load
    // with provenance None, cost None, and intact output.
    let cfg = harness_store::StoreConfig::at(&path);
    let s = Store::open(&cfg).expect("open migrates");
    assert_eq!(s.schema_version().expect("version"), "7");
    let loaded = s
        .load_task_result(TaskId(uuid::Uuid::from_bytes([5; 16])))
        .expect("load")
        .expect("present");
    assert_eq!(loaded.output, Some(json!({"old": true})));
    assert!(loaded.provenance.is_none());
    assert!(loaded.cost_usd.is_none(), "pre-5.9 rows read NULL cost");
}

#[test]
fn t08_result_cost_round_trip_and_ledger_queries() {
    // 5.9: write_result_cost persists dollars readable via
    // load_task_result AND the bounded ledger feeds.
    let s = fresh_store();
    let id = TaskId::new_v7();
    let plan_id = harness_core::PlanId::new_v7();
    let mut task = dummy_task(id, "echo");
    task.plan_id = Some(plan_id);
    let signer = harness_core::Identity::generate();
    task.sign(&signer).expect("re-sign");
    s.insert_task(&task).expect("insert");
    s.write_task_result_done(
        id,
        &json!({"cost_usd": 0.5}),
        1_000,
        NodeId::from_bytes([2; 16]),
    )
    .expect("row");
    s.write_result_cost(id, 0.5).expect("cost");
    let loaded = s.load_task_result(id).expect("load").expect("present");
    assert_eq!(loaded.cost_usd, Some(0.5));

    // A NULL-cost row must be excluded IN SQL, before the cap —
    // free completions cannot evict paid rows (Codex P1 on #60).
    let free = TaskId::new_v7();
    s.insert_task(&dummy_task(free, "echo")).expect("insert");
    s.write_task_result_done(free, &json!({}), 1_500, NodeId::from_bytes([2; 16]))
        .expect("row");
    let rows = s.recent_result_costs(0, 1).expect("feed");
    assert_eq!(rows.len(), 1, "cap spent on the COSTED row only");
    assert_eq!(rows[0].4, Some(0.5));
    let rows = s.recent_result_costs(0, 100).expect("feed");
    assert_eq!(rows.len(), 1);
    let (cap, pid, by, at, cost) = &rows[0];
    assert_eq!(cap, "echo");
    assert_eq!(*pid, Some(plan_id));
    assert_eq!(*by, NodeId::from_bytes([1; 16]));
    assert_eq!(*at, 1_000);
    assert_eq!(*cost, Some(0.5));
    // Window bound excludes it.
    assert!(s.recent_result_costs(2_000, 100).expect("feed").is_empty());

    let outs = s
        .recent_outputs_for_capability("echo", 0, 10)
        .expect("outs");
    assert_eq!(outs.len(), 2, "both echo outputs (cost is irrelevant here)");
    assert!(s
        .recent_outputs_for_capability("other", 0, 10)
        .expect("outs")
        .is_empty());
}
