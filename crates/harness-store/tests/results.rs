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
