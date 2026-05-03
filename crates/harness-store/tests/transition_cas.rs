//! Phase 3.3a — `try_transition_task` (CAS) semantics.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use harness_core::{
    Constraints, ExecutionPolicy, NodeId, ResourceHints, RetryPolicy, Signable, Signature, Task,
    TaskId, TraceContext,
};
use harness_store::{Store, StoreError, TaskState};
use serde_json::json;

fn fresh_store() -> Store {
    Store::open_memory().expect("open memory store")
}

fn dummy_task(id: TaskId) -> Task {
    let mut t = Task {
        id,
        parent: None,
        plan_id: None,
        capability: "echo".into(),
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
    t.sign(&harness_core::Identity::generate()).expect("sign");
    t
}

#[test]
fn t05_cas_legal_hop_first_wins_second_loses() {
    // Two sequential CAS calls on the same legal hop. Second observes
    // the row already past `Submitted` and returns Ok(false).
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");

    let first = s
        .try_transition_task(id, TaskState::Submitted, TaskState::Dispatched)
        .expect("first cas");
    assert!(first, "first transition must win");

    let second = s
        .try_transition_task(id, TaskState::Submitted, TaskState::Dispatched)
        .expect("second cas");
    assert!(
        !second,
        "second transition must lose (state already advanced)"
    );
}

#[test]
fn t06_cas_illegal_hop_returns_invalid_transition_error() {
    // `Submitted → Running` is forbidden by `can_transition_to`. The
    // CAS rejects up-front (programmer error) instead of silently
    // returning Ok(false).
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");

    let err = s
        .try_transition_task(id, TaskState::Submitted, TaskState::Running)
        .expect_err("illegal hop must error");
    match err {
        StoreError::InvalidTransition { from, to } => {
            assert_eq!(from, "submitted");
            assert_eq!(to, "running");
        }
        other => panic!("expected InvalidTransition, got {other:?}"),
    }
}

#[test]
fn t06b_cas_full_ladder_walks_to_running() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");

    assert!(s
        .try_transition_task(id, TaskState::Submitted, TaskState::Dispatched)
        .expect("hop"));
    assert!(s
        .try_transition_task(id, TaskState::Dispatched, TaskState::Claimed)
        .expect("hop"));
    assert!(s
        .try_transition_task(id, TaskState::Claimed, TaskState::Running)
        .expect("hop"));
    assert_eq!(
        s.task_state(id).expect("query").expect("present"),
        TaskState::Running
    );
}

#[test]
fn t06c_cas_loses_on_missing_task() {
    // Task that was never inserted — UPDATE matches zero rows; CAS
    // returns Ok(false), not Err(NotFound). Caller decides what to do.
    let s = fresh_store();
    let id = TaskId::new_v7();
    let won = s
        .try_transition_task(id, TaskState::Submitted, TaskState::Dispatched)
        .expect("cas");
    assert!(!won);
}
