//! Phase 3.3-fanout — `assigned_node` plumbing: `try_dispatch_task`,
//! `insert_task_dispatched`, `list_tasks_by_state_assigned`, and the
//! `expire_and_reset_task` assignment-clearing invariant.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use harness_core::{
    Constraints, ExecutionPolicy, NodeId, ResourceHints, RetryPolicy, Signable, Signature, Task,
    TaskId, TraceContext,
};
use harness_store::{Store, TaskState};
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

const NODE_A: [u8; 16] = [0xAA; 16];
const NODE_B: [u8; 16] = [0xBB; 16];

#[test]
fn t01_try_dispatch_sets_state_and_assignment() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");

    let node = NodeId::from_bytes(NODE_A);
    assert!(s.try_dispatch_task(id, node).expect("dispatch"));
    assert_eq!(
        s.task_state(id).expect("state"),
        Some(TaskState::Dispatched)
    );
    assert_eq!(s.assigned_node(id).expect("assigned"), Some(node));
}

#[test]
fn t02_try_dispatch_second_caller_loses() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");

    assert!(s
        .try_dispatch_task(id, NodeId::from_bytes(NODE_A))
        .expect("first"));
    assert!(!s
        .try_dispatch_task(id, NodeId::from_bytes(NODE_B))
        .expect("second"));
    // Loser must not have overwritten the assignment.
    assert_eq!(
        s.assigned_node(id).expect("assigned"),
        Some(NodeId::from_bytes(NODE_A))
    );
}

#[test]
fn t03_try_dispatch_missing_task_is_ok_false() {
    let s = fresh_store();
    assert!(!s
        .try_dispatch_task(TaskId::new_v7(), NodeId::from_bytes(NODE_A))
        .expect("missing"));
}

#[test]
fn t04_insert_dispatched_is_idempotent() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    let task = dummy_task(id);
    let me = NodeId::from_bytes(NODE_B);

    assert!(s.insert_task_dispatched(&task, me).expect("first insert"));
    // Re-delivered assignment (reconnect): no error, no second row.
    assert!(!s.insert_task_dispatched(&task, me).expect("second insert"));
    assert_eq!(
        s.task_state(id).expect("state"),
        Some(TaskState::Dispatched)
    );
    assert_eq!(s.assigned_node(id).expect("assigned"), Some(me));

    // The stored envelope round-trips intact.
    let loaded = s.load_task(id).expect("load").expect("present");
    assert_eq!(loaded, task);
}

#[test]
fn t05_insert_dispatched_does_not_clobber_existing_row() {
    // A task that already exists locally (e.g. submitted here, then a
    // buggy/malicious peer re-assigns the same id) must be untouched.
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert submitted");

    let evil = dummy_task(id);
    assert!(!s
        .insert_task_dispatched(&evil, NodeId::from_bytes(NODE_B))
        .expect("ignored"));
    assert_eq!(s.task_state(id).expect("state"), Some(TaskState::Submitted));
    assert_eq!(s.assigned_node(id).expect("assigned"), None);
}

#[test]
fn t06_list_by_state_assigned_filters() {
    let s = fresh_store();
    let a = NodeId::from_bytes(NODE_A);
    let b = NodeId::from_bytes(NODE_B);

    let id_a = TaskId::new_v7();
    let id_b = TaskId::new_v7();
    let id_unassigned = TaskId::new_v7();
    s.insert_task(&dummy_task(id_a)).expect("insert a");
    s.insert_task(&dummy_task(id_b)).expect("insert b");
    s.insert_task(&dummy_task(id_unassigned)).expect("insert u");
    assert!(s.try_dispatch_task(id_a, a).expect("dispatch a"));
    assert!(s.try_dispatch_task(id_b, b).expect("dispatch b"));

    let for_a = s
        .list_tasks_by_state_assigned(TaskState::Dispatched, Some(a))
        .expect("list a");
    assert_eq!(for_a.len(), 1);
    assert_eq!(for_a[0].id, id_a);

    let for_b = s
        .list_tasks_by_state_assigned(TaskState::Dispatched, Some(b))
        .expect("list b");
    assert_eq!(for_b.len(), 1);
    assert_eq!(for_b[0].id, id_b);

    let unassigned_submitted = s
        .list_tasks_by_state_assigned(TaskState::Submitted, None)
        .expect("list unassigned");
    assert_eq!(unassigned_submitted.len(), 1);
    assert_eq!(unassigned_submitted[0].id, id_unassigned);
}

#[test]
fn t07_expire_and_reset_clears_assignment() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");
    let node = NodeId::from_bytes(NODE_A);
    assert!(s.try_dispatch_task(id, node).expect("dispatch"));

    let lease = s.create_lease(id, node, 1, 1).expect("lease");
    // TTL 1ms — force wall-clock expiry.
    std::thread::sleep(std::time::Duration::from_millis(5));
    let expired = s.find_expired(u64::MAX, 10).expect("find expired");
    assert!(expired.iter().any(|l| l.lease_id == lease.lease_id));

    assert!(s.expire_and_reset_task(lease.lease_id).expect("reset"));
    assert_eq!(s.task_state(id).expect("state"), Some(TaskState::Submitted));
    assert_eq!(
        s.assigned_node(id).expect("assigned"),
        None,
        "reset must clear assigned_node or re-dispatch will see a stale owner"
    );
}

#[test]
fn t08_try_complete_pending_or_claimed() {
    // A result whose claim was lost must still complete the lease (R3).
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");
    let node = NodeId::from_bytes(NODE_A);
    assert!(s.try_dispatch_task(id, node).expect("dispatch"));
    let lease = s.create_lease(id, node, 60_000, 1).expect("lease");

    // Straight from `pending` (no claim ever arrived).
    assert!(s
        .try_complete_pending_or_claimed(lease.lease_id)
        .expect("complete from pending"));
    // Second completion attempt: lease is terminal now.
    assert!(!s
        .try_complete_pending_or_claimed(lease.lease_id)
        .expect("already completed"));

    // And from `claimed` on a fresh lease.
    let id2 = TaskId::new_v7();
    s.insert_task(&dummy_task(id2)).expect("insert2");
    assert!(s.try_dispatch_task(id2, node).expect("dispatch2"));
    let lease2 = s.create_lease(id2, node, 60_000, 1).expect("lease2");
    assert!(s.try_claim(lease2.lease_id, node).expect("claim"));
    assert!(s
        .try_complete_pending_or_claimed(lease2.lease_id)
        .expect("complete from claimed"));
}

#[test]
fn t09_expire_and_reset_guarded_by_lease_worker() {
    // An orphan lease naming a different worker must not reset a task
    // that has since been (re-)dispatched elsewhere (R12).
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");
    let node_a = NodeId::from_bytes(NODE_A);
    let node_b = NodeId::from_bytes(NODE_B);

    // Task is assigned to B, but the stale lease names A.
    assert!(s.try_dispatch_task(id, node_b).expect("dispatch to b"));
    let stale = s.create_lease(id, node_a, 1, 1).expect("stale lease");
    std::thread::sleep(std::time::Duration::from_millis(5));

    assert!(s.expire_and_reset_task(stale.lease_id).expect("expire"));
    // Lease is expired, but the task assignment to B survives.
    assert_eq!(
        s.task_state(id).expect("state"),
        Some(TaskState::Dispatched),
        "guarded reset must not touch a task assigned to another node"
    );
    assert_eq!(s.assigned_node(id).expect("assigned"), Some(node_b));
}

#[test]
fn t10_submitted_to_failed_supervisor_hop_is_legal() {
    let s = fresh_store();
    let id = TaskId::new_v7();
    s.insert_task(&dummy_task(id)).expect("insert");
    assert!(s
        .try_transition_task(id, TaskState::Submitted, TaskState::Failed)
        .expect("supervisor hop"));
    assert_eq!(s.task_state(id).expect("state"), Some(TaskState::Failed));
}
