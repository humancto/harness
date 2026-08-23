//! Pinned sub-task construction + store-poll await — shared by the
//! `mesh.*` wrappers ([`crate::mesh_exec::StoreMeshExec`]) and the 4.5
//! federated coordinator ([`crate::federated::FederatedCoordinator`]).
//!
//! Extracted from `StoreMeshExec::submit_remote` (ADR-0022) so both
//! callers build byte-identical sub-task rows: signed, `parent`-linked,
//! `pin_to_node` set, and `max_attempts: 1` (the posthumous-work rule —
//! the lease TTL always outlives the caller's await deadline, so a
//! retry could only ever fire after the caller stopped listening).

use std::time::Duration;

use harness_capabilities::{CapabilityError, SubTaskOutcome};
use harness_core::{
    Constraints, ExecutionPolicy, Identity, NodeId, ResourceHints, RetryPolicy, Signable,
    Signature, Task, TaskId, TraceContext,
};
use harness_store::{Store, TaskState};

const AWAIT_POLL_MS: u64 = 100;

/// Workers CAS the terminal state FIRST, then write the result row
/// (`executor.rs`, `dispatch.rs::on_result`) — a poll landing in that
/// gap must not turn a successful sub-task into a failure. Grace polls
/// while state-is-terminal-but-row-is-missing, before giving up
/// (PR #48 review): 10 × 100 ms.
const TERMINAL_ROW_GRACE_POLLS: u32 = 10;

/// Build a signed sub-task pinned to one node. The caller inserts it;
/// the dispatch runtime routes it (self-pins run through the local
/// executor, remote pins go over QUIC).
pub(crate) fn build_pinned_subtask(
    identity: &Identity,
    capability: &str,
    input: serde_json::Value,
    pin_to: NodeId,
    parent: TaskId,
    timeout_ms: u32,
) -> Result<Task, CapabilityError> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let mut task = Task {
        id: TaskId::new_v7(),
        parent: Some(parent),
        plan_id: None,
        capability: capability.to_string(),
        input,
        constraints: Constraints {
            pin_to_node: Some(pin_to),
            ..Constraints::default()
        },
        retry: RetryPolicy {
            // No lease-expiry retry: the lease TTL (timeout + slack)
            // always outlives the caller's await deadline, so a retry
            // could only ever fire posthumously — pure orphan work
            // (ADR-0022 §Orphaned sub-tasks).
            max_attempts: 1,
            ..RetryPolicy::default()
        },
        execution: ExecutionPolicy {
            timeout_ms,
            ..ExecutionPolicy::default()
        },
        resource_hints: ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::Light,
            disk_io_class: harness_core::protocol::DiskIoClass::Light,
            estimated_duration_ms: None,
        },
        trace_ctx: TraceContext::default(),
        issued_by: identity.node_id(),
        issued_at: now_ms,
        tags: vec![],
        sig: Signature::from_bytes([0u8; 64]),
    };
    task.sign(identity)
        .map_err(|e| CapabilityError::Failed(format!("sign sub-task: {e}")))?;
    Ok(task)
}

/// Poll the store until `id` reaches a terminal state or `deadline`
/// elapses. 100 ms cadence — the same loop every wrapper await uses.
pub(crate) async fn await_terminal(
    store: &Store,
    id: TaskId,
    deadline: Duration,
) -> SubTaskOutcome {
    let until = tokio::time::Instant::now() + deadline;
    // Counts polls spent on a terminal state whose result row hasn't
    // landed yet (the worker's state-CAS → row-write gap). Once the
    // state is terminal the deadline no longer applies — only this
    // bounded grace does.
    let mut row_grace = 0u32;
    loop {
        match store.task_state(id) {
            Ok(Some(TaskState::Done)) => {
                if let Ok(Some(row)) = store.load_task_result(id) {
                    return SubTaskOutcome::Done(row.output.unwrap_or(serde_json::Value::Null));
                }
                row_grace += 1;
                if row_grace > TERMINAL_ROW_GRACE_POLLS {
                    return SubTaskOutcome::Failed("done without result row".into());
                }
            }
            Ok(Some(TaskState::Failed | TaskState::Expired | TaskState::Cancelled)) => {
                if let Ok(Some(row)) = store.load_task_result(id) {
                    return SubTaskOutcome::Failed(
                        row.error.unwrap_or_else(|| "sub-task failed".into()),
                    );
                }
                row_grace += 1;
                if row_grace > TERMINAL_ROW_GRACE_POLLS {
                    return SubTaskOutcome::Failed("sub-task failed".into());
                }
            }
            Ok(_) => {
                if tokio::time::Instant::now() >= until {
                    return SubTaskOutcome::TimedOut;
                }
            }
            Err(e) => return SubTaskOutcome::Failed(format!("store error: {e}")),
        }
        tokio::time::sleep(Duration::from_millis(AWAIT_POLL_MS)).await;
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Constraints, ExecutionPolicy, ResourceHints, RetryPolicy, TraceContext};

    fn insert_signed(store: &Store, identity: &Identity) -> TaskId {
        let mut t = Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: "echo".into(),
            input: serde_json::json!({}),
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
            issued_by: identity.node_id(),
            issued_at: 1_700_000_000_000,
            tags: vec![],
            sig: Signature::from_bytes([0u8; 64]),
        };
        t.sign(identity).expect("sign");
        store.insert_task(&t).expect("insert");
        t.id
    }

    fn walk_to_done_without_row(store: &Store, id: TaskId) {
        for next in [
            TaskState::Dispatched,
            TaskState::Claimed,
            TaskState::Running,
            TaskState::Done,
        ] {
            store.transition_task(id, next).expect("hop");
        }
    }

    /// PR #48 review (P2): a poll observing the worker's
    /// terminal-state → result-row write gap must wait for the row, not
    /// report a false failure.
    #[tokio::test(start_paused = true)]
    async fn await_terminal_rides_out_the_state_to_row_gap() {
        let store = Store::open_memory().expect("store");
        let identity = Identity::generate();
        let id = insert_signed(&store, &identity);
        walk_to_done_without_row(&store, id);

        // Land the row 300 ms after the state flip — inside the grace.
        let row_store = store.clone();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            row_store
                .write_task_result_done(
                    id,
                    &serde_json::json!({"late": true}),
                    1_700_000_000_100,
                    identity.node_id(),
                )
                .expect("write row");
        });
        let outcome = await_terminal(&store, id, Duration::from_secs(5)).await;
        writer.await.expect("writer");
        match outcome {
            SubTaskOutcome::Done(v) => assert_eq!(v, serde_json::json!({"late": true})),
            other => panic!("expected Done after the row lands, got {other:?}"),
        }
    }

    /// A row that never lands still terminates — after the bounded
    /// grace, not the (possibly much longer) caller deadline.
    #[tokio::test(start_paused = true)]
    async fn await_terminal_gives_up_after_bounded_grace() {
        let store = Store::open_memory().expect("store");
        let identity = Identity::generate();
        let id = insert_signed(&store, &identity);
        walk_to_done_without_row(&store, id);

        let outcome = await_terminal(&store, id, Duration::from_secs(600)).await;
        match outcome {
            SubTaskOutcome::Failed(e) => assert_eq!(e, "done without result row"),
            other => panic!("expected bounded-grace failure, got {other:?}"),
        }
    }
}
