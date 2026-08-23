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
    loop {
        match store.task_state(id) {
            Ok(Some(TaskState::Done)) => {
                return match store.load_task_result(id) {
                    Ok(Some(row)) => {
                        SubTaskOutcome::Done(row.output.unwrap_or(serde_json::Value::Null))
                    }
                    _ => SubTaskOutcome::Failed("done without result row".into()),
                };
            }
            Ok(Some(TaskState::Failed | TaskState::Expired | TaskState::Cancelled)) => {
                let err = store
                    .load_task_result(id)
                    .ok()
                    .flatten()
                    .and_then(|r| r.error)
                    .unwrap_or_else(|| "sub-task failed".into());
                return SubTaskOutcome::Failed(err);
            }
            Ok(_) => {}
            Err(e) => return SubTaskOutcome::Failed(format!("store error: {e}")),
        }
        if tokio::time::Instant::now() >= until {
            return SubTaskOutcome::TimedOut;
        }
        tokio::time::sleep(Duration::from_millis(AWAIT_POLL_MS)).await;
    }
}
