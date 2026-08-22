//! Cross-node dispatch envelopes (PRD §13.6 `harness.task.*` channels).
//!
//! Three messages carry a task across the mesh in 3.3-fanout:
//!
//! - [`TaskAssign`] — issuer → worker on `harness.task.assign`. Carries the
//!   full signed [`Task`] plus the lease the issuer minted for it.
//! - [`TaskClaim`] — worker → issuer on `harness.task.claim`. Acknowledges
//!   the assignment; the issuer CASes the lease `pending → claimed`.
//! - [`TaskResultMsg`] — worker → issuer on `harness.task.result`. Carries
//!   the worker's signed [`FinalResult`].
//!
//! PRD §13.6 names per-task logical channels
//! (`harness.task.result.<task_id>`); on the wire these are multiplexed
//! onto one aggregate stream per peer pair per direction, with `task_id`
//! inside the payload (ADR-0017). Aggregate streams keep the replay design
//! (`&'static str` channel keys, monotonic `seq` per stream) and bound the
//! stream count.
//!
//! Signature layering on [`TaskResultMsg`]: the **outer** `sig`
//! authenticates the channel peer that relayed the message; the **inner**
//! `FinalResult::sig` authenticates the node that executed the task. In
//! 3.3-fanout worker == peer, but the split keeps Phase 4 federated relays
//! honest — both are verified independently.

use serde::{Deserialize, Serialize};

use crate::identity::{NodeId, Signature};
use crate::ids::TaskId;
use crate::protocol::result::FinalResult;
use crate::protocol::signable::Signable;
use crate::protocol::task::Task;

/// Lease identifier minted by the issuer's store when it dispatches a task
/// to a remote worker. Opaque to the worker — it is echoed back verbatim in
/// [`TaskClaim`] and [`TaskResultMsg`] so the issuer can CAS the right
/// lease row. (The store-side row type lives in `harness-store::leases`;
/// the wire carries only the id.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseId(pub uuid::Uuid);

impl LeaseId {
    /// Generate a fresh Uuid v7 (sortable by creation time).
    ///
    /// # Panics
    ///
    /// See [`TaskId::new_v7`] — same `getrandom` semantics.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

/// Issuer → worker: "run this task under this lease."
///
/// `task` keeps its own issuer signature; the envelope `sig` is the channel
/// peer's. Workers verify both, then ingest the task at `Dispatched` and
/// answer with [`TaskClaim`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskAssign {
    /// Monotonic per (connection, channel). Replay protection at the
    /// transport layer via `Sequenced` in `harness-mesh`.
    pub seq: u64,
    pub lease_id: LeaseId,
    pub task: Task,
    /// The dispatching node. Must equal the connection peer's `NodeId`.
    pub assigned_by: NodeId,
    /// Unix milliseconds when the lease expires; informational for the
    /// worker (the issuer's lease table is authoritative).
    pub lease_expires_at: u64,
    pub sig: Signature,
}

impl Signable for TaskAssign {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

/// Worker → issuer: "assignment accepted; the task is in my store."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskClaim {
    /// Monotonic per (connection, channel).
    pub seq: u64,
    pub lease_id: LeaseId,
    pub task_id: TaskId,
    /// The claiming node. Must equal the connection peer's `NodeId`.
    pub worker: NodeId,
    pub sig: Signature,
}

impl Signable for TaskClaim {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

/// Worker → issuer: terminal result for a leased task.
///
/// See the module docs for the outer/inner signature layering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskResultMsg {
    /// Monotonic per (connection, channel).
    pub seq: u64,
    pub lease_id: LeaseId,
    pub result: FinalResult,
    pub sig: Signature,
}

impl Signable for TaskResultMsg {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::identity::Identity;
    use crate::protocol::result::{Cost, Status};
    use crate::protocol::support::{CpuClass, DiskIoClass, NetworkClass};
    use crate::protocol::task::{Constraints, ExecutionPolicy, RetryPolicy, TraceContext};
    use crate::protocol::ResourceHints;

    fn sample_task(issuer: NodeId) -> Task {
        Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: "echo".into(),
            input: serde_json::json!({"msg": "hello"}),
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy::default(),
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: NetworkClass::None,
                disk_io_class: DiskIoClass::None,
                estimated_duration_ms: None,
            },
            trace_ctx: TraceContext::default(),
            issued_by: issuer,
            issued_at: 1_700_000_000_000,
            tags: Vec::new(),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    fn sample_result(task_id: TaskId, node: NodeId) -> FinalResult {
        FinalResult {
            task_id,
            node_id: node,
            started_at: 1_700_000_000_000,
            finished_at: 1_700_000_000_500,
            status: Status::Ok,
            output: serde_json::json!({"echoed": {"msg": "hello"}}),
            cost: Cost {
                tokens_in: 0,
                tokens_out: 0,
                usd: 0.0,
                wall_ms: 500,
                node_id: node,
            },
            logs: Vec::new(),
            provenance: Vec::new(),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    fn round_trip<T: Signable + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(v: &T) {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(v, &mut buf).expect("encode");
        let back: T = ciborium::de::from_reader(buf.as_slice()).expect("decode");
        assert_eq!(*v, back);
    }

    #[test]
    fn assign_round_trip_and_sign_verify() {
        let issuer = Identity::generate();
        let issuer_id = issuer.public_key().node_id();
        let mut task = sample_task(issuer_id);
        task.sign(&issuer).expect("sign task");
        let mut assign = TaskAssign {
            seq: 1,
            lease_id: LeaseId::new_v7(),
            task,
            assigned_by: issuer_id,
            lease_expires_at: 1_700_000_010_000,
            sig: Signature::from_bytes([0u8; 64]),
        };
        assign.sign(&issuer).expect("sign assign");
        round_trip(&assign);
        assert!(assign.verify_signature(issuer.public_key()).is_ok());
        // The inner task signature survives the outer signing untouched.
        assert!(assign.task.verify_signature(issuer.public_key()).is_ok());
    }

    #[test]
    fn assign_tamper_inner_task_breaks_outer_sig() {
        let issuer = Identity::generate();
        let issuer_id = issuer.public_key().node_id();
        let mut task = sample_task(issuer_id);
        task.sign(&issuer).expect("sign task");
        let mut assign = TaskAssign {
            seq: 1,
            lease_id: LeaseId::new_v7(),
            task,
            assigned_by: issuer_id,
            lease_expires_at: 0,
            sig: Signature::from_bytes([0u8; 64]),
        };
        assign.sign(&issuer).expect("sign assign");
        assign.task.capability = "evil".into();
        assert!(assign.verify_signature(issuer.public_key()).is_err());
    }

    #[test]
    fn claim_round_trip_and_sign_verify() {
        let worker = Identity::generate();
        let mut claim = TaskClaim {
            seq: 1,
            lease_id: LeaseId::new_v7(),
            task_id: TaskId::new_v7(),
            worker: worker.public_key().node_id(),
            sig: Signature::from_bytes([0u8; 64]),
        };
        claim.sign(&worker).expect("sign");
        round_trip(&claim);
        assert!(claim.verify_signature(worker.public_key()).is_ok());
        claim.seq = 2;
        assert!(claim.verify_signature(worker.public_key()).is_err());
    }

    #[test]
    fn result_msg_inner_and_outer_sigs_are_independent() {
        let issuer = Identity::generate();
        let worker = Identity::generate();
        let worker_id = worker.public_key().node_id();
        let task = sample_task(issuer.public_key().node_id());
        let mut result = sample_result(task.id, worker_id);
        result.sign(&worker).expect("sign inner");
        let mut msg = TaskResultMsg {
            seq: 7,
            lease_id: LeaseId::new_v7(),
            result,
            sig: Signature::from_bytes([0u8; 64]),
        };
        msg.sign(&worker).expect("sign outer");
        round_trip(&msg);
        assert!(msg.verify_signature(worker.public_key()).is_ok());
        assert!(msg.result.verify_signature(worker.public_key()).is_ok());
        // Outer verification with the wrong key fails even though the inner
        // result is intact.
        assert!(msg.verify_signature(issuer.public_key()).is_err());
    }
}
