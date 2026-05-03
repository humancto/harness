//! Task envelope (PRD §13.3). The unit of dispatch.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::identity::{NodeId, Signature};
use crate::ids::{PlanId, TaskId};
use crate::protocol::cardinality::PartialPolicy;
use crate::protocol::manifest::ResourceHints;
use crate::protocol::signable::Signable;

/// W3C Trace Context fields carried on every task.
///
/// `traceparent` and `tracestate` are opaque ASCII strings; validation lives
/// in the `tracing-opentelemetry` glue (Phase 5+). Empty strings are valid
/// and mean "no parent span."
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct TraceContext {
    pub traceparent: String,
    pub tracestate: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Constraints {
    /// Unix milliseconds. After this point the dispatcher must not assign a
    /// fresh worker; an in-flight worker may still complete (PRD §14.5).
    pub deadline: Option<u64>,
    pub max_cost_usd: Option<f64>,
    /// Refuses cloud capabilities even if eligible; PRD §15 local-first.
    pub must_be_local: bool,
    pub require_tags: Vec<String>,
    pub exclude_tags: Vec<String>,
    pub pin_to_node: Option<NodeId>,
    /// Free-form scope identifier (capability-defined; e.g. directory path).
    pub pin_to_scope: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u8,
    pub backoff_initial_ms: u32,
    pub backoff_multiplier: u8,
    pub backoff_max_ms: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_initial_ms: 250,
            backoff_multiplier: 2,
            backoff_max_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    /// 1 = normal, 2 = speculative (PRD §14.5; first worker wins).
    pub redundancy: u8,
    pub timeout_ms: u32,
    pub on_partial: PartialPolicy,
    /// Length of a single claim. Worker must extend or release before
    /// expiry; dispatcher may re-dispatch on lease expiry (PRD §14.5).
    pub lease_ms: u32,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            redundancy: 1,
            timeout_ms: 30_000,
            on_partial: PartialPolicy::FailFast,
            lease_ms: 10_000,
        }
    }
}

/// One unit of work as it travels on the wire (PRD §13.3).
///
/// `PartialEq` only — `input: JsonValue` is not `Eq` (NaN). Same constraint
/// `NodeManifest` already lives with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub parent: Option<TaskId>,
    pub plan_id: Option<PlanId>,
    pub capability: String,
    pub input: JsonValue,
    pub constraints: Constraints,
    pub retry: RetryPolicy,
    pub execution: ExecutionPolicy,
    pub resource_hints: ResourceHints,
    pub trace_ctx: TraceContext,
    pub issued_by: NodeId,
    /// Unix milliseconds.
    pub issued_at: u64,
    /// Caller hints — task-level routing/policy/scheduling metadata,
    /// distinct from capability-input arguments. Honored variably by
    /// capabilities; e.g. `llm.local.*` reads `"interactive"` to
    /// bypass the micro-batcher (3.5). Default empty for backward
    /// compatibility with old senders.
    #[serde(default)]
    pub tags: Vec<String>,
    pub sig: Signature,
}

impl Signable for Task {
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
    use crate::protocol::support::{CpuClass, DiskIoClass, NetworkClass};

    fn sample_hints() -> ResourceHints {
        ResourceHints {
            cpu_class: CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: NetworkClass::None,
            disk_io_class: DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    fn sample_task() -> Task {
        Task {
            id: TaskId::new_v7(),
            parent: None,
            plan_id: None,
            capability: "echo".into(),
            input: serde_json::json!({"msg": "hello"}),
            constraints: Constraints::default(),
            retry: RetryPolicy::default(),
            execution: ExecutionPolicy::default(),
            resource_hints: sample_hints(),
            trace_ctx: TraceContext::default(),
            issued_by: NodeId::from_bytes([0x11; 16]),
            issued_at: 1_700_000_000_000,
            tags: Vec::new(),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    fn round_trip(task: &Task) -> Task {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(task, &mut buf).expect("encode");
        ciborium::de::from_reader(buf.as_slice()).expect("decode")
    }

    #[test]
    fn task_round_trip() {
        let t = sample_task();
        assert_eq!(t, round_trip(&t));
    }

    #[test]
    fn task_minimal_round_trip() {
        let t = Task {
            input: JsonValue::Null,
            ..sample_task()
        };
        assert_eq!(t, round_trip(&t));
    }

    #[test]
    fn task_with_parent_and_plan_id_round_trip() {
        let t = Task {
            parent: Some(TaskId::new_v7()),
            plan_id: Some(PlanId::new_v7()),
            ..sample_task()
        };
        assert_eq!(t, round_trip(&t));
    }

    #[test]
    fn sign_then_verify() {
        let id = Identity::generate();
        let mut t = sample_task();
        t.issued_by = id.public_key().node_id();
        t.sign(&id).expect("sign");
        assert!(t.verify_signature(id.public_key()).is_ok());
    }

    #[test]
    fn tampered_field_fails_verify() {
        let id = Identity::generate();
        let mut t = sample_task();
        t.issued_by = id.public_key().node_id();
        t.sign(&id).expect("sign");
        t.capability = "evil".into();
        assert!(t.verify_signature(id.public_key()).is_err());
    }
}
