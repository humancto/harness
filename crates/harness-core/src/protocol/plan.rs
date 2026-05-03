//! Plan envelope (PRD §13.5). A signed DAG of tasks the brain emits.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::identity::{NodeId, Signature};
use crate::ids::{PlanId, TaskId};
use crate::protocol::manifest::ResourceHints;
use crate::protocol::signable::Signable;

/// One node within a plan DAG. Effectively a `Task` minus the issuer
/// identity (the plan's signer is the issuer for every embedded task).
///
/// The dispatcher materializes `Task` envelopes from `PlanNode`s at
/// dispatch time, then signs each one — this keeps the plan small and
/// avoids two layers of redundant signatures.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanNode {
    pub id: TaskId,
    pub capability: String,
    pub input: JsonValue,
    pub resource_hints: ResourceHints,
    /// Optional override; falls back to the plan's defaults.
    pub timeout_ms: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BudgetAction {
    Pause,
    Cancel,
    Notify,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Budget {
    pub max_cost_usd: Option<f64>,
    pub soft_limit_usd: Option<f64>,
    pub on_exceed: BudgetAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HashFn {
    Blake3,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum CheckpointStorage {
    None,
    Sqlite,
    File { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckpointConfig {
    pub enabled: bool,
    pub interval_items: u32,
    pub storage: CheckpointStorage,
    pub input_hash_fn: HashFn,
}

/// A signed DAG of tasks (PRD §13.5).
///
/// `tasks` is keyed by `TaskId` so the brain can emit tasks in any order;
/// `edges` are `(from, to)` pairs encoding "from depends on to" — i.e.,
/// `to` must complete before `from` starts. Plan validation in 3.9
/// enforces acyclicity. Plans are content-addressable only after sorting
/// tasks by `TaskId` (a 3.9-time concern; ADR-0002 captures the deferral).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    pub id: PlanId,
    pub name: String,
    pub tasks: HashMap<TaskId, PlanNode>,
    pub edges: Vec<(TaskId, TaskId)>,
    pub budget: Option<Budget>,
    pub checkpoint: Option<CheckpointConfig>,
    pub issued_by: NodeId,
    pub sig: Signature,
}

impl Signable for Plan {
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

    fn sample_node(id: TaskId) -> PlanNode {
        PlanNode {
            id,
            capability: "echo".into(),
            input: serde_json::json!({"msg": "hi"}),
            resource_hints: sample_hints(),
            timeout_ms: None,
        }
    }

    fn sample_plan() -> Plan {
        let n1 = TaskId::new_v7();
        let mut tasks = HashMap::new();
        tasks.insert(n1, sample_node(n1));
        Plan {
            id: PlanId::new_v7(),
            name: "minimal".into(),
            tasks,
            edges: vec![],
            budget: None,
            checkpoint: None,
            issued_by: NodeId::from_bytes([0x11; 16]),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    fn round_trip<T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug>(
        v: &T,
    ) -> T {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(v, &mut buf).expect("encode");
        ciborium::de::from_reader(buf.as_slice()).expect("decode")
    }

    #[test]
    fn plan_empty_round_trip() {
        let p = sample_plan();
        assert_eq!(p, round_trip(&p));
    }

    #[test]
    fn plan_three_node_chain_round_trip() {
        let a = TaskId::new_v7();
        let b = TaskId::new_v7();
        let c = TaskId::new_v7();
        let mut tasks = HashMap::new();
        tasks.insert(a, sample_node(a));
        tasks.insert(b, sample_node(b));
        tasks.insert(c, sample_node(c));
        // c depends on b, b depends on a (`(from, to)` = "from depends on to")
        let p = Plan {
            tasks,
            edges: vec![(b, a), (c, b)],
            ..sample_plan()
        };
        assert_eq!(p, round_trip(&p));
    }

    #[test]
    fn plan_node_round_trip() {
        let n = sample_node(TaskId::new_v7());
        assert_eq!(n, round_trip(&n));
    }

    #[test]
    fn budget_round_trip() {
        let b = Budget {
            max_cost_usd: Some(5.0),
            soft_limit_usd: Some(2.5),
            on_exceed: BudgetAction::Cancel,
        };
        assert_eq!(b, round_trip(&b));
    }

    #[test]
    fn checkpoint_each_storage_round_trips() {
        for storage in [
            CheckpointStorage::None,
            CheckpointStorage::Sqlite,
            CheckpointStorage::File {
                path: "/tmp/cp.db".into(),
            },
        ] {
            let cfg = CheckpointConfig {
                enabled: true,
                interval_items: 100,
                storage,
                input_hash_fn: HashFn::Blake3,
            };
            assert_eq!(cfg, round_trip(&cfg));
        }
    }

    #[test]
    fn budget_action_each_variant_round_trips() {
        for action in [
            BudgetAction::Pause,
            BudgetAction::Cancel,
            BudgetAction::Notify,
        ] {
            let b = Budget {
                max_cost_usd: None,
                soft_limit_usd: None,
                on_exceed: action,
            };
            assert_eq!(b, round_trip(&b));
        }
    }

    #[test]
    fn plan_sign_then_verify() {
        let id = Identity::generate();
        let mut p = sample_plan();
        p.issued_by = id.public_key().node_id();
        p.sign(&id).expect("sign");
        assert!(p.verify_signature(id.public_key()).is_ok());
    }

    #[test]
    fn plan_with_budget_and_checkpoint_round_trips() {
        let p = Plan {
            budget: Some(Budget {
                max_cost_usd: Some(10.0),
                soft_limit_usd: None,
                on_exceed: BudgetAction::Notify,
            }),
            checkpoint: Some(CheckpointConfig {
                enabled: true,
                interval_items: 10,
                storage: CheckpointStorage::Sqlite,
                input_hash_fn: HashFn::Blake3,
            }),
            ..sample_plan()
        };
        assert_eq!(p, round_trip(&p));
    }
}
