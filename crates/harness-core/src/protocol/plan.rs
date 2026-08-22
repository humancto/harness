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

// --- `$task_output` references (roadmap 4.3, ADR-0025) ---------------
//
// A `PlanNode.input` may embed, anywhere in its JSON tree, an object of
// the reserved shape
//
// ```json
// {"$task_output": "<step TaskId as uuid string>", "pointer": "/opt/json/pointer"}
// ```
//
// which the DAG executor replaces with the referenced step's output
// (or the value at the RFC 6901 `pointer` within it) before dispatch.
// `$task_output` is a reserved key: an object containing it is ALWAYS
// treated as a reference, and any other shape around it is malformed.
// A reference is only legal to a direct declared dependency — enforced
// by plan validation (harness-brain) and again by the executor.

/// One parsed `$task_output` reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRef {
    pub task: TaskId,
    /// RFC 6901 JSON pointer into the referenced output.
    pub pointer: Option<String>,
}

/// Reference walking / resolution failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OutputRefError {
    #[error("malformed $task_output reference: {0}")]
    Malformed(String),
    #[error("reference to unavailable task output {}", .0.0)]
    UnknownTask(TaskId),
    #[error("pointer {pointer:?} not found in output of task {}", .task.0)]
    PointerMiss { task: TaskId, pointer: String },
}

fn parse_ref(obj: &serde_json::Map<String, JsonValue>) -> Result<OutputRef, OutputRefError> {
    let raw = obj
        .get("$task_output")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| OutputRefError::Malformed("$task_output must be a uuid string".into()))?;
    let task = raw
        .parse::<uuid::Uuid>()
        .map(TaskId)
        .map_err(|_| OutputRefError::Malformed(format!("not a uuid: {raw}")))?;
    let pointer = match obj.get("pointer") {
        None => None,
        Some(JsonValue::String(p)) => Some(p.clone()),
        Some(_) => return Err(OutputRefError::Malformed("pointer must be a string".into())),
    };
    if obj.keys().any(|k| k != "$task_output" && k != "pointer") {
        return Err(OutputRefError::Malformed(
            "reference object allows only $task_output and pointer keys".into(),
        ));
    }
    Ok(OutputRef { task, pointer })
}

/// Every `$task_output` reference in `input`, in walk order. A
/// malformed reference (the reserved key present with the wrong shape)
/// is an error, not a skip.
pub fn find_output_refs(input: &JsonValue) -> Result<Vec<OutputRef>, OutputRefError> {
    let mut out = Vec::new();
    collect_refs(input, &mut out)?;
    Ok(out)
}

fn collect_refs(v: &JsonValue, out: &mut Vec<OutputRef>) -> Result<(), OutputRefError> {
    match v {
        JsonValue::Object(obj) => {
            if obj.contains_key("$task_output") {
                out.push(parse_ref(obj)?);
                return Ok(());
            }
            for child in obj.values() {
                collect_refs(child, out)?;
            }
            Ok(())
        }
        JsonValue::Array(items) => {
            for child in items {
                collect_refs(child, out)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// `input` with every reference replaced via `lookup`. `lookup`
/// returning `None` fails resolution (the referenced output is not
/// available — undeclared dependency or unsettled step).
pub fn resolve_output_refs(
    input: &JsonValue,
    lookup: &dyn Fn(TaskId) -> Option<JsonValue>,
) -> Result<JsonValue, OutputRefError> {
    match input {
        JsonValue::Object(obj) => {
            if obj.contains_key("$task_output") {
                let r = parse_ref(obj)?;
                let output = lookup(r.task).ok_or(OutputRefError::UnknownTask(r.task))?;
                return match &r.pointer {
                    None => Ok(output),
                    Some(p) => {
                        output
                            .pointer(p)
                            .cloned()
                            .ok_or_else(|| OutputRefError::PointerMiss {
                                task: r.task,
                                pointer: p.clone(),
                            })
                    }
                };
            }
            let mut resolved = serde_json::Map::with_capacity(obj.len());
            for (k, child) in obj {
                resolved.insert(k.clone(), resolve_output_refs(child, lookup)?);
            }
            Ok(JsonValue::Object(resolved))
        }
        JsonValue::Array(items) => Ok(JsonValue::Array(
            items
                .iter()
                .map(|child| resolve_output_refs(child, lookup))
                .collect::<Result<_, _>>()?,
        )),
        other => Ok(other.clone()),
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

    /// Locks the edge orientation — `(from, to)` means "from depends on to,"
    /// i.e. `to` must complete before `from` starts. ADR-0002 carries the
    /// rationale; this test catches a silent flip if a future refactor
    /// reverses the convention without updating the doc-comment + ADR.
    ///
    /// Concrete contract: in a chain `a → b → c` (where `c` depends on `b`,
    /// and `b` depends on `a`), the predecessors of `c` according to
    /// `Plan::edges` are `{b}`, and `a`'s predecessors are `{}` (a is a
    /// root: nothing must complete before it starts).
    #[test]
    fn plan_edges_express_from_depends_on_to() {
        let a = TaskId::new_v7();
        let b = TaskId::new_v7();
        let c = TaskId::new_v7();
        let mut tasks = HashMap::new();
        tasks.insert(a, sample_node(a));
        tasks.insert(b, sample_node(b));
        tasks.insert(c, sample_node(c));
        let p = Plan {
            tasks,
            edges: vec![(b, a), (c, b)],
            ..sample_plan()
        };

        // For each task, find its dependencies — the right-hand side of
        // any `(from, to)` pair where `from == task`.
        let predecessors_of = |task: TaskId| -> Vec<TaskId> {
            p.edges
                .iter()
                .filter_map(|(from, to)| if *from == task { Some(*to) } else { None })
                .collect()
        };

        assert_eq!(predecessors_of(a), Vec::<TaskId>::new(), "a is a root");
        assert_eq!(predecessors_of(b), vec![a], "b depends on a");
        assert_eq!(predecessors_of(c), vec![b], "c depends on b");
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
    fn output_refs_found_and_resolved_with_pointer() {
        let dep = TaskId::new_v7();
        let input = serde_json::json!({
            "msg": { "$task_output": dep.0.to_string(), "pointer": "/echoed/msg" },
            "nested": [ { "$task_output": dep.0.to_string() } ],
            "literal": 42,
        });
        let refs = find_output_refs(&input).expect("find");
        assert_eq!(refs.len(), 2);
        assert!(refs.iter().all(|r| r.task == dep));
        assert_eq!(refs[0].pointer.as_deref(), Some("/echoed/msg"));

        let output = serde_json::json!({ "echoed": { "msg": "hi" } });
        let resolved =
            resolve_output_refs(&input, &|t| (t == dep).then(|| output.clone())).expect("resolve");
        assert_eq!(resolved["msg"], "hi");
        assert_eq!(resolved["nested"][0], output);
        assert_eq!(resolved["literal"], 42);
    }

    #[test]
    fn output_ref_malformed_shapes_error() {
        for bad in [
            serde_json::json!({ "$task_output": 7 }),
            serde_json::json!({ "$task_output": "not-a-uuid" }),
            serde_json::json!({ "$task_output": TaskId::new_v7().0.to_string(), "pointer": 3 }),
            serde_json::json!({ "$task_output": TaskId::new_v7().0.to_string(), "extra": true }),
        ] {
            assert!(matches!(
                find_output_refs(&bad),
                Err(OutputRefError::Malformed(_))
            ));
        }
    }

    #[test]
    fn output_ref_unknown_task_and_pointer_miss() {
        let dep = TaskId::new_v7();
        let input = serde_json::json!({ "$task_output": dep.0.to_string() });
        assert!(matches!(
            resolve_output_refs(&input, &|_| None),
            Err(OutputRefError::UnknownTask(t)) if t == dep
        ));
        let with_ptr =
            serde_json::json!({ "$task_output": dep.0.to_string(), "pointer": "/missing" });
        assert!(matches!(
            resolve_output_refs(&with_ptr, &|_| Some(serde_json::json!({"a": 1}))),
            Err(OutputRefError::PointerMiss { .. })
        ));
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
