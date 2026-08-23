//! Plan validation (Phase 3.8 well-formedness + 3.9 schema/cost).
//!
//! 3.8 ships:
//! - non-empty `tasks`
//! - every edge endpoint references an existing task id
//! - DAG is acyclic (Kahn's algorithm)
//! - every node's `capability` exists in `available_capabilities` by id
//!
//! 3.9 adds:
//! - JSON-Schema validation of `PlanNode.input` against the registered
//!   capability's `input_schema` (via [`crate::CapabilitySchemaIndex`])
//! - estimated cost ≤ `constraints.max_cost_usd` (when set)
//!
//! 5.3 (ADR-0032) adds §15.4 rule 4:
//! - `must_be_local` × cloud-capability conflict: a plan that names a
//!   cloud-tier capability (per the snapshot's `cloud_caps` set) while
//!   `constraints.must_be_local` is set fails with `LocalityConflict`.
//!   Foreign caps (input-override path) are not in the local set and
//!   are not flagged — the executing node's policy is the §10.4
//!   backstop.
//!
//! Still NOT enforced (deferred):
//! - `version_major` of capabilities (`PlanNode` has no version field today;
//!   ADR-0013 §10 documents the gap, ADR-0014 §11 carries it forward).

use std::collections::{HashMap, HashSet};

use harness_core::{CapabilityRef, Plan, TaskId};

use crate::backend::PlanConstraints;
use crate::error::PlanValidationError;
use crate::schema::CapabilitySchemaIndex;

/// 3.8 well-formedness checks only. Retained so callers that don't yet
/// have a `CapabilitySchemaIndex` and a cost number can still validate
/// the structural sanity of a plan. New code should use
/// [`validate_plan`].
#[deprecated(
    since = "0.1.0",
    note = "Phase 3.9 adds schema + cost-cap validation; use `validate_plan` instead. \
            `validate_plan_well_formed` will be removed in a future release."
)]
pub fn validate_plan_well_formed(
    plan: &Plan,
    available: &[CapabilityRef],
) -> Result<(), PlanValidationError> {
    well_formed(plan, available)
}

/// 3.9 full validation. Strict superset of 3.8 well-formedness:
/// 1. (3.8) `tasks` non-empty.
/// 2. (3.8) edges reference existing task ids.
/// 3. (3.8) DAG acyclic (Kahn's).
/// 4. (3.8) every node's capability id is in `available`.
/// 5. (3.9) every node's `input` validates against `schemas.get(cap_id)`.
///    Missing schema → `UnknownSchema`. **4.3 (ADR-0025):** nodes whose
///    input embeds `$task_output` references defer this check — the
///    resolved shape isn't known at plan time; the DAG executor
///    re-validates the resolved input immediately before dispatch.
/// 6. (3.9) `constraints.max_cost_usd` is respected when set:
///    `response_cost_usd <= cap`. Equality allowed.
/// 7. (4.3) `tasks[k].id == k` for every entry, and every
///    `$task_output` reference targets a direct declared dependency
///    (an edge `(node, referenced)` exists) — data dependencies must be
///    a subset of control dependencies.
///
/// 8. (5.3, §15.4 rule 4) when `constraints.must_be_local` is set, no
///    node may reference a capability in `cloud_caps` (the snapshot's
///    set of locally-registered cloud-tier capability ids).
///
/// `confidence_threshold` is NOT checked here — that lives at the
/// brain.plan executor so a low-confidence `Confident(_)` becomes an
/// escalation diagnostic, not a hard validation failure.
pub fn validate_plan(
    plan: &Plan,
    response_cost_usd: f64,
    constraints: &PlanConstraints,
    schemas: &CapabilitySchemaIndex,
    available: &[CapabilityRef],
    cloud_caps: &HashSet<String>,
) -> Result<(), PlanValidationError> {
    well_formed(plan, available)?;

    // 8. Locality conflict (5.3, ADR-0032). Checked early — it is the
    // cheapest rule and the clearest diagnostic for a repair prompt.
    if constraints.must_be_local {
        for (task_id, node) in &plan.tasks {
            if cloud_caps.contains(node.capability.as_str()) {
                return Err(PlanValidationError::LocalityConflict {
                    task: *task_id,
                    cap: node.capability.clone(),
                });
            }
        }
    }

    // 7. Key ↔ node-id agreement, and output-reference legality.
    let mut declared: HashMap<TaskId, HashSet<TaskId>> = HashMap::new();
    for &(from, to) in &plan.edges {
        declared.entry(from).or_default().insert(to);
    }
    for (task_id, node) in &plan.tasks {
        if node.id != *task_id {
            return Err(PlanValidationError::NodeIdMismatch {
                key: *task_id,
                id: node.id,
            });
        }
        let refs = harness_core::find_output_refs(&node.input).map_err(|e| {
            PlanValidationError::MalformedOutputRef {
                task: *task_id,
                detail: e.to_string(),
            }
        })?;
        for r in &refs {
            let ok = declared
                .get(task_id)
                .is_some_and(|deps| deps.contains(&r.task));
            if !ok {
                return Err(PlanValidationError::UndeclaredOutputRef {
                    task: *task_id,
                    referenced: r.task,
                });
            }
        }
    }

    // 5. Per-node schema validation (deferred for ref-bearing nodes —
    // the executor re-validates the resolved input, ADR-0025).
    for (task_id, node) in &plan.tasks {
        let has_refs = harness_core::find_output_refs(&node.input)
            .map(|r| !r.is_empty())
            .unwrap_or(false);
        if has_refs {
            continue;
        }
        match schemas.validate(&node.capability, &node.input) {
            Ok(()) => {}
            Err(errors) if errors == ["unknown capability schema"] => {
                return Err(PlanValidationError::UnknownSchema {
                    task: *task_id,
                    cap: node.capability.clone(),
                });
            }
            Err(errors) => {
                return Err(PlanValidationError::SchemaViolation {
                    task: *task_id,
                    cap: node.capability.clone(),
                    errors,
                });
            }
        }
    }

    // 6. Cost cap.
    if let Some(cap) = constraints.max_cost_usd {
        if response_cost_usd > cap {
            return Err(PlanValidationError::CostExceeded {
                estimated_usd: response_cost_usd,
                max_usd: cap,
            });
        }
    }

    Ok(())
}

fn well_formed(plan: &Plan, available: &[CapabilityRef]) -> Result<(), PlanValidationError> {
    if plan.tasks.is_empty() {
        return Err(PlanValidationError::Empty);
    }

    for &(from, to) in &plan.edges {
        if !plan.tasks.contains_key(&from) || !plan.tasks.contains_key(&to) {
            return Err(PlanValidationError::DanglingEdge { from, to });
        }
    }

    if has_cycle(plan) {
        return Err(PlanValidationError::Cycle);
    }

    let avail_ids: HashSet<&str> = available.iter().map(|c| c.id.as_str()).collect();
    for (task_id, node) in &plan.tasks {
        if !avail_ids.contains(node.capability.as_str()) {
            return Err(PlanValidationError::UnknownCapability {
                task: *task_id,
                cap: node.capability.clone(),
            });
        }
    }

    Ok(())
}

/// Kahn's algorithm — repeatedly remove zero-in-degree nodes; if any
/// remain after the sweep, there's a cycle.
///
/// Edge orientation: `(from, to)` means "`from` depends on `to`," so
/// `to` is a prerequisite of `from`. Per ADR-0002, that means in-degree
/// counts incoming "depends-on" edges — i.e. for each edge `(from, to)`
/// we increment `in_degree[from]`.
fn has_cycle(plan: &Plan) -> bool {
    let mut in_degree: HashMap<TaskId, usize> = plan.tasks.keys().map(|&k| (k, 0)).collect();
    for &(from, _to) in &plan.edges {
        if let Some(slot) = in_degree.get_mut(&from) {
            *slot += 1;
        }
    }

    let mut queue: Vec<TaskId> = in_degree
        .iter()
        .filter_map(|(k, &v)| if v == 0 { Some(*k) } else { None })
        .collect();
    let mut removed = 0usize;

    while let Some(node) = queue.pop() {
        removed += 1;
        // Every edge whose `to == node` reduces its `from`'s in-degree.
        for &(from, to) in &plan.edges {
            if to == node {
                if let Some(slot) = in_degree.get_mut(&from) {
                    if *slot > 0 {
                        *slot -= 1;
                        if *slot == 0 {
                            queue.push(from);
                        }
                    }
                }
            }
        }
    }

    removed != plan.tasks.len()
}
