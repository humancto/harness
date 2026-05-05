//! Plan well-formedness checks (Phase 3.8 scope).
//!
//! 3.8 ships:
//! - non-empty `tasks`
//! - every edge endpoint references an existing task id
//! - DAG is acyclic (Kahn's algorithm)
//! - every node's `capability` exists in `available_capabilities` by id
//!
//! 3.9 layers on:
//! - JSON-Schema validation of `PlanNode.input` against
//!   `Capability::input_schema`
//! - `must_be_local` / `cloud_ok` consistency
//! - estimated cost ≤ `constraints.max_cost_usd`
//!
//! `version_major` is intentionally NOT checked in 3.8 because
//! `PlanNode` has no `capability_version_major` field today (ADR-0013 §10).

use std::collections::{HashMap, HashSet};

use harness_core::{CapabilityRef, Plan, TaskId};

use crate::error::PlanValidationError;

/// Run the well-formedness checks against `plan`. Returns `Ok(())` if
/// every check passes; the first failure short-circuits.
pub fn validate_plan_well_formed(
    plan: &Plan,
    available: &[CapabilityRef],
) -> Result<(), PlanValidationError> {
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
