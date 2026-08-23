//! Phase 3.9 — `validate_plan` integration tests (schema match + cost cap).

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp,
    clippy::many_single_char_names
)]

use std::collections::HashMap;

use harness_brain::{validate_plan, CapabilitySchemaIndex, PlanConstraints, PlanValidationError};
use harness_core::protocol::{CpuClass, DiskIoClass, NetworkClass, ResourceHints};
use harness_core::{CapabilityRef, NodeId, Plan, PlanId, PlanNode, Signature, TaskId};
use serde_json::json;

fn shell_only() -> Vec<CapabilityRef> {
    vec![CapabilityRef {
        id: "shell.exec".to_string(),
        version_major: 0,
    }]
}

fn shell_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["cmd"],
        "additionalProperties": false,
        "properties": {
            "cmd":  { "type": "string", "minLength": 1 },
            "args": { "type": "array", "items": { "type": "string" } },
        },
    })
}

fn shell_index() -> CapabilitySchemaIndex {
    CapabilitySchemaIndex::from_pairs(vec![("shell.exec".into(), shell_schema())])
}

/// 5.3: the common case — no cloud capabilities registered locally.
fn no_cloud() -> std::collections::HashSet<String> {
    std::collections::HashSet::new()
}

fn empty_hints() -> ResourceHints {
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

fn one_node_plan(cap: &str, input: serde_json::Value) -> Plan {
    let id = TaskId::new_v7();
    let mut tasks = HashMap::new();
    tasks.insert(
        id,
        PlanNode {
            id,
            capability: cap.into(),
            input,
            resource_hints: empty_hints(),
            timeout_ms: None,
        },
    );
    Plan {
        id: PlanId::new_v7(),
        name: "fixture".into(),
        tasks,
        edges: vec![],
        budget: None,
        checkpoint: None,
        issued_by: NodeId::from_bytes([0; 16]),
        sig: Signature::from_bytes([0u8; Signature::LEN]),
    }
}

#[test]
fn t06_schema_match_passes_for_shell_exec() {
    let p = one_node_plan("shell.exec", json!({"cmd": "ls", "args": ["-la"]}));
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(r.is_ok(), "got {r:?}");
}

#[test]
fn t07_schema_match_fails_when_cmd_is_integer() {
    let p = one_node_plan("shell.exec", json!({"cmd": 42}));
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    let Err(PlanValidationError::SchemaViolation { errors, cap, .. }) = r else {
        panic!("expected SchemaViolation; got {r:?}");
    };
    assert_eq!(cap, "shell.exec");
    assert!(
        !errors.is_empty(),
        "errors must be populated via iter_errors"
    );
}

#[test]
fn t08_cost_exceeded_fails() {
    let p = one_node_plan("shell.exec", json!({"cmd": "ls"}));
    let constraints = PlanConstraints {
        max_cost_usd: Some(0.50),
        ..PlanConstraints::default()
    };
    let r = validate_plan(
        &p,
        1.00,
        &constraints,
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    let Err(PlanValidationError::CostExceeded {
        estimated_usd,
        max_usd,
    }) = r
    else {
        panic!("expected CostExceeded; got {r:?}");
    };
    assert_eq!(estimated_usd, 1.00);
    assert_eq!(max_usd, 0.50);
}

#[test]
fn t08b_plan_carried_budget_below_estimate_is_inconsistent() {
    // 5.8 (ADR-0036, future-proofing — planner backends emit
    // budget: None today, pinned elsewhere): a plan promising a
    // budget below its own estimate is rejected at plan time.
    let mut p = one_node_plan("shell.exec", json!({"cmd": "ls"}));
    p.budget = Some(harness_core::protocol::Budget {
        max_cost_usd: Some(0.50),
        soft_limit_usd: None,
        on_exceed: harness_core::protocol::BudgetAction::Cancel,
    });
    let r = validate_plan(
        &p,
        1.00,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    let Err(PlanValidationError::BudgetInconsistent {
        estimated_usd,
        budget_usd,
    }) = r
    else {
        panic!("expected BudgetInconsistent; got {r:?}");
    };
    assert!((estimated_usd - 1.00).abs() < f64::EPSILON);
    assert!((budget_usd - 0.50).abs() < f64::EPSILON);

    // A budget AT the estimate passes (equality allowed, like rule 6),
    // and so does a waiver (max: None).
    p.budget = Some(harness_core::protocol::Budget {
        max_cost_usd: Some(1.00),
        soft_limit_usd: None,
        on_exceed: harness_core::protocol::BudgetAction::Cancel,
    });
    assert!(validate_plan(
        &p,
        1.00,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    )
    .is_ok());
}

#[test]
fn t09_cost_at_cap_passes() {
    // <= not < — equality at the cap is allowed.
    let p = one_node_plan("shell.exec", json!({"cmd": "ls"}));
    let constraints = PlanConstraints {
        max_cost_usd: Some(1.00),
        ..PlanConstraints::default()
    };
    let r = validate_plan(
        &p,
        1.00,
        &constraints,
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(r.is_ok(), "got {r:?}");
}

#[test]
fn t10_cost_none_skips_check() {
    let p = one_node_plan("shell.exec", json!({"cmd": "ls"}));
    let constraints = PlanConstraints {
        max_cost_usd: None,
        ..PlanConstraints::default()
    };
    // Even an absurd cost passes because the cap is unset.
    let r = validate_plan(
        &p,
        1_000_000.0,
        &constraints,
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(r.is_ok(), "got {r:?}");
}

#[test]
fn t11_unknown_capability_fails() {
    let p = one_node_plan("never.registered", json!({}));
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(matches!(
        r,
        Err(PlanValidationError::UnknownCapability { .. })
    ));
}

#[test]
fn t11b_unknown_schema_for_foreign_cap() {
    // Cap is in `available` but not in the schema index — input-override
    // path (foreign cap from a remote brain). 3.9 surfaces UnknownSchema
    // rather than rubber-stamping.
    let p = one_node_plan("foreign.cap", json!({"any": "shape"}));
    let foreign_available = vec![CapabilityRef {
        id: "foreign.cap".to_string(),
        version_major: 0,
    }];
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),     // shell.exec only; foreign.cap absent
        &foreign_available, // foreign.cap appears here so well-formed passes
        &no_cloud(),
    );
    let Err(PlanValidationError::UnknownSchema { cap, .. }) = r else {
        panic!("expected UnknownSchema; got {r:?}");
    };
    assert_eq!(cap, "foreign.cap");
}

#[test]
fn t31_well_formed_3_8_shape_still_passes() {
    // 3.8 callers invoking `validate_plan` with empty schemas + zero
    // cost should still get Ok for a well-formed plan whose caps are
    // all present in `available`. The migration is backward-compatible
    // for the structural-only contract.
    let p = one_node_plan("shell.exec", json!({"cmd": "ls"}));
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(r.is_ok(), "got {r:?}");
}

#[test]
#[allow(deprecated)]
fn t32_validate_plan_well_formed_still_callable() {
    // The deprecated symbol still works; downstream callers get a
    // compile warning (not a hard error), giving them a migration
    // window.
    let p = one_node_plan("shell.exec", json!({"cmd": "ls"}));
    let r = harness_brain::validate_plan_well_formed(&p, &shell_only());
    assert!(r.is_ok());
}

// --- 4.3 (ADR-0025): $task_output reference validation ---

fn two_node_plan_with_ref(edge: bool, ref_input: serde_json::Value) -> (Plan, TaskId, TaskId) {
    let a = TaskId::new_v7();
    let b = TaskId::new_v7();
    let mut tasks = HashMap::new();
    tasks.insert(
        a,
        PlanNode {
            id: a,
            capability: "shell.exec".into(),
            input: json!({"cmd": "ls"}),
            resource_hints: empty_hints(),
            timeout_ms: None,
        },
    );
    tasks.insert(
        b,
        PlanNode {
            id: b,
            capability: "shell.exec".into(),
            input: ref_input,
            resource_hints: empty_hints(),
            timeout_ms: None,
        },
    );
    let plan = Plan {
        id: PlanId::new_v7(),
        name: "ref".into(),
        tasks,
        edges: if edge { vec![(b, a)] } else { vec![] },
        budget: None,
        checkpoint: None,
        issued_by: NodeId::from_bytes([0; 16]),
        sig: Signature::from_bytes([0u8; Signature::LEN]),
    };
    (plan, a, b)
}

#[test]
fn t20_ref_to_declared_dependency_defers_schema_and_passes() {
    // b's input would FAIL the shell schema as-is (cmd is a ref object,
    // not a string) — the deferral is what lets it pass plan-time.
    let (p, a, _b) = two_node_plan_with_ref(
        true,
        json!({"cmd": {"$task_output": TaskId::new_v7().0.to_string()}}),
    );
    // Fix the ref to target the actual dependency.
    let (p2, a2, b2) = {
        let (mut p2, a2, b2) = two_node_plan_with_ref(true, json!({}));
        p2.tasks.get_mut(&b2).unwrap().input =
            json!({"cmd": {"$task_output": a2.0.to_string(), "pointer": "/stdout"}});
        (p2, a2, b2)
    };
    let _ = (p, a);
    let r = validate_plan(
        &p2,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(r.is_ok(), "{r:?} — edge ({b2:?},{a2:?}) declared");
}

#[test]
fn t21_ref_without_declared_edge_rejected() {
    let (mut p, a, b) = two_node_plan_with_ref(false, json!({}));
    p.tasks.get_mut(&b).unwrap().input = json!({"cmd": {"$task_output": a.0.to_string()}});
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(matches!(
        r,
        Err(PlanValidationError::UndeclaredOutputRef { task, referenced })
            if task == b && referenced == a
    ));
}

#[test]
fn t22_malformed_ref_rejected() {
    let (mut p, _a, b) = two_node_plan_with_ref(true, json!({}));
    p.tasks.get_mut(&b).unwrap().input = json!({"cmd": {"$task_output": "not-a-uuid"}});
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(matches!(
        r,
        Err(PlanValidationError::MalformedOutputRef { task, .. }) if task == b
    ));
}

#[test]
fn t23_node_id_mismatch_rejected() {
    let (mut p, a, _b) = two_node_plan_with_ref(true, json!({"cmd": "ls"}));
    p.tasks.get_mut(&a).unwrap().id = TaskId::new_v7();
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(matches!(
        r,
        Err(PlanValidationError::NodeIdMismatch { key, .. }) if key == a
    ));
}

#[test]
fn t24_ref_free_nodes_keep_full_schema_validation() {
    // The deferral must not weaken validation for literal inputs.
    let p = one_node_plan("shell.exec", json!({"bogus_field": true}));
    let r = validate_plan(
        &p,
        0.0,
        &PlanConstraints::default(),
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(matches!(
        r,
        Err(PlanValidationError::SchemaViolation { .. })
    ));
}

// ───────────────────────────────────────── 5.3 locality conflict (§15.4 rule 4)

#[test]
fn t33_must_be_local_with_cloud_cap_is_locality_conflict() {
    let mut cloud = no_cloud();
    cloud.insert("llm.cloud.claude".to_string());
    let avail = vec![CapabilityRef {
        id: "llm.cloud.claude".to_string(),
        version_major: 0,
    }];
    let p = one_node_plan("llm.cloud.claude", json!({}));
    let constraints = PlanConstraints {
        must_be_local: true,
        ..PlanConstraints::default()
    };
    // Schema deliberately unknown for the cloud cap: the locality rule
    // must fire FIRST (clearest repair diagnostic).
    let r = validate_plan(
        &p,
        0.0,
        &constraints,
        &CapabilitySchemaIndex::default(),
        &avail,
        &cloud,
    );
    assert!(
        matches!(r, Err(PlanValidationError::LocalityConflict { ref cap, .. }) if cap == "llm.cloud.claude"),
        "got {r:?}"
    );

    // Same plan without must_be_local → the rule does not fire (falls
    // through to UnknownSchema, proving locality was the only gate).
    let relaxed = PlanConstraints::default();
    let r = validate_plan(
        &p,
        0.0,
        &relaxed,
        &CapabilitySchemaIndex::default(),
        &avail,
        &cloud,
    );
    assert!(
        matches!(r, Err(PlanValidationError::UnknownSchema { .. })),
        "got {r:?}"
    );
}

#[test]
fn t34_must_be_local_without_cloud_caps_passes() {
    // must_be_local against purely local capabilities: fine. Foreign
    // caps (input-override path) are never in the local cloud set —
    // same shape as this test — and are deliberately not flagged
    // (§10.4 backstop, ADR-0032).
    let p = one_node_plan("shell.exec", json!({"cmd": "ls"}));
    let constraints = PlanConstraints {
        must_be_local: true,
        ..PlanConstraints::default()
    };
    let r = validate_plan(
        &p,
        0.0,
        &constraints,
        &shell_index(),
        &shell_only(),
        &no_cloud(),
    );
    assert!(r.is_ok(), "got {r:?}");
}
