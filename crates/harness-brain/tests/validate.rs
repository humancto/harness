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
    let r = validate_plan(&p, 1.00, &constraints, &shell_index(), &shell_only());
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
fn t09_cost_at_cap_passes() {
    // <= not < — equality at the cap is allowed.
    let p = one_node_plan("shell.exec", json!({"cmd": "ls"}));
    let constraints = PlanConstraints {
        max_cost_usd: Some(1.00),
        ..PlanConstraints::default()
    };
    let r = validate_plan(&p, 1.00, &constraints, &shell_index(), &shell_only());
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
    let r = validate_plan(&p, 1_000_000.0, &constraints, &shell_index(), &shell_only());
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
