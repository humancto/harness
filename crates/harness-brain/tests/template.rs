//! Phase 3.8 — `TemplateBackend` integration tests.
//!
//! Tests assert structural shape (lengths, capability ids, JSON shape),
//! never `UUIDv7` ids — those are non-deterministic and would be flaky.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp,
    clippy::many_single_char_names,
    deprecated
)]

use harness_brain::{
    backend::{PlanOutcome, PlanRequest, PlannerBackend},
    template::TemplateBackend,
    validate::validate_plan_well_formed,
    PlanConstraints, PlanValidationError, TEMPLATE_CONFIDENCE,
};
use harness_core::protocol::{CpuClass, DiskIoClass, NetworkClass, ResourceHints};
use harness_core::{CapabilityRef, NodeId, Plan, PlanId, PlanNode, Signature, TaskId};
use serde_json::json;
use std::collections::HashMap;

fn shell_only() -> Vec<CapabilityRef> {
    vec![CapabilityRef {
        id: "shell.exec".to_string(),
        version_major: 0,
    }]
}

fn req(goal: &str, available: Vec<CapabilityRef>) -> PlanRequest {
    PlanRequest {
        goal: goal.to_string(),
        available_capabilities: available,
        constraints: PlanConstraints::default(),
        context: None,
        issuing_node: NodeId::from_bytes([7; 16]),
    }
}

fn template() -> TemplateBackend {
    TemplateBackend::new(NodeId::from_bytes([1; 16]))
}

// ─────────────────────────────────────────── shell.exec emission

#[tokio::test]
async fn t01_template_run_prefix_emits_shell_exec_input() {
    let r = template()
        .plan(&req("run: ls -la", shell_only()))
        .await
        .expect("ok");
    let resp = match r {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    let plan = resp.plan.as_inner();
    assert_eq!(plan.tasks.len(), 1);
    let node = plan.tasks.values().next().expect("one node");
    assert_eq!(node.capability, "shell.exec");
    assert_eq!(node.input, json!({"cmd": "ls", "args": ["-la"]}));
}

#[tokio::test]
async fn t02_template_shell_prefix_alias() {
    let r = template()
        .plan(&req("shell: echo hi", shell_only()))
        .await
        .expect("ok");
    let resp = match r {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    let node = resp.plan.as_inner().tasks.values().next().unwrap();
    assert_eq!(node.input, json!({"cmd": "echo", "args": ["hi"]}));
}

#[tokio::test]
async fn t03_template_run_quoted_args() {
    let r = template()
        .plan(&req("run: echo \"hello world\"", shell_only()))
        .await
        .expect("ok");
    let resp = match r {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    let node = resp.plan.as_inner().tasks.values().next().unwrap();
    assert_eq!(node.input, json!({"cmd": "echo", "args": ["hello world"]}));
}

#[tokio::test]
async fn t04_template_run_empty_command_is_no_match() {
    let r = template()
        .plan(&req("run:", shell_only()))
        .await
        .expect("ok");
    assert!(matches!(r, PlanOutcome::NoMatch), "got {r:?}");
}

#[tokio::test]
async fn t04b_template_run_unclosed_quote_is_no_match() {
    let r = template()
        .plan(&req("run: echo \"oops", shell_only()))
        .await
        .expect("ok");
    assert!(matches!(r, PlanOutcome::NoMatch), "got {r:?}");
}

#[tokio::test]
async fn t04c_template_run_only_whitespace_is_no_match() {
    let r = template()
        .plan(&req("run:    ", shell_only()))
        .await
        .expect("ok");
    assert!(matches!(r, PlanOutcome::NoMatch), "got {r:?}");
}

#[tokio::test]
async fn t05_template_unknown_prefix_is_no_match() {
    let r = template()
        .plan(&req("summarize PDFs in folder", shell_only()))
        .await
        .expect("ok");
    assert!(matches!(r, PlanOutcome::NoMatch), "got {r:?}");
}

#[tokio::test]
async fn t06_template_unsupported_capability_emits_diagnostic() {
    // `fetch:` matches but `http.fetch` is not in available.
    let r = template()
        .plan(&req("fetch: https://example.com", shell_only()))
        .await
        .expect("ok");
    match r {
        PlanOutcome::MatchedButUnsupported {
            matched_pattern,
            missing_capability,
        } => {
            assert_eq!(matched_pattern, "fetch:");
            assert_eq!(missing_capability, "http.fetch");
        }
        other => panic!("want MatchedButUnsupported, got {other:?}"),
    }
}

#[tokio::test]
async fn t06b_template_empty_available_capabilities_emits_diagnostic() {
    let r = template().plan(&req("run: ls", vec![])).await.expect("ok");
    match r {
        PlanOutcome::MatchedButUnsupported {
            matched_pattern,
            missing_capability,
        } => {
            assert_eq!(matched_pattern, "run:");
            assert_eq!(missing_capability, "shell.exec");
        }
        other => panic!("want MatchedButUnsupported, got {other:?}"),
    }
}

#[tokio::test]
async fn t07_template_confidence_is_template_constant() {
    let r = template()
        .plan(&req("run: ls", shell_only()))
        .await
        .expect("ok");
    let resp = match r {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    assert!(
        (resp.confidence - TEMPLATE_CONFIDENCE).abs() < f64::EPSILON,
        "expected TEMPLATE_CONFIDENCE ({TEMPLATE_CONFIDENCE}); got {}",
        resp.confidence
    );
}

#[tokio::test]
async fn t08_template_cost_is_zero() {
    let r = template()
        .plan(&req("run: ls", shell_only()))
        .await
        .expect("ok");
    let resp = match r {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    assert_eq!(resp.estimated_cost_usd, 0.0);
    assert_eq!(resp.estimated_duration_ms, 0);
}

#[tokio::test]
async fn t09_template_emits_unsigned_plan_with_zero_sig() {
    let r = template()
        .plan(&req("run: ls", shell_only()))
        .await
        .expect("ok");
    let resp = match r {
        PlanOutcome::Confident(p) => p,
        other => panic!("want Confident, got {other:?}"),
    };
    // Inner sig is the zero signature — the wrapper enforces "must be
    // signed before treated as authentic," but at planner-emit time
    // that signing has not happened yet.
    assert_eq!(
        resp.plan.as_inner().sig,
        Signature::from_bytes([0u8; Signature::LEN])
    );
    assert!(resp.fallback_plan.is_none());
}

#[tokio::test]
async fn t09b_template_pattern_matching_is_case_sensitive() {
    let r = template()
        .plan(&req("Run: ls", shell_only()))
        .await
        .expect("ok");
    assert!(matches!(r, PlanOutcome::NoMatch), "got {r:?}");
}

#[tokio::test]
async fn t15_template_never_returns_err() {
    // Battery of weird goals — Template is pure pattern matching, so
    // it must never return Err. NoMatch / MatchedButUnsupported /
    // Confident are all fine.
    let long_goal = "very long ".repeat(2000);
    let inputs = [
        "",
        "   ",
        "💀💀💀",
        long_goal.as_str(),
        "run:",
        "run: ",
        "run: \"",
        "run: rm -rf /",
        "run: $(whoami)",
        "run: `pwd`",
        "RUN: ls",
        "fetch: ftp://example.com",
        "search: anything",
        "summarize: /tmp",
        "schedule: */5 * * * *",
        "shell: command | pipe",
        "shell: cmd && other",
        "shell: cmd ; other",
        "newlines\nin\ngoal: run: ls",
        "run: echo \"\\n\"",
    ];
    let t = template();
    for goal in inputs {
        let r = t.plan(&req(goal, shell_only())).await;
        assert!(r.is_ok(), "Template returned Err on goal {goal:?}: {r:?}");
    }
}

// ─────────────────────────────────────────── validate_plan_well_formed

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

fn node(id: TaskId, cap: &str) -> PlanNode {
    PlanNode {
        id,
        capability: cap.into(),
        input: json!({}),
        resource_hints: empty_hints(),
        timeout_ms: None,
    }
}

fn plan_with(tasks: Vec<(TaskId, &str)>, edges: Vec<(TaskId, TaskId)>) -> Plan {
    let mut t = HashMap::new();
    for (id, cap) in tasks {
        t.insert(id, node(id, cap));
    }
    Plan {
        id: PlanId::new_v7(),
        name: "test".into(),
        tasks: t,
        edges,
        budget: None,
        checkpoint: None,
        issued_by: NodeId::from_bytes([0; 16]),
        sig: Signature::from_bytes([0u8; Signature::LEN]),
    }
}

#[test]
fn t10_validate_empty_tasks_fails() {
    let p = Plan {
        id: PlanId::new_v7(),
        name: "empty".into(),
        tasks: HashMap::new(),
        edges: vec![],
        budget: None,
        checkpoint: None,
        issued_by: NodeId::from_bytes([0; 16]),
        sig: Signature::from_bytes([0u8; Signature::LEN]),
    };
    assert!(matches!(
        validate_plan_well_formed(&p, &shell_only()),
        Err(PlanValidationError::Empty)
    ));
}

#[test]
fn t11_validate_dangling_edge_fails() {
    let a = TaskId::new_v7();
    let dangling = TaskId::new_v7();
    let p = plan_with(vec![(a, "shell.exec")], vec![(a, dangling)]);
    assert!(matches!(
        validate_plan_well_formed(&p, &shell_only()),
        Err(PlanValidationError::DanglingEdge { .. })
    ));
}

#[test]
fn t12_validate_unknown_capability_fails() {
    let a = TaskId::new_v7();
    let p = plan_with(vec![(a, "unregistered.cap")], vec![]);
    assert!(matches!(
        validate_plan_well_formed(&p, &shell_only()),
        Err(PlanValidationError::UnknownCapability { .. })
    ));
}

#[test]
fn t13_validate_cycle_fails() {
    // a → b, b → c, c → a. Each `(from, to)` means "from depends on to".
    let a = TaskId::new_v7();
    let b = TaskId::new_v7();
    let c = TaskId::new_v7();
    let p = plan_with(
        vec![(a, "shell.exec"), (b, "shell.exec"), (c, "shell.exec")],
        vec![(a, b), (b, c), (c, a)],
    );
    assert!(matches!(
        validate_plan_well_formed(&p, &shell_only()),
        Err(PlanValidationError::Cycle)
    ));
}

#[test]
fn t14_validate_diamond_passes() {
    // a → b, a → c, b → d, c → d (b and c depend on a; d depends on
    // both b and c). Acyclic.
    let a = TaskId::new_v7();
    let b = TaskId::new_v7();
    let c = TaskId::new_v7();
    let d = TaskId::new_v7();
    let p = plan_with(
        vec![
            (a, "shell.exec"),
            (b, "shell.exec"),
            (c, "shell.exec"),
            (d, "shell.exec"),
        ],
        // Edges encode "X depends on Y" — root `a` has no
        // dependencies; `b` and `c` depend on `a`; `d` depends on both.
        vec![(b, a), (c, a), (d, b), (d, c)],
    );
    assert!(
        validate_plan_well_formed(&p, &shell_only()).is_ok(),
        "diamond DAG must validate"
    );
}
