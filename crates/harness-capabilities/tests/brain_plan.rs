//! Phase 3.8 + 3.9 — `brain.plan` capability integration tests.

#![cfg(feature = "brain")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::float_cmp,
    clippy::unnecessary_literal_bound
)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use harness_brain::backend::{PlanOutcome, PlanRequest, PlanResponse, PlannerBackend, Unsigned};
use harness_brain::error::PlannerError;
use harness_brain::{CapabilitySchemaIndex, PlanConstraints, TemplateBackend};
use harness_capabilities::brain_plan::{
    enrich_with_brain_plan, BrainPlanCapability, BrainPlanConfig, ID,
};
use harness_capabilities::traits::{Capability, ExecutionContext};
use harness_capabilities::{CapabilityRegistry, CapabilitySnapshot};
use harness_core::protocol::{CpuClass, DiskIoClass, NetworkClass, ResourceHints};
use harness_core::{CapabilityRef, NodeId, Plan, PlanId, PlanNode, Signature, TaskId};
use serde_json::json;
use std::collections::HashMap;

// ───────────────────────────────────────── Fixtures

fn shell_ref() -> CapabilityRef {
    CapabilityRef {
        id: "shell.exec".to_string(),
        version_major: 0,
    }
}

fn shell_only() -> Vec<CapabilityRef> {
    vec![shell_ref()]
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

fn shell_schema_index() -> CapabilitySchemaIndex {
    CapabilitySchemaIndex::from_pairs(vec![("shell.exec".into(), shell_schema())])
}

fn snapshot_with_shell() -> CapabilitySnapshot {
    CapabilitySnapshot {
        refs: shell_only(),
        schemas: shell_schema_index(),
        cloud_caps: std::collections::HashSet::new(),
    }
}

fn empty_snapshot() -> CapabilitySnapshot {
    CapabilitySnapshot {
        refs: vec![],
        schemas: CapabilitySchemaIndex::default(),
        cloud_caps: std::collections::HashSet::new(),
    }
}

fn ctx() -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
        tags: Arc::from(Vec::<String>::new()),
        frame_sink: None,
        audit: None,
    }
}

fn cap_with_template_and(snapshot: CapabilitySnapshot) -> BrainPlanCapability {
    let template = Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> =
        Arc::new(move || snapshot.clone());
    BrainPlanCapability::new(vec![template], provider, PlanConstraints::default())
}

// ───────────────────────────────────────── 3.8 carry-forward tests

#[test]
fn t16_brain_plan_capability_id_is_brain_plan() {
    let cap = cap_with_template_and(snapshot_with_shell());
    assert_eq!(ID, "brain.plan");
    assert_eq!(cap.id(), "brain.plan");
}

#[test]
fn t17_brain_plan_manifest_advertises_anyone_local_fast_no_secrets() {
    let cap = cap_with_template_and(snapshot_with_shell());
    let m = cap.manifest();
    assert_eq!(m.id, "brain.plan");
    assert!(matches!(m.cardinality, harness_core::Cardinality::Anyone));
    assert!(matches!(
        m.cost_hint,
        harness_core::protocol::CostHint::LocalFast
    ));
    assert!(m.requires_secrets.is_empty());
    assert!(m.tags.iter().any(|t| t == "brain"));
    assert!(m.tags.iter().any(|t| t == "planner"));
}

#[tokio::test]
async fn t18_brain_plan_invalid_input_returns_invalid_input() {
    let cap = cap_with_template_and(snapshot_with_shell());
    let err = cap
        .execute(&ctx(), json!({}))
        .await
        .expect_err("must reject missing goal");
    assert!(matches!(
        err,
        harness_capabilities::traits::CapabilityError::InvalidInput(_)
    ));
    let err = cap
        .execute(&ctx(), json!({"goal": "   "}))
        .await
        .expect_err("must reject empty goal");
    assert!(matches!(
        err,
        harness_capabilities::traits::CapabilityError::InvalidInput(_)
    ));
}

#[tokio::test]
async fn t19_brain_plan_template_match_returns_serialized_plan() {
    let cap = cap_with_template_and(snapshot_with_shell());
    let out = cap
        .execute(&ctx(), json!({"goal": "run: ls"}))
        .await
        .expect("happy path");
    let plan = out.get("plan").expect("plan field");
    let tasks = plan.get("tasks").expect("tasks").as_object().expect("obj");
    assert_eq!(tasks.len(), 1);
    let node = tasks.values().next().unwrap();
    assert_eq!(node["capability"], json!("shell.exec"));
    assert_eq!(node["input"]["cmd"], json!("ls"));
    let confidence = out
        .get("confidence")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((confidence - 0.6).abs() < 1e-9);
}

#[tokio::test]
async fn t20_brain_plan_uses_input_available_capabilities_when_provided() {
    // Provider returns empty — local registry has no shell.exec. The
    // input override supplies the cap; schemas come from the local
    // registry which is empty, so well-formedness passes (cap is in
    // available) but UnknownSchema fires for the foreign cap.
    let cap = cap_with_template_and(empty_snapshot());
    let err = cap
        .execute(
            &ctx(),
            json!({
                "goal": "run: ls",
                "available_capabilities": [
                    {"id": "shell.exec", "version_major": 0}
                ]
            }),
        )
        .await
        .expect_err("input-override path must surface UnknownSchema");
    let msg = format!("{err}");
    assert!(
        msg.contains("UnknownSchema") || msg.contains("not compiled"),
        "expected UnknownSchema diagnostic; got {msg}"
    );
}

#[tokio::test]
async fn t20b_brain_plan_input_override_with_locally_registered_cap_validates() {
    // Provider returns a snapshot WITH shell.exec + schema. Input
    // override re-states shell.exec; validation succeeds (the
    // intersection is the locally-registered cap).
    let cap = cap_with_template_and(snapshot_with_shell());
    let out = cap
        .execute(
            &ctx(),
            json!({
                "goal": "run: ls",
                "available_capabilities": [
                    {"id": "shell.exec", "version_major": 0}
                ]
            }),
        )
        .await
        .expect("locally-registered override path must succeed");
    assert_eq!(
        out["plan"]["tasks"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()["capability"],
        json!("shell.exec")
    );
}

// ───────────────────────────────────────── Snapshot consistency (B4)

/// A backend that records each call and returns whatever variant the
/// test set up. Used by t21 to verify that two backends share the same
/// `available_capabilities` snapshot inside one execute call.
#[derive(Debug)]
struct SnapshotProbe {
    id: &'static str,
    seen: Arc<parking_lot::Mutex<Vec<Vec<CapabilityRef>>>>,
    outcome: PlanOutcomeKind,
}

#[derive(Debug, Clone, Copy)]
enum PlanOutcomeKind {
    NoMatch,
}

#[async_trait]
impl PlannerBackend for SnapshotProbe {
    fn id(&self) -> &str {
        self.id
    }

    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        self.seen.lock().push(req.available_capabilities.clone());
        match self.outcome {
            PlanOutcomeKind::NoMatch => Ok(PlanOutcome::NoMatch),
        }
    }
}

#[tokio::test]
async fn t21_brain_plan_snapshot_consistent_across_backends() {
    let seen_a = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen_b = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let backend_a: Arc<dyn PlannerBackend> = Arc::new(SnapshotProbe {
        id: "probe_a",
        seen: seen_a.clone(),
        outcome: PlanOutcomeKind::NoMatch,
    });
    let backend_b: Arc<dyn PlannerBackend> = Arc::new(SnapshotProbe {
        id: "probe_b",
        seen: seen_b.clone(),
        outcome: PlanOutcomeKind::NoMatch,
    });

    // Provider returns a shared mutex-backed Vec; the test mutates it
    // between the two backend calls. If the cap snapshotted ONCE,
    // backend B sees the same list backend A saw.
    let live_caps: Arc<parking_lot::Mutex<Vec<CapabilityRef>>> =
        Arc::new(parking_lot::Mutex::new(shell_only()));
    let counter = Arc::new(AtomicUsize::new(0));
    let live_for_provider = live_caps.clone();
    let counter_for_provider = counter.clone();
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(move || {
        let v = live_for_provider.lock().clone();
        if counter_for_provider.fetch_add(1, Ordering::SeqCst) == 0 {
            live_for_provider.lock().clear();
        }
        CapabilitySnapshot {
            refs: v,
            schemas: shell_schema_index(),
            cloud_caps: std::collections::HashSet::new(),
        }
    });
    let cap = BrainPlanCapability::new(
        vec![backend_a, backend_b],
        provider,
        PlanConstraints::default(),
    );

    let _ = cap.execute(&ctx(), json!({"goal": "run: ls"})).await;
    let a = seen_a.lock().clone();
    let b = seen_b.lock().clone();
    assert_eq!(a.len(), 1, "backend a invoked once");
    assert_eq!(b.len(), 1, "backend b invoked once");
    assert_eq!(a[0], b[0], "snapshot must be identical across backends");
    assert_eq!(a[0], shell_only(), "snapshot is the pre-mutation list");
}

// ───────────────────────────────────────── Final-failure diagnostics + idempotent enrich

#[tokio::test]
async fn t22_brain_plan_no_backend_match_returns_failed() {
    let cap = cap_with_template_and(snapshot_with_shell());
    let err = cap
        .execute(&ctx(), json!({"goal": "complicated multi-step request"}))
        .await
        .expect_err("must fail");
    assert!(format!("{err}").contains("no backend produced"));

    // MatchedButUnsupported propagates the missing cap into diagnostic.
    let err = cap
        .execute(&ctx(), json!({"goal": "fetch: https://x"}))
        .await
        .expect_err("must fail with diagnostic");
    let msg = format!("{err}");
    assert!(
        msg.contains("http.fetch"),
        "diagnostic must name the missing cap; got {msg}"
    );
}

#[tokio::test]
async fn t23_brain_plan_idempotent_enrich_panics() {
    let registry = CapabilityRegistry::new();
    let backends: Vec<Arc<dyn PlannerBackend>> =
        vec![Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])))];
    enrich_with_brain_plan(&registry, backends.clone(), BrainPlanConfig::default()).await;
    let registry2 = registry.clone();
    let backends2 = backends;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            enrich_with_brain_plan(&registry2, backends2, BrainPlanConfig::default()).await;
        });
    }));
    assert!(result.is_err(), "second enrich must panic");
}

// ───────────────────────────────────────── 3.9 backend-lineup behavior

/// Backend that emits a confident plan referencing a capability that
/// the executor's `available` list does NOT contain. The executor
/// must reject via `validate_plan` and surface a diagnostic.
#[derive(Debug)]
struct EmitsUnknownCap;

#[async_trait]
impl PlannerBackend for EmitsUnknownCap {
    fn id(&self) -> &str {
        "emits-unknown"
    }

    async fn plan(&self, _req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        let plan = plan_one_node("never.registered", json!({}));
        Ok(PlanOutcome::Confident(Box::new(PlanResponse {
            plan: Unsigned(plan),
            confidence: 0.95,
            rationale: "test".into(),
            estimated_cost_usd: 0.0,
            estimated_duration_ms: 0,
            fallback_plan: None,
        })))
    }
}

fn plan_one_node(cap: &str, input: serde_json::Value) -> Plan {
    let id = TaskId::new_v7();
    let mut tasks = HashMap::new();
    tasks.insert(
        id,
        PlanNode {
            id,
            capability: cap.into(),
            input,
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: NetworkClass::None,
                disk_io_class: DiskIoClass::None,
                estimated_duration_ms: None,
            },
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

/// Spy backend that counts calls and returns a fixed outcome.
#[derive(Debug)]
struct SpyBackend {
    id: String,
    calls: Arc<AtomicUsize>,
    outcome: SpyOutcome,
}

#[derive(Clone)]
enum SpyOutcome {
    Confident(Box<PlanResponse>),
    NoMatch,
}

impl std::fmt::Debug for SpyOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Confident(_) => f.write_str("Confident(<elided>)"),
            Self::NoMatch => f.write_str("NoMatch"),
        }
    }
}

#[async_trait]
impl PlannerBackend for SpyBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn plan(&self, _req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.outcome {
            SpyOutcome::Confident(b) => Ok(PlanOutcome::Confident(b.clone())),
            SpyOutcome::NoMatch => Ok(PlanOutcome::NoMatch),
        }
    }
}

fn good_plan_response(cap_id: &str, confidence: f64) -> PlanResponse {
    PlanResponse {
        plan: Unsigned(plan_one_node(cap_id, json!({"cmd": "ls"}))),
        confidence,
        rationale: "spy".into(),
        estimated_cost_usd: 0.0,
        estimated_duration_ms: 0,
        fallback_plan: None,
    }
}

#[tokio::test]
async fn t23a_localfast_first_template_skipped_when_localfast_confident() {
    let lf_calls = Arc::new(AtomicUsize::new(0));
    let tpl_calls = Arc::new(AtomicUsize::new(0));
    let lf: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "localfast:llama3.1:8b".into(),
        calls: lf_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
    });
    let tpl: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "template".into(),
        calls: tpl_calls.clone(),
        outcome: SpyOutcome::NoMatch,
    });
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(vec![lf, tpl], provider, PlanConstraints::default());

    let _ = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect("LocalFast confident → return");
    assert_eq!(lf_calls.load(Ordering::SeqCst), 1, "LocalFast invoked once");
    assert_eq!(
        tpl_calls.load(Ordering::SeqCst),
        0,
        "Template never invoked when LocalFast is Confident"
    );
}

#[tokio::test]
async fn t23b_localfast_nomatch_falls_through_to_template() {
    let lf_calls = Arc::new(AtomicUsize::new(0));
    let tpl_calls = Arc::new(AtomicUsize::new(0));
    let lf: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "localfast:llama3.1:8b".into(),
        calls: lf_calls.clone(),
        outcome: SpyOutcome::NoMatch,
    });
    let tpl: Arc<dyn PlannerBackend> = Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let tpl_id = tpl.id().to_string();
    let _ = tpl_id;
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(
        vec![
            lf,
            tpl,
            Arc::new(SpyBackend {
                id: "tail-spy".into(),
                calls: tpl_calls.clone(),
                outcome: SpyOutcome::NoMatch,
            }),
        ],
        provider,
        PlanConstraints::default(),
    );

    let _ = cap
        .execute(&ctx(), json!({"goal": "run: ls"}))
        .await
        .expect("Template covers");
    assert_eq!(lf_calls.load(Ordering::SeqCst), 1);
    // tail-spy is not reached because Template returned Confident
    // before it. Spies confirm escalation order.
    assert_eq!(tpl_calls.load(Ordering::SeqCst), 0);
}

/// Backend that emits a confident plan whose `PlanNode.input` violates
/// the registered cap's schema (cmd = integer instead of string). The
/// executor must call `validate_plan`, get back `SchemaViolation`, and
/// surface a diagnostic — not silently rubber-stamp the bad plan.
#[derive(Debug)]
struct SchemaViolatingBackend;

#[async_trait]
impl PlannerBackend for SchemaViolatingBackend {
    fn id(&self) -> &str {
        "violator"
    }

    async fn plan(&self, _req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        // shell.exec schema requires `cmd: string` — emit `cmd: 42` so
        // schema validation rejects.
        let plan = plan_one_node("shell.exec", json!({"cmd": 42}));
        Ok(PlanOutcome::Confident(Box::new(PlanResponse {
            plan: Unsigned(plan),
            confidence: 0.95,
            rationale: "violator".into(),
            estimated_cost_usd: 0.0,
            estimated_duration_ms: 0,
            fallback_plan: None,
        })))
    }
}

#[tokio::test]
async fn t24b_schema_violation_propagates_through_executor() {
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(
        vec![Arc::new(SchemaViolatingBackend)],
        provider,
        PlanConstraints::default(),
    );
    let err = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect_err("must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("SchemaViolation") || msg.contains("schema"),
        "diagnostic must name SchemaViolation; got {msg}"
    );
    assert!(
        msg.contains("shell.exec"),
        "diagnostic must name the failing capability; got {msg}"
    );
}

#[tokio::test]
async fn t24_brain_plan_validation_failure_propagates_to_diagnostic() {
    // EmitsUnknownCap returns a Confident plan referencing a cap not in
    // available. validate_plan returns UnknownCapability; the executor
    // surfaces the diagnostic and the call fails (Template is also in
    // the lineup but matches no prefix for "anything").
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(
        vec![Arc::new(EmitsUnknownCap)],
        provider,
        PlanConstraints::default(),
    );
    let err = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect_err("must fail");
    let msg = format!("{err}");
    assert!(msg.contains("validation failed"), "msg = {msg}");
}

/// 5.1 (ADR-0030): the three-tier local_first lineup — a
/// low-confidence LocalFast escalates to a confident LocalStrong;
/// Template never runs.
#[tokio::test]
async fn t29_low_confidence_fast_escalates_to_confident_strong() {
    let fast_calls = Arc::new(AtomicUsize::new(0));
    let strong_calls = Arc::new(AtomicUsize::new(0));
    let tpl_calls = Arc::new(AtomicUsize::new(0));
    let fast: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "localfast:llama3.1:8b".into(),
        calls: fast_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.2))),
    });
    let strong: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "localstrong:llama3.1:70b".into(),
        calls: strong_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.95))),
    });
    let tpl: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "template".into(),
        calls: tpl_calls.clone(),
        outcome: SpyOutcome::NoMatch,
    });
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let constraints = PlanConstraints {
        confidence_threshold: Some(0.7),
        ..PlanConstraints::default()
    };
    let cap = BrainPlanCapability::new(vec![fast, strong, tpl], provider, constraints);

    let out = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect("strong tier serves the plan");
    // The output carries no backend id — the winning tier is proven
    // by its distinctive confidence.
    assert_eq!(out["confidence"], 0.95);
    assert_eq!(fast_calls.load(Ordering::SeqCst), 1);
    assert_eq!(strong_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        tpl_calls.load(Ordering::SeqCst),
        0,
        "Template never invoked when a stronger tier is confident"
    );
}

#[tokio::test]
async fn t25_low_confidence_backend_escalated_past() {
    let lf_calls = Arc::new(AtomicUsize::new(0));
    let lf: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "localfast:llama3.1:8b".into(),
        calls: lf_calls.clone(),
        // Below the 0.7 threshold supplied via input.
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.5))),
    });
    let tpl: Arc<dyn PlannerBackend> = Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(vec![lf, tpl], provider, PlanConstraints::default());

    // Threshold 0.55 — LocalFast's 0.5 fails, Template's 0.6 passes.
    let out = cap
        .execute(
            &ctx(),
            json!({
                "goal": "run: ls",
                "constraints": { "confidence_threshold": 0.55 }
            }),
        )
        .await
        .expect("Template covers after LocalFast escalates");
    let confidence = out
        .get("confidence")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((confidence - 0.6).abs() < 1e-9, "Template's 0.6 wins");
    assert_eq!(lf_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn t26_cost_cap_rejects_expensive_plan() {
    let lf_resp = good_plan_response("shell.exec", 0.95);
    let lf: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "localfast:expensive".into(),
        calls: Arc::new(AtomicUsize::new(0)),
        outcome: SpyOutcome::Confident(Box::new(PlanResponse {
            estimated_cost_usd: 5.00,
            ..lf_resp
        })),
    });
    let tpl: Arc<dyn PlannerBackend> = Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(vec![lf, tpl], provider, PlanConstraints::default());

    let out = cap
        .execute(
            &ctx(),
            json!({
                "goal": "run: ls",
                "constraints": { "max_cost_usd": 1.00 }
            }),
        )
        .await
        .expect("Template covers after LocalFast cost-exceeds");
    let cost = out
        .get("estimated_cost_usd")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert_eq!(cost, 0.0, "Template's free plan wins");
}

#[tokio::test]
async fn t28_default_constraints_flow_when_input_omits() {
    // BrainPlanConfig sets default confidence_threshold = 0.95.
    // Template returns 0.6, which is below default → escalation.
    // Lineup is just Template, so the call fails.
    let tpl: Arc<dyn PlannerBackend> = Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(
        vec![tpl],
        provider,
        PlanConstraints {
            confidence_threshold: Some(0.95),
            ..PlanConstraints::default()
        },
    );

    let err = cap
        .execute(&ctx(), json!({"goal": "run: ls"}))
        .await
        .expect_err("Template's 0.6 < default 0.95 → escalation diagnostic");
    let msg = format!("{err}");
    assert!(
        msg.contains("confidence") && msg.contains("0.60") && msg.contains("0.95"),
        "diagnostic must spell out the threshold mismatch; got {msg}"
    );
}

#[tokio::test]
async fn t30_no_localfast_no_ollama_template_works() {
    // Daemon path with `local_fast_model = None`: only Template
    // registered; `run: ls` succeeds.
    let registry = CapabilityRegistry::new();
    let template: Arc<dyn PlannerBackend> =
        Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    enrich_with_brain_plan(&registry, vec![template], BrainPlanConfig::default()).await;
    // Also register a shell.exec stub so brain.plan can observe it.
    register_shell_stub(&registry);

    let bp = registry.get("brain.plan").expect("registered");
    let out = bp
        .execute(&ctx(), json!({"goal": "run: ls"}))
        .await
        .expect("Template covers");
    assert_eq!(
        out["plan"]["tasks"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()["capability"],
        json!("shell.exec")
    );
}

/// Helper: register a no-op `shell.exec` cap so `brain.plan` sees it
/// in `WeakCapabilityRegistry::snapshot()`.
fn register_shell_stub(registry: &CapabilityRegistry) {
    use harness_core::Cardinality;
    use harness_core::SemVer;

    #[derive(Debug)]
    struct ShellStub;

    #[async_trait]
    impl Capability for ShellStub {
        fn id(&self) -> &str {
            "shell.exec"
        }
        fn manifest(&self) -> harness_core::Capability {
            harness_core::Capability {
                id: "shell.exec".into(),
                version: SemVer {
                    major: 0,
                    minor: 1,
                    patch: 0,
                },
                cardinality: Cardinality::Anyone,
                input_schema: shell_schema(),
                output_schema: json!({"type": "object"}),
                cost_hint: harness_core::protocol::CostHint::LocalFast,
                tags: vec![],
                rate_limit: None,
                resource_hints: ResourceHints {
                    cpu_class: CpuClass::Light,
                    memory_mb: None,
                    gpu_required: false,
                    gpu_memory_mb: None,
                    network_class: NetworkClass::None,
                    disk_io_class: DiskIoClass::None,
                    estimated_duration_ms: None,
                },
                requires_secrets: vec![],
            }
        }
        async fn execute(
            &self,
            _ctx: &ExecutionContext,
            input: serde_json::Value,
        ) -> Result<serde_json::Value, harness_capabilities::traits::CapabilityError> {
            Ok(input)
        }
    }

    registry.register(Arc::new(ShellStub)).expect("register");
}

#[tokio::test]
async fn t27_brain_plan_capability_dropped_when_registry_dropped() {
    // 3.8 carry-forward: registry drop → cap drop (no leak from Weak
    // provider). The Weak<RwLock<...>> in the snapshot closure does
    // NOT extend the registry's lifetime.
    let weak_cap: std::sync::Weak<dyn Capability> = {
        let registry = CapabilityRegistry::new();
        let template: Arc<dyn PlannerBackend> =
            Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
        enrich_with_brain_plan(&registry, vec![template], BrainPlanConfig::default()).await;
        let strong = registry.get("brain.plan").expect("registered");
        Arc::downgrade(&strong)
    };
    assert!(
        weak_cap.upgrade().is_none(),
        "brain.plan capability must drop with its registry"
    );
}

// ───────────────────────────────────────── 5.2 policy-driven gating

/// Spy that records the constraints it was invoked with, then defers.
#[derive(Debug)]
struct ConstraintRecorder {
    seen: Arc<std::sync::Mutex<Option<PlanConstraints>>>,
}

#[async_trait]
impl PlannerBackend for ConstraintRecorder {
    fn id(&self) -> &str {
        "recorder"
    }
    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        *self.seen.lock().expect("lock") = Some(req.constraints);
        Ok(PlanOutcome::NoMatch)
    }
}

fn recording_cap(
    default_constraints: PlanConstraints,
    local_only_tags: &[&str],
) -> (
    BrainPlanCapability,
    Arc<std::sync::Mutex<Option<PlanConstraints>>>,
) {
    let seen = Arc::new(std::sync::Mutex::new(None));
    let recorder: Arc<dyn PlannerBackend> = Arc::new(ConstraintRecorder {
        seen: Arc::clone(&seen),
    });
    let template: Arc<dyn PlannerBackend> =
        Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(vec![recorder, template], provider, default_constraints)
        .with_local_only_tags(local_only_tags.iter().map(|s| (*s).to_string()).collect());
    (cap, seen)
}

fn ctx_with_tags(tags: &[&str]) -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
        tags: Arc::from(tags.iter().map(|s| (*s).to_string()).collect::<Vec<_>>()),
        frame_sink: None,
        audit: None,
    }
}

#[tokio::test]
async fn t31_local_only_for_tags_forces_must_be_local() {
    let (cap, seen) = recording_cap(PlanConstraints::default(), &["medical", "legal"]);

    // Tagged task → must_be_local forced true.
    let _ = cap
        .execute(&ctx_with_tags(&["medical"]), json!({"goal": "run: ls"}))
        .await
        .expect("template still covers");
    let c = seen.lock().expect("lock").take().expect("recorded");
    assert!(c.must_be_local, "medical tag must force local planning");

    // Untagged task → default (false) preserved.
    let _ = cap
        .execute(&ctx_with_tags(&[]), json!({"goal": "run: ls"}))
        .await
        .expect("ok");
    let c = seen.lock().expect("lock").take().expect("recorded");
    assert!(!c.must_be_local, "no tag → no forcing");
}

#[tokio::test]
async fn t32_cloud_needs_policy_approval_and_per_task_opt_in() {
    // Policy approves cloud (allow_cloud_escalation=true → default
    // allow_cloud=true), but PRD §15.2 gates the tier on a per-task
    // cloud_ok opt-in.
    let cloud_ok_defaults = PlanConstraints {
        allow_cloud: true,
        ..PlanConstraints::default()
    };

    // Arm 1: approval without opt-in → allow_cloud narrowed to false.
    let (cap, seen) = recording_cap(cloud_ok_defaults, &[]);
    let _ = cap
        .execute(&ctx_with_tags(&[]), json!({"goal": "run: ls"}))
        .await
        .expect("ok");
    let c = seen.lock().expect("lock").take().expect("recorded");
    assert!(!c.allow_cloud, "no cloud_ok tag and no explicit opt-in");

    // Arm 2: cloud_ok tag opts in.
    let _ = cap
        .execute(&ctx_with_tags(&["cloud_ok"]), json!({"goal": "run: ls"}))
        .await
        .expect("ok");
    let c = seen.lock().expect("lock").take().expect("recorded");
    assert!(c.allow_cloud, "cloud_ok tag + policy approval → cloud on");

    // Arm 3: explicit request constraint is the programmatic opt-in.
    let _ = cap
        .execute(
            &ctx_with_tags(&[]),
            json!({"goal": "run: ls", "constraints": {"allow_cloud": true}}),
        )
        .await
        .expect("ok");
    let c = seen.lock().expect("lock").take().expect("recorded");
    assert!(c.allow_cloud, "explicit allow_cloud: true opts in");

    // Arm 4: without policy approval the opt-ins are inert.
    let (cap, seen) = recording_cap(PlanConstraints::default(), &[]);
    let _ = cap
        .execute(&ctx_with_tags(&["cloud_ok"]), json!({"goal": "run: ls"}))
        .await
        .expect("ok");
    let c = seen.lock().expect("lock").take().expect("recorded");
    assert!(
        !c.allow_cloud,
        "cloud_ok tag alone cannot resurrect cloud when policy denies it"
    );
}

// ───────────────────────────────────────── 5.3 escalation triggers + replanning

use harness_policy::CloudTrigger;

/// A `Confident` response whose plan fails schema validation
/// (`cmd` is an integer) — the pure `plan_validation_failed` shape
/// (NOT `tool_not_found`: the capability exists, its input is wrong).
fn bad_schema_plan_response() -> PlanResponse {
    PlanResponse {
        plan: Unsigned(plan_one_node("shell.exec", json!({"cmd": 5}))),
        confidence: 0.9,
        rationale: "spy".into(),
        estimated_cost_usd: 0.0,
        estimated_duration_ms: 0,
        fallback_plan: None,
    }
}

/// Repair hints observed by a [`SeqSpy`], one entry per call.
type RepairLog = Arc<std::sync::Mutex<Vec<Option<String>>>>;

/// Sequence spy (5.3): scripted per-call outcomes, records the
/// `repair` field of every request it sees.
#[derive(Debug)]
struct SeqSpy {
    id: String,
    outcomes: std::sync::Mutex<std::collections::VecDeque<SpyOutcome>>,
    repairs: RepairLog,
}

#[async_trait]
impl PlannerBackend for SeqSpy {
    fn id(&self) -> &str {
        &self.id
    }
    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        self.repairs.lock().expect("lock").push(req.repair.clone());
        let next = self
            .outcomes
            .lock()
            .expect("lock")
            .pop_front()
            .expect("SeqSpy exhausted");
        match next {
            SpyOutcome::Confident(b) => Ok(PlanOutcome::Confident(b)),
            SpyOutcome::NoMatch => Ok(PlanOutcome::NoMatch),
        }
    }
}

fn seq_spy(id: &str, outcomes: Vec<SpyOutcome>) -> (Arc<dyn PlannerBackend>, RepairLog) {
    let repairs = Arc::new(std::sync::Mutex::new(Vec::new()));
    let spy = SeqSpy {
        id: id.to_string(),
        outcomes: std::sync::Mutex::new(outcomes.into_iter().collect()),
        repairs: Arc::clone(&repairs),
    };
    (Arc::new(spy), repairs)
}

fn escalation_cap(
    backends: Vec<Arc<dyn PlannerBackend>>,
    triggers: &[CloudTrigger],
    max_replanning: u32,
    budget: Option<u64>,
) -> BrainPlanCapability {
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    BrainPlanCapability::new(backends, provider, PlanConstraints::default())
        .with_escalation(triggers.iter().copied().collect(), max_replanning)
        .with_chain_budget(budget)
}

#[tokio::test]
async fn t33_validation_failure_opens_cloud_and_empty_trigger_set_keeps_it_shut() {
    // Arm 1: a local tier emits an invalid plan; trigger set contains
    // plan_validation_failed → the cloud tier is attempted and wins.
    let cloud_calls = Arc::new(AtomicUsize::new(0));
    let (bad_local, _) = seq_spy(
        "localfast:stub",
        vec![SpyOutcome::Confident(Box::new(bad_schema_plan_response()))],
    );
    let cloud: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "cloud:stub".into(),
        calls: cloud_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
    });
    let cap = escalation_cap(
        vec![bad_local, cloud],
        &[CloudTrigger::PlanValidationFailed],
        0,
        None,
    );
    let out = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect("cloud serves after the trigger fires");
    assert_eq!(out["confidence"], 0.9);
    assert_eq!(cloud_calls.load(Ordering::SeqCst), 1);

    // Arm 2: same failure, EMPTY trigger set → cloud never invoked;
    // the walk ends with the trigger-skip diagnostic.
    let cloud_calls = Arc::new(AtomicUsize::new(0));
    let (bad_local, _) = seq_spy(
        "localfast:stub",
        vec![SpyOutcome::Confident(Box::new(bad_schema_plan_response()))],
    );
    let cloud: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "cloud:stub".into(),
        calls: cloud_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
    });
    let cap = escalation_cap(vec![bad_local, cloud], &[], 0, None);
    let err = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect_err("no tier serves");
    assert_eq!(cloud_calls.load(Ordering::SeqCst), 0, "cloud stays shut");
    let msg = format!("{err}");
    assert!(
        msg.contains("no escalation trigger fired"),
        "diagnostic names the gate: {msg}"
    );
}

#[tokio::test]
async fn t34_tool_not_found_trigger_via_matched_but_unsupported() {
    let cloud_calls = Arc::new(AtomicUsize::new(0));
    let unsupported: Arc<dyn PlannerBackend> = Arc::new(EmitsUnsupported {
        id: "localfast:stub".into(),
    });
    let cloud: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "cloud:stub".into(),
        calls: cloud_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
    });
    let cap = escalation_cap(
        vec![unsupported, cloud],
        &[CloudTrigger::ToolNotFound],
        0,
        None,
    );
    let out = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect("cloud serves");
    assert_eq!(out["confidence"], 0.9);
    assert_eq!(cloud_calls.load(Ordering::SeqCst), 1);
}

/// Emits `MatchedButUnsupported` — the `tool_not_found` shape.
#[derive(Debug)]
struct EmitsUnsupported {
    id: String,
}

#[async_trait]
impl PlannerBackend for EmitsUnsupported {
    fn id(&self) -> &str {
        &self.id
    }
    async fn plan(&self, _req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        Ok(PlanOutcome::MatchedButUnsupported {
            matched_pattern: "run:",
            missing_capability: "shell.exec".to_string(),
        })
    }
}

#[tokio::test]
async fn t35_nomatch_never_opens_cloud() {
    // NoMatch is "this tier cannot help", not a failure — even the
    // full trigger set leaves cloud shut (pinning the ADR-0032
    // vocabulary: NoMatch is deliberately not a trigger).
    let cloud_calls = Arc::new(AtomicUsize::new(0));
    let (local, _) = seq_spy("localfast:stub", vec![SpyOutcome::NoMatch]);
    let cloud: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "cloud:stub".into(),
        calls: cloud_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
    });
    let all = [
        CloudTrigger::PlanValidationFailed,
        CloudTrigger::ToolNotFound,
        CloudTrigger::LowConfidence,
        CloudTrigger::BackendError,
    ];
    let cap = escalation_cap(vec![local, cloud], &all, 0, None);
    let _ = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect_err("nothing serves");
    assert_eq!(cloud_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn t36_cloud_is_baseline_when_no_local_llm_tier_exists() {
    // Model-less mesh with cloud configured: there is nothing to
    // escalate FROM, so cloud plans as baseline (ADR-0032 rule b) —
    // even with an empty trigger set.
    let cloud_calls = Arc::new(AtomicUsize::new(0));
    let cloud: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "cloud:stub".into(),
        calls: cloud_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
    });
    let cap = escalation_cap(vec![cloud], &[], 0, None);
    let out = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect("cloud baseline serves");
    assert_eq!(out["confidence"], 0.9);
    assert_eq!(cloud_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn t37_replanning_repairs_with_stricter_prompt() {
    // Invalid then valid: the second attempt must carry the repair
    // hint and the tier succeeds without escalating.
    let (local, repairs) = seq_spy(
        "localfast:stub",
        vec![
            SpyOutcome::Confident(Box::new(bad_schema_plan_response())),
            SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
        ],
    );
    let cap = escalation_cap(vec![local], &[], 2, None);
    let out = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect("repaired attempt serves");
    assert_eq!(out["confidence"], 0.9);
    let seen = repairs.lock().expect("lock").clone();
    assert_eq!(seen.len(), 2, "exactly one retry");
    assert!(seen[0].is_none(), "first attempt has no repair hint");
    let hint = seen[1].as_deref().expect("retry carries the hint");
    assert!(
        hint.contains("schema"),
        "hint is the validation error: {hint}"
    );
}

#[tokio::test]
async fn t38_zero_replanning_attempts_advances_after_one_call() {
    let (local, repairs) = seq_spy(
        "localfast:stub",
        vec![SpyOutcome::Confident(Box::new(bad_schema_plan_response()))],
    );
    let tpl: Arc<dyn PlannerBackend> = Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let cap = escalation_cap(vec![local, tpl], &[], 0, None);
    let _ = cap
        .execute(&ctx(), json!({"goal": "run: ls"}))
        .await
        .expect("template covers");
    assert_eq!(repairs.lock().expect("lock").len(), 1, "no retry");
}

#[tokio::test]
async fn t39_exhausted_chain_budget_skips_llm_tiers_but_never_template() {
    let llm_calls = Arc::new(AtomicUsize::new(0));
    let local: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "localfast:stub".into(),
        calls: llm_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
    });
    let tpl: Arc<dyn PlannerBackend> = Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let cap = escalation_cap(vec![local, tpl], &[], 0, Some(0));
    let out = cap
        .execute(&ctx(), json!({"goal": "run: ls"}))
        .await
        .expect("template is exempt from the budget");
    assert!(out["plan"].is_object());
    assert_eq!(llm_calls.load(Ordering::SeqCst), 0, "LLM tier never starts");
}

#[tokio::test]
async fn t40_low_confidence_trigger_opens_cloud() {
    let cloud_calls = Arc::new(AtomicUsize::new(0));
    let (local, _) = seq_spy(
        "localfast:stub",
        vec![SpyOutcome::Confident(Box::new(good_plan_response(
            "shell.exec",
            0.2,
        )))],
    );
    let cloud: Arc<dyn PlannerBackend> = Arc::new(SpyBackend {
        id: "cloud:stub".into(),
        calls: cloud_calls.clone(),
        outcome: SpyOutcome::Confident(Box::new(good_plan_response("shell.exec", 0.9))),
    });
    let provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> = Arc::new(snapshot_with_shell);
    let cap = BrainPlanCapability::new(
        vec![local, cloud],
        provider,
        PlanConstraints {
            confidence_threshold: Some(0.7),
            ..PlanConstraints::default()
        },
    )
    .with_escalation([CloudTrigger::LowConfidence].into_iter().collect(), 0);
    let out = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect("cloud serves after low-confidence trigger");
    assert_eq!(out["confidence"], 0.9);
    assert_eq!(cloud_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn t41_snapshot_cloud_caps_detects_paid_and_tagged_caps() {
    use harness_core::protocol::CostHint;
    use harness_core::{Cardinality, SemVer};

    #[derive(Debug)]
    struct StubCap {
        id: &'static str,
        cost_hint: CostHint,
        tags: Vec<String>,
    }

    #[async_trait]
    impl Capability for StubCap {
        fn id(&self) -> &str {
            self.id
        }
        fn manifest(&self) -> harness_core::Capability {
            harness_core::Capability {
                id: self.id.into(),
                version: SemVer {
                    major: 0,
                    minor: 1,
                    patch: 0,
                },
                cardinality: Cardinality::Anyone,
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                cost_hint: self.cost_hint,
                tags: self.tags.clone(),
                rate_limit: None,
                resource_hints: ResourceHints {
                    cpu_class: CpuClass::Light,
                    memory_mb: None,
                    gpu_required: false,
                    gpu_memory_mb: None,
                    network_class: NetworkClass::None,
                    disk_io_class: DiskIoClass::None,
                    estimated_duration_ms: None,
                },
                requires_secrets: vec![],
            }
        }
        async fn execute(
            &self,
            _ctx: &ExecutionContext,
            input: serde_json::Value,
        ) -> Result<serde_json::Value, harness_capabilities::traits::CapabilityError> {
            Ok(input)
        }
    }

    let registry = CapabilityRegistry::new();
    for cap in [
        StubCap {
            id: "llm.cloud.stub",
            cost_hint: CostHint::CloudPaid,
            tags: vec![],
        },
        StubCap {
            id: "gateway.stub",
            cost_hint: CostHint::LocalFast,
            tags: vec!["cloud".to_string()],
        },
        StubCap {
            id: "shell.stub",
            cost_hint: CostHint::LocalFast,
            tags: vec!["shell".to_string()],
        },
    ] {
        registry.register(Arc::new(cap)).expect("register");
    }

    let snap = registry.downgrade().snapshot();
    assert!(snap.cloud_caps.contains("llm.cloud.stub"), "CloudPaid hint");
    assert!(snap.cloud_caps.contains("gateway.stub"), "\"cloud\" tag");
    assert!(
        !snap.cloud_caps.contains("shell.stub"),
        "local caps stay out"
    );
    assert_eq!(snap.refs.len(), 3, "cloud detection never drops refs");
}
