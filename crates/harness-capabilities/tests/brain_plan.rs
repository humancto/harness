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
    }
}

fn empty_snapshot() -> CapabilitySnapshot {
    CapabilitySnapshot {
        refs: vec![],
        schemas: CapabilitySchemaIndex::default(),
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
