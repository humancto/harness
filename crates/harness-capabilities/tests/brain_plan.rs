//! Phase 3.8 — `brain.plan` capability integration tests.

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
use harness_brain::TemplateBackend;
use harness_capabilities::brain_plan::{enrich_with_brain_plan, BrainPlanCapability, ID};
use harness_capabilities::traits::{Capability, ExecutionContext};
use harness_capabilities::CapabilityRegistry;
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

fn ctx() -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
        tags: Arc::from(Vec::<String>::new()),
    }
}

fn cap_with_template_and(provider_caps: Vec<CapabilityRef>) -> BrainPlanCapability {
    let template = Arc::new(TemplateBackend::new(NodeId::from_bytes([1; 16])));
    let provider: Arc<dyn Fn() -> Vec<CapabilityRef> + Send + Sync> =
        Arc::new(move || provider_caps.clone());
    BrainPlanCapability::new(vec![template], provider)
}

#[test]
fn t16_brain_plan_capability_id_is_brain_plan() {
    let cap = cap_with_template_and(shell_only());
    assert_eq!(ID, "brain.plan");
    assert_eq!(cap.id(), "brain.plan");
}

#[test]
fn t17_brain_plan_manifest_advertises_anyone_local_fast_no_secrets() {
    let cap = cap_with_template_and(shell_only());
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
    let cap = cap_with_template_and(shell_only());
    // Missing `goal` field.
    let err = cap
        .execute(&ctx(), json!({}))
        .await
        .expect_err("must reject missing goal");
    assert!(matches!(
        err,
        harness_capabilities::traits::CapabilityError::InvalidInput(_)
    ));

    // Empty goal string.
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
    let cap = cap_with_template_and(shell_only());
    let out = cap
        .execute(&ctx(), json!({"goal": "run: ls"}))
        .await
        .expect("happy path");
    // Output JSON: { plan: { tasks: { <id>: { capability, input: ... } } }, ... }
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
    // input override supplies it, so the plan should still emit.
    let cap = cap_with_template_and(vec![]);
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
        .expect("override path must succeed");
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
    let seen_a: Arc<parking_lot::Mutex<Vec<Vec<CapabilityRef>>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen_b: Arc<parking_lot::Mutex<Vec<Vec<CapabilityRef>>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
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
    // backend B sees the same Vec backend A saw — even though the
    // mutex now holds something different.
    let live_caps: Arc<parking_lot::Mutex<Vec<CapabilityRef>>> =
        Arc::new(parking_lot::Mutex::new(shell_only()));
    let counter = Arc::new(AtomicUsize::new(0));
    let live_for_provider = live_caps.clone();
    let counter_for_provider = counter.clone();
    let provider: Arc<dyn Fn() -> Vec<CapabilityRef> + Send + Sync> = Arc::new(move || {
        // After the first call, mutate the registry by clearing the
        // "live" caps. If the cap calls the provider per-backend, the
        // second call would see the mutation.
        let v = live_for_provider.lock().clone();
        if counter_for_provider.fetch_add(1, Ordering::SeqCst) == 0 {
            live_for_provider.lock().clear();
        }
        v
    });
    let cap = BrainPlanCapability::new(vec![backend_a, backend_b], provider);

    let _ = cap.execute(&ctx(), json!({"goal": "run: ls"})).await;
    // Both backends must have seen the same list (the pre-mutation one).
    let a = seen_a.lock().clone();
    let b = seen_b.lock().clone();
    assert_eq!(a.len(), 1, "backend a invoked once");
    assert_eq!(b.len(), 1, "backend b invoked once");
    assert_eq!(a[0], b[0], "snapshot must be identical across backends");
    assert_eq!(a[0], shell_only(), "snapshot is the pre-mutation list");
}

#[tokio::test]
async fn t22_brain_plan_no_backend_match_returns_failed() {
    // No backend matches "complicated". Since template's `fetch:` etc.
    // require unsupported caps, we use plain "complicated".
    let cap = cap_with_template_and(shell_only());
    let err = cap
        .execute(&ctx(), json!({"goal": "complicated multi-step request"}))
        .await
        .expect_err("must fail");
    let msg = format!("{err}");
    assert!(msg.contains("no backend produced"), "msg = {msg}");

    // Confirm MatchedButUnsupported propagates into the diagnostic.
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
    enrich_with_brain_plan(&registry, NodeId::from_bytes([1; 16])).await;
    let registry2 = registry.clone();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // tokio::runtime::Handle::current() does not work inside
        // catch_unwind in this test harness, so we use a fresh
        // single-threaded runtime to drive the second enrich.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            enrich_with_brain_plan(&registry2, NodeId::from_bytes([1; 16])).await;
        });
    }));
    assert!(result.is_err(), "second enrich must panic");
}

/// Validates well-formedness checks reject a plan with an unknown
/// capability the template would never emit. Constructed by hand so
/// the test does not depend on Template.
fn plan_one_node(cap: &str) -> Plan {
    let id = TaskId::new_v7();
    let mut tasks = HashMap::new();
    tasks.insert(
        id,
        PlanNode {
            id,
            capability: cap.into(),
            input: json!({}),
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

/// Backend that emits a confident plan referencing a capability that
/// the executor's `available` list does NOT contain. The executor
/// must reject via `validate_plan_well_formed` and surface a diagnostic.
#[derive(Debug)]
struct EmitsUnknownCap;

#[async_trait]
impl PlannerBackend for EmitsUnknownCap {
    fn id(&self) -> &str {
        "emits-unknown"
    }

    async fn plan(&self, _req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        let plan = plan_one_node("never.registered");
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

#[tokio::test]
async fn t23b_brain_plan_validation_failure_propagates_to_diagnostic() {
    let provider: Arc<dyn Fn() -> Vec<CapabilityRef> + Send + Sync> = Arc::new(shell_only);
    let cap = BrainPlanCapability::new(vec![Arc::new(EmitsUnknownCap)], provider);
    let err = cap
        .execute(&ctx(), json!({"goal": "anything"}))
        .await
        .expect_err("must fail");
    let msg = format!("{err}");
    assert!(msg.contains("validation failed"), "msg = {msg}");
}

#[tokio::test]
async fn t24_brain_plan_capability_dropped_when_registry_dropped() {
    // Build a registry, register the cap, capture a Weak to the inner
    // Arc<dyn Capability>, drop the registry, assert the Weak fails to
    // upgrade — the WeakCapabilityRegistry held by the cap does NOT
    // extend the registry's lifetime.
    let weak_cap: std::sync::Weak<dyn Capability> = {
        let registry = CapabilityRegistry::new();
        enrich_with_brain_plan(&registry, NodeId::from_bytes([1; 16])).await;
        let strong = registry.get("brain.plan").expect("registered");
        Arc::downgrade(&strong)
        // `strong` and `registry` drop here.
    };
    assert!(
        weak_cap.upgrade().is_none(),
        "brain.plan capability must drop with its registry"
    );
}
