//! `brain.plan` — the `Anyone`-cardinality planner capability.
//!
//! Holds an ordered list of [`PlannerBackend`]s and walks them in tier
//! order until one returns a confident, well-formed plan. The Phase 3.8
//! lineup is `[TemplateBackend]`; 3.9 prepends `LocalFastBackend`,
//! 3.6-cloud-escalation prepends a cloud tier.
//!
//! The capability is *policy-blind* by design (PRD §10.4): the planner
//! does not consult `PolicyEngine`. Plans are emitted; the executing
//! node enforces policy at execute time. The executing node may be a
//! different node with a different policy than the brain.

use std::sync::Arc;

use async_trait::async_trait;
// Phase 3.9 migration to `validate_plan` lands in the next commit
// (CapabilitySnapshot + brain_plan migration). Until then, suppress
// the deprecation warning at the use site so the workspace builds
// clean against `-D warnings`.
#[allow(deprecated)]
use harness_brain::{
    backend::{PlanOutcome, PlanRequest, PlannerBackend, Unsigned},
    validate::validate_plan_well_formed,
    PlanConstraints,
};
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, CapabilityRef, Cardinality, SemVer};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use crate::registry::CapabilityRegistry;
use crate::traits::{Capability, CapabilityError, ExecutionContext};

/// Stable id for the capability surface.
pub const ID: &str = "brain.plan";

/// `brain.plan` capability — wraps an ordered list of backends.
pub struct BrainPlanCapability {
    backends: Vec<Arc<dyn PlannerBackend>>,
    /// Snapshot provider for `available_capabilities`. Set by
    /// [`enrich_with_brain_plan`] to a closure that downgrades the
    /// host registry to a `WeakCapabilityRegistry` and reads through
    /// it on every call. Tests can supply a static-Vec closure.
    available_provider: Arc<dyn Fn() -> Vec<CapabilityRef> + Send + Sync>,
}

impl std::fmt::Debug for BrainPlanCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrainPlanCapability")
            .field(
                "backends",
                &self.backends.iter().map(|b| b.id()).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl BrainPlanCapability {
    #[must_use]
    pub fn new(
        backends: Vec<Arc<dyn PlannerBackend>>,
        available_provider: Arc<dyn Fn() -> Vec<CapabilityRef> + Send + Sync>,
    ) -> Self {
        Self {
            backends,
            available_provider,
        }
    }
}

/// JSON shape of the `brain.plan` capability input.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrainPlanInput {
    goal: String,
    #[serde(default)]
    constraints: Option<PlanConstraints>,
    #[serde(default)]
    context: Option<JsonValue>,
    /// Optional override for `available_capabilities`. Per PRD §15.5:
    /// a brain on `mac-mini` may dispatch `brain.plan` to a `gpu-box`
    /// peer with the brain's own capability list rather than the
    /// peer's. Absent → snapshot the local registry via the closure.
    #[serde(default)]
    available_capabilities: Option<Vec<CapabilityRef>>,
}

#[async_trait]
impl Capability for BrainPlanCapability {
    fn id(&self) -> &str {
        ID
    }

    fn manifest(&self) -> ManifestEntry {
        ManifestEntry {
            id: ID.to_string(),
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            cardinality: Cardinality::Anyone,
            input_schema: json!({
                "type": "object",
                "required": ["goal"],
                "additionalProperties": false,
                "properties": {
                    "goal": { "type": "string", "minLength": 1 },
                    "constraints": { "type": "object" },
                    "context": {},
                    "available_capabilities": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["id", "version_major"],
                            "properties": {
                                "id": { "type": "string", "minLength": 1 },
                                "version_major": { "type": "integer", "minimum": 0 }
                            }
                        }
                    }
                }
            }),
            output_schema: json!({
                "type": "object",
                "required": [
                    "plan", "confidence", "rationale",
                    "estimated_cost_usd", "estimated_duration_ms"
                ],
                "properties": {
                    "plan": { "type": "object" },
                    "confidence": { "type": "number", "minimum": 0, "maximum": 1 },
                    "rationale": { "type": "string" },
                    "estimated_cost_usd": { "type": "number", "minimum": 0 },
                    "estimated_duration_ms": { "type": "integer", "minimum": 0 },
                    "fallback_plan": { "type": ["object", "null"] }
                }
            }),
            cost_hint: CostHint::LocalFast,
            tags: vec!["brain".to_string(), "planner".to_string()],
            rate_limit: Some(RateLimit {
                per_second: 5,
                burst: 10,
            }),
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: NetworkClass::None,
                disk_io_class: DiskIoClass::None,
                estimated_duration_ms: Some(5),
            },
            requires_secrets: vec![],
        }
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        let input: BrainPlanInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;
        if input.goal.trim().is_empty() {
            return Err(CapabilityError::InvalidInput("goal is empty".to_string()));
        }

        // Snapshot available_capabilities once. The same Vec is reused
        // for every backend so a registration race mid-flight cannot
        // give backend N+1 a different list than backend N.
        let available = input
            .available_capabilities
            .unwrap_or_else(|| (self.available_provider)());

        let req = PlanRequest {
            goal: input.goal,
            available_capabilities: available,
            constraints: input.constraints.unwrap_or_default(),
            context: input.context,
            issuing_node: ctx.issued_by,
        };

        let mut diagnostics: Vec<String> = Vec::new();
        for backend in &self.backends {
            match backend.plan(&req).await {
                Ok(PlanOutcome::Confident(resp)) => {
                    // 3.8 has no confidence-threshold check. Validate
                    // well-formedness; 3.9 layers on schema/cost.
                    #[allow(deprecated)]
                    let validation = validate_plan_well_formed(
                        resp.plan.as_inner(),
                        &req.available_capabilities,
                    );
                    if let Err(e) = validation {
                        diagnostics.push(format!("{}: validation failed: {e}", backend.id()));
                        continue;
                    }
                    let inner = resp.plan.as_inner().clone();
                    let fallback = resp.fallback_plan.as_ref().map(Unsigned::as_inner);
                    return serde_json::to_value(serde_json::json!({
                        "plan":                  inner,
                        "confidence":            resp.confidence,
                        "rationale":             resp.rationale,
                        "estimated_cost_usd":    resp.estimated_cost_usd,
                        "estimated_duration_ms": resp.estimated_duration_ms,
                        "fallback_plan":         fallback,
                    }))
                    .map_err(|e| CapabilityError::Failed(format!("encode plan response: {e}")));
                }
                Ok(PlanOutcome::NoMatch) => continue,
                Ok(PlanOutcome::MatchedButUnsupported {
                    matched_pattern,
                    missing_capability,
                }) => {
                    diagnostics.push(format!(
                        "{}: matched pattern {matched_pattern:?} but capability {missing_capability:?} not registered",
                        backend.id()
                    ));
                    continue;
                }
                Err(e) => {
                    diagnostics.push(format!("{}: backend error: {e}", backend.id()));
                    continue;
                }
                // PlanOutcome is `#[non_exhaustive]`. A future variant
                // we don't recognize is treated as "this backend can
                // not help" — the only fail-closed answer.
                Ok(_other) => {
                    diagnostics.push(format!(
                        "{}: backend returned an unknown PlanOutcome variant",
                        backend.id()
                    ));
                    continue;
                }
            }
        }

        let summary = if diagnostics.is_empty() {
            "no backend matched the goal".to_string()
        } else {
            diagnostics.join("; ")
        };
        Err(CapabilityError::Failed(format!(
            "no backend produced a confident plan: {summary}"
        )))
    }
}

/// Register a `brain.plan` capability into `registry`. The backend
/// lineup is just `[TemplateBackend]` for Phase 3.8; 3.9 will prepend
/// the `LocalFast` tier.
///
/// `async` even though Template construction is sync — keeps the API
/// stable for 3.9, which probes Ollama at enrich time.
///
/// Idempotent only for fresh registries: a duplicate call panics with
/// `BUG: enrich_with_brain_plan called twice`, matching
/// `enrich_with_llm_local` / `enrich_with_llm_cloud_claude`.
pub async fn enrich_with_brain_plan(
    registry: &CapabilityRegistry,
    local_node: harness_core::NodeId,
) {
    let weak = registry.downgrade();
    let provider: Arc<dyn Fn() -> Vec<CapabilityRef> + Send + Sync> = Arc::new(move || weak.refs());
    let template = Arc::new(harness_brain::TemplateBackend::new(local_node));
    let cap = BrainPlanCapability::new(vec![template], provider);
    #[allow(clippy::expect_used)]
    registry
        .register(Arc::new(cap))
        .expect("BUG: enrich_with_brain_plan called twice");
}
