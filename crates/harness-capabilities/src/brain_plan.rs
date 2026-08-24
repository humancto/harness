//! `brain.plan` — the `Anyone`-cardinality planner capability.
//!
//! Holds an ordered list of [`PlannerBackend`]s and walks them in tier
//! order until one returns a confident, well-formed, schema-valid plan
//! whose cost fits the cap. Phase 3.8 lineup was `[TemplateBackend]`;
//! 3.9 may prepend `LocalFastBackend` (via [`BrainPlanConfig`]).
//!
//! The capability is *policy-blind* by design (PRD §10.4): the planner
//! does not consult `PolicyEngine`. Plans are emitted; the executing
//! node enforces policy at execute time. The executing node may be a
//! different node with a different policy than the brain.
//!
//! Validation:
//! - 3.8 well-formedness (acyclic, caps-exist, non-empty, dangling-edge)
//! - 3.9 schema match (every `PlanNode.input` against the cap's
//!   `input_schema`) and cost cap (`estimated_cost_usd <= max_cost_usd`)
//!
//! Confidence threshold:
//! - When `constraints.confidence_threshold = Some(t)` and the backend
//!   returns `Confident(_)` with `confidence < t`, the executor treats
//!   the outcome as escalation (advance to next backend with a
//!   diagnostic). 3.8 had no threshold check; 3.9's default flows from
//!   `harness-policy::PlanningPolicy.confidence_threshold` (PRD §15.2
//!   default 0.7) via [`BrainPlanConfig::default_constraints`].

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use harness_brain::{
    backend::{PlanOutcome, PlanRequest, PlannerBackend, Unsigned},
    validate::validate_plan,
    CapabilitySchemaIndex, PlanConstraints,
};
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, CapabilityRef, Cardinality, SemVer};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use crate::registry::{CapabilityRegistry, CapabilitySnapshot};
use crate::traits::{audit_actor, Capability, CapabilityError, ExecutionContext};

/// Stable id for the capability surface.
pub const ID: &str = "brain.plan";

/// Daemon-supplied configuration for [`enrich_with_brain_plan`].
#[derive(Debug, Clone, Default)]
pub struct BrainPlanConfig {
    /// Default constraints applied to every `PlanRequest` whose input
    /// `constraints` field is absent or omits a sub-field. Daemon
    /// builds this from `harness-policy::PlanningPolicy`
    /// (`confidence_threshold`, `default_max_cost_usd`).
    pub default_constraints: PlanConstraints,

    /// 5.2 (ADR-0031) — PRD §10.4 `[planning] local_only_for_tags`:
    /// a task carrying any of these tags plans as `must_be_local`,
    /// whatever the merged constraints say (force-to-true only; an
    /// explicit `must_be_local: true` is never loosened). Gates the
    /// cloud tier for tagged tasks.
    pub local_only_for_tags: HashSet<String>,

    /// 5.3 (ADR-0032) — triggers that open the cloud tier after an
    /// earlier LLM tier failed (PRD §15.2 `escalate_to_cloud_if`).
    /// The derived default is EMPTY — cloud then runs only as
    /// baseline (no earlier LLM tier); the daemon always passes the
    /// policy value.
    pub escalate_to_cloud_if: Vec<harness_policy::CloudTrigger>,

    /// 5.3 — validation-repair retries per tier (PRD §15.2
    /// `max_replanning_attempts`; cloud is further capped at 1 by
    /// the executor). Derived default 0 = single attempt per tier
    /// (pre-5.3 behavior).
    pub max_replanning_attempts: u32,

    /// 5.3 — chain planning budget in ms. `None` = unbounded
    /// (derived default — direct-construction tests keep pre-5.3
    /// behavior); `Some(0)` = LLM/cloud tiers immediately skipped.
    /// The daemon passes `Some(210_000)` (the 240 s CLI plan default
    /// minus Template + polling slack).
    pub chain_budget_ms: Option<u64>,
}

/// `brain.plan` capability — wraps an ordered list of backends.
pub struct BrainPlanCapability {
    backends: Vec<Arc<dyn PlannerBackend>>,
    /// Snapshot provider — closure that returns
    /// `(refs, CapabilitySchemaIndex)` atomically. Set by
    /// [`enrich_with_brain_plan`] to a closure that downgrades the host
    /// registry to a `WeakCapabilityRegistry`. Tests can supply a
    /// static snapshot.
    snapshot_provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync>,
    /// Defaults applied when input.constraints is absent or omits a
    /// sub-field. Set by [`enrich_with_brain_plan`] from
    /// [`BrainPlanConfig::default_constraints`].
    default_constraints: PlanConstraints,
    /// Tags that force `must_be_local` planning (5.2, PRD §10.4).
    /// Empty by default (and in `new`); set via
    /// [`BrainPlanCapability::with_local_only_tags`].
    local_only_for_tags: HashSet<String>,
    /// 5.3 — see [`BrainPlanConfig::escalate_to_cloud_if`]. Empty in
    /// `new`; set via [`BrainPlanCapability::with_escalation`].
    escalate_to_cloud_if: HashSet<harness_policy::CloudTrigger>,
    /// 5.3 — see [`BrainPlanConfig::max_replanning_attempts`]. 0 in
    /// `new` (single attempt per tier, pre-5.3 behavior).
    max_replanning_attempts: u32,
    /// 5.3 — see [`BrainPlanConfig::chain_budget_ms`]. `None` in
    /// `new` (unbounded).
    chain_budget_ms: Option<u64>,
}

impl std::fmt::Debug for BrainPlanCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrainPlanCapability")
            .field(
                "backends",
                &self.backends.iter().map(|b| b.id()).collect::<Vec<_>>(),
            )
            .field("default_constraints", &self.default_constraints)
            .finish_non_exhaustive()
    }
}

impl BrainPlanCapability {
    #[must_use]
    pub fn new(
        backends: Vec<Arc<dyn PlannerBackend>>,
        snapshot_provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync>,
        default_constraints: PlanConstraints,
    ) -> Self {
        Self {
            backends,
            snapshot_provider,
            default_constraints,
            local_only_for_tags: HashSet::new(),
            escalate_to_cloud_if: HashSet::new(),
            max_replanning_attempts: 0,
            chain_budget_ms: None,
        }
    }

    /// 5.2: install the `local_only_for_tags` set (PRD §10.4).
    #[must_use]
    pub fn with_local_only_tags(mut self, tags: HashSet<String>) -> Self {
        self.local_only_for_tags = tags;
        self
    }

    /// 5.3: install the escalation-trigger set and per-tier repair
    /// budget (ADR-0032).
    #[must_use]
    pub fn with_escalation(
        mut self,
        triggers: HashSet<harness_policy::CloudTrigger>,
        max_replanning_attempts: u32,
    ) -> Self {
        self.escalate_to_cloud_if = triggers;
        self.max_replanning_attempts = max_replanning_attempts;
        self
    }

    /// 5.3: install the chain planning budget (ADR-0032). `None` =
    /// unbounded.
    #[must_use]
    pub fn with_chain_budget(mut self, budget_ms: Option<u64>) -> Self {
        self.chain_budget_ms = budget_ms;
        self
    }

    /// Merge request overrides onto the daemon defaults, then apply
    /// the 5.2 policy-driven narrowing rules (ADR-0031):
    ///
    /// - PRD §10.4 `local_only_for_tags`: tagged tasks plan
    ///   local-only. Force-to-true only — an explicit
    ///   `must_be_local: true` is never loosened by an absent tag.
    /// - PRD §15.2 "Cloud (if `cloud_ok`-tagged)": cloud escalation
    ///   needs policy approval AND a per-task opt-in — the
    ///   `cloud_ok` tag, or an explicit `allow_cloud: true` in the
    ///   request (the programmatic equivalent; both are
    ///   issuer-controlled). Narrowing only: a false `allow_cloud`
    ///   stays false.
    fn effective_constraints(
        &self,
        ctx: &ExecutionContext,
        input_constraints: Option<PlanConstraintsInput>,
    ) -> PlanConstraints {
        // Captured before the merge erases the distinction between
        // "the request explicitly asked for cloud" and "the policy
        // default happens to allow it".
        let explicit_cloud_opt_in = matches!(
            input_constraints.as_ref().and_then(|c| c.allow_cloud),
            Some(true)
        );
        let mut constraints = input_constraints
            .unwrap_or_default()
            .merge_over(self.default_constraints);

        if ctx
            .tags
            .iter()
            .any(|t| self.local_only_for_tags.contains(t.as_str()))
        {
            constraints.must_be_local = true;
        }

        if constraints.allow_cloud
            && !explicit_cloud_opt_in
            && !ctx.tags.iter().any(|t| t == "cloud_ok")
        {
            constraints.allow_cloud = false;
        }
        constraints
    }
}

/// Per-call constraint overrides. Each field is `Option<T>` so an
/// absent field means "use the default" (NOT "false" / "zero"). The
/// distinction matters for `bool` fields: `must_be_local` defaults to
/// `false` in the daemon's `default_constraints`, and an explicit
/// `false` in input is indistinguishable from absence — but an
/// explicit `true` overrides while absence preserves the (potentially
/// `true`) default.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PlanConstraintsInput {
    max_cost_usd: Option<f64>,
    allow_cloud: Option<bool>,
    must_be_local: Option<bool>,
    plan_max_nodes: Option<u32>,
    confidence_threshold: Option<f64>,
}

impl PlanConstraintsInput {
    fn merge_over(self, defaults: PlanConstraints) -> PlanConstraints {
        PlanConstraints {
            max_cost_usd: self.max_cost_usd.or(defaults.max_cost_usd),
            allow_cloud: self.allow_cloud.unwrap_or(defaults.allow_cloud),
            must_be_local: self.must_be_local.unwrap_or(defaults.must_be_local),
            plan_max_nodes: self.plan_max_nodes.or(defaults.plan_max_nodes),
            confidence_threshold: self.confidence_threshold.or(defaults.confidence_threshold),
        }
    }
}

/// JSON shape of the `brain.plan` capability input.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrainPlanInput {
    goal: String,
    #[serde(default)]
    constraints: Option<PlanConstraintsInput>,
    #[serde(default)]
    context: Option<JsonValue>,
    /// Optional override for `available_capabilities`. Per PRD §15.5:
    /// a brain on `mac-mini` may dispatch `brain.plan` to a `gpu-box`
    /// peer with the brain's own capability list rather than the
    /// peer's. Absent → snapshot the local registry.
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
                    "constraints": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "max_cost_usd":         { "type": ["number", "null"], "minimum": 0 },
                            "allow_cloud":          { "type": ["boolean", "null"] },
                            "must_be_local":        { "type": ["boolean", "null"] },
                            "plan_max_nodes":       { "type": ["integer", "null"], "minimum": 1 },
                            "confidence_threshold": { "type": ["number", "null"], "minimum": 0, "maximum": 1 }
                        }
                    },
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

        // Snapshot once. The same `(refs, schemas)` is reused for every
        // backend so a registration race mid-flight cannot give backend
        // N+1 a different view than backend N.
        let snapshot = (self.snapshot_provider)();

        // Input-override path: caller supplies `available_capabilities`.
        // We still pull `schemas` from the local registry — locally-
        // registered caps validate, foreign caps surface UnknownSchema
        // (the dispatcher's tag-aware routing in 3.6-encrypted picks up
        // the validation slack on the executing node).
        let available = input
            .available_capabilities
            .unwrap_or_else(|| snapshot.refs.clone());
        let schemas: CapabilitySchemaIndex = snapshot.schemas;

        let constraints = self.effective_constraints(ctx, input.constraints);

        let req = PlanRequest {
            goal: input.goal,
            available_capabilities: available,
            schemas,
            constraints,
            context: input.context,
            issuing_node: ctx.issued_by,
            repair: None,
        };

        self.walk_lineup(&req, &snapshot.cloud_caps, ctx).await
    }
}

/// Backend tier, classified from the backend id (5.3, ADR-0032). The
/// trait object exposes only `id()`; the shipped prefixes are stable
/// (`localfast:`/`localstrong:`/`cloud:`/template). Unknown prefixes
/// count as LLM tiers — the conservative reading for escalation
/// accounting, and never eligible for the cloud gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlannerTier {
    Llm,
    Cloud,
    Template,
}

fn classify_tier(id: &str) -> PlannerTier {
    if id.starts_with("cloud:") {
        PlannerTier::Cloud
    } else if id == harness_brain::TEMPLATE_BACKEND_ID {
        PlannerTier::Template
    } else {
        PlannerTier::Llm
    }
}

/// Escalation accounting (ADR-0032, diff review MINOR-1): only LOCAL
/// LLM tiers feed the trigger set — a cloud tier's own failure must
/// never open a later cloud tier, and Template sits after every gate.
/// Latent today (one cloud tier, evaluated before it runs), enforced
/// so a future multi-cloud lineup cannot self-escalate.
fn fire_trigger(
    fired: &mut HashSet<harness_policy::CloudTrigger>,
    tier: PlannerTier,
    trigger: harness_policy::CloudTrigger,
) {
    if tier == PlannerTier::Llm {
        fired.insert(trigger);
    }
}

impl BrainPlanCapability {
    /// 5.3 (ADR-0032): the escalation walk. Tier order is the lineup;
    /// per tier there is an attempt loop (first try + validation-repair
    /// retries), a chain planning budget that LLM/cloud attempts may
    /// neither start past nor run beyond (Template is exempt — the
    /// §15.2 floor), and a trigger gate in front of the cloud tier:
    /// cloud is attempted only if an earlier LLM tier produced a
    /// condition in `escalate_to_cloud_if` — or if no earlier LLM tier
    /// exists at all (cloud-as-baseline on a model-less mesh; there is
    /// nothing to escalate FROM).
    #[allow(clippy::too_many_lines)]
    async fn walk_lineup(
        &self,
        req: &PlanRequest,
        cloud_caps: &HashSet<String>,
        ctx: &ExecutionContext,
    ) -> Result<JsonValue, CapabilityError> {
        use harness_brain::PlanValidationError;
        use harness_policy::CloudTrigger;

        let started = std::time::Instant::now();
        let mut diagnostics: Vec<String> = Vec::new();
        let mut fired: HashSet<CloudTrigger> = HashSet::new();
        let mut earlier_llm_tier = false;

        for backend in &self.backends {
            let id = backend.id();
            let tier = classify_tier(id);

            if tier == PlannerTier::Cloud
                && earlier_llm_tier
                && !self.escalate_to_cloud_if.iter().any(|t| fired.contains(t))
            {
                diagnostics.push(format!(
                    "{id}: skipped — no escalation trigger fired (policy escalate_to_cloud_if)"
                ));
                continue;
            }
            if tier == PlannerTier::Llm {
                earlier_llm_tier = true;
            }
            if tier == PlannerTier::Cloud {
                // 5.13a (ADR-0041): §10.6 names cloud escalation a
                // privileged action — this is the moment a local-first
                // mesh decides to send a goal off-LAN. The GOAL never
                // enters the record (it is user text, and this row is
                // destined for replication); the backend id and the
                // triggers that opened the gate do.
                ctx.audit(
                    harness_core::AuditRecord::new(
                        harness_core::AuditAction::CloudEscalated,
                        audit_actor(ctx),
                    )
                    .with_subject(id.to_string())
                    .with_detail(&serde_json::json!({
                        "task_id": ctx.task_id.0.to_string(),
                        "triggers": fired.iter().map(|t| format!("{t:?}")).collect::<Vec<_>>(),
                        "baseline": !earlier_llm_tier,
                    })),
                );
            }

            // Validation-repair retries (§15.4 "retry with stricter
            // prompt"). Template is deterministic — never retried.
            // Cloud retries cost real money — capped at one.
            let max_retries = match tier {
                PlannerTier::Template => 0,
                PlannerTier::Cloud => self.max_replanning_attempts.min(1),
                PlannerTier::Llm => self.max_replanning_attempts,
            };
            let mut repair: Option<String> = None;

            'attempts: for attempt in 0..=max_retries {
                // Chain budget: an LLM/cloud attempt may not START
                // past the budget, and a running one is clipped to
                // the remainder — Template always gets to run.
                let remaining_ms = match (tier, self.chain_budget_ms) {
                    (PlannerTier::Template, _) | (_, None) => None,
                    (_, Some(budget)) => {
                        let elapsed =
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                        if elapsed >= budget {
                            diagnostics
                                .push(format!("{id}: skipped — chain planning budget exhausted"));
                            break 'attempts;
                        }
                        Some(budget - elapsed)
                    }
                };

                let repaired_req;
                let request = if let Some(r) = &repair {
                    repaired_req = PlanRequest {
                        repair: Some(r.clone()),
                        ..req.clone()
                    };
                    &repaired_req
                } else {
                    req
                };

                let planned = match remaining_ms {
                    Some(ms) => {
                        let clip = std::time::Duration::from_millis(ms);
                        let Ok(clipped) = tokio::time::timeout(clip, backend.plan(request)).await
                        else {
                            diagnostics.push(format!(
                                "{id}: aborted — chain planning budget exhausted mid-attempt"
                            ));
                            break 'attempts;
                        };
                        clipped
                    }
                    None => backend.plan(request).await,
                };

                match planned {
                    Ok(PlanOutcome::Confident(resp)) => {
                        // 3.9 confidence-threshold gate — escalation,
                        // not repair (a threshold miss is a capability
                        // ceiling, not a fixable mistake).
                        if let Some(threshold) = req.constraints.confidence_threshold {
                            if resp.confidence < threshold {
                                diagnostics.push(format!(
                                    "{id}: confidence {:.2} < threshold {threshold:.2}",
                                    resp.confidence,
                                ));
                                fire_trigger(&mut fired, tier, CloudTrigger::LowConfidence);
                                break 'attempts;
                            }
                        }
                        // Full validation: structural + schema + cost
                        // + locality (5.3).
                        let validation = validate_plan(
                            resp.plan.as_inner(),
                            resp.estimated_cost_usd,
                            &req.constraints,
                            &req.schemas,
                            &req.available_capabilities,
                            cloud_caps,
                        );
                        match validation {
                            Ok(()) => return encode_success(&resp),
                            Err(e) => {
                                fire_trigger(&mut fired, tier, CloudTrigger::PlanValidationFailed);
                                if matches!(
                                    e,
                                    PlanValidationError::UnknownCapability { .. }
                                        | PlanValidationError::UnknownSchema { .. }
                                ) {
                                    fire_trigger(&mut fired, tier, CloudTrigger::ToolNotFound);
                                }
                                diagnostics.push(format!(
                                    "{id}: validation failed (attempt {}): {e}",
                                    attempt + 1
                                ));
                                if attempt < max_retries {
                                    repair = Some(e.to_string());
                                    continue 'attempts;
                                }
                                break 'attempts;
                            }
                        }
                    }
                    Ok(PlanOutcome::NoMatch) => break 'attempts,
                    Ok(PlanOutcome::MatchedButUnsupported {
                        matched_pattern,
                        missing_capability,
                    }) => {
                        fire_trigger(&mut fired, tier, CloudTrigger::ToolNotFound);
                        diagnostics.push(format!(
                            "{id}: matched pattern {matched_pattern:?} but capability {missing_capability:?} not registered"
                        ));
                        break 'attempts;
                    }
                    Err(e) => {
                        fire_trigger(&mut fired, tier, CloudTrigger::BackendError);
                        diagnostics.push(format!("{id}: backend error: {e}"));
                        break 'attempts;
                    }
                    // PlanOutcome is `#[non_exhaustive]`. A future
                    // variant we don't recognize is treated as "this
                    // backend cannot help" — the only fail-closed
                    // answer.
                    Ok(_other) => {
                        diagnostics.push(format!(
                            "{id}: backend returned an unknown PlanOutcome variant"
                        ));
                        break 'attempts;
                    }
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

fn encode_success(resp: &harness_brain::PlanResponse) -> Result<JsonValue, CapabilityError> {
    let inner = resp.plan.as_inner().clone();
    let fallback = resp.fallback_plan.as_ref().map(Unsigned::as_inner);
    serde_json::to_value(serde_json::json!({
        "plan":                  inner,
        "confidence":            resp.confidence,
        "rationale":             resp.rationale,
        "estimated_cost_usd":    resp.estimated_cost_usd,
        "estimated_duration_ms": resp.estimated_duration_ms,
        "fallback_plan":         fallback,
    }))
    .map_err(|e| CapabilityError::Failed(format!("encode plan response: {e}")))
}

/// Register a `brain.plan` capability into `registry` with the given
/// backend lineup. The daemon builds the lineup; for 3.8 it was
/// `[TemplateBackend]`, for 3.9 it may be `[LocalFastBackend, TemplateBackend]`.
///
/// `async` even though Template construction is sync — `LocalFast` may
/// probe Ollama at enrich time in future extensions.
///
/// Idempotent-detected via `expect("BUG: enrich_with_brain_plan called twice")`,
/// matching `enrich_with_llm_local` / `enrich_with_llm_cloud_claude`.
pub async fn enrich_with_brain_plan(
    registry: &CapabilityRegistry,
    backends: Vec<Arc<dyn PlannerBackend>>,
    config: BrainPlanConfig,
) {
    let weak = registry.downgrade();
    let snapshot_provider: Arc<dyn Fn() -> CapabilitySnapshot + Send + Sync> =
        Arc::new(move || weak.snapshot());
    let cap = BrainPlanCapability::new(backends, snapshot_provider, config.default_constraints)
        .with_local_only_tags(config.local_only_for_tags)
        .with_escalation(
            config.escalate_to_cloud_if.into_iter().collect(),
            config.max_replanning_attempts,
        )
        .with_chain_budget(config.chain_budget_ms);
    #[allow(clippy::expect_used)]
    registry
        .register(Arc::new(cap))
        .expect("BUG: enrich_with_brain_plan called twice");
}
