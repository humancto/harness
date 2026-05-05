//! `PlannerBackend` trait and the request/response/outcome types.
//!
//! The trait is the abstraction every planner tier implements
//! (Template, `LocalFast` in 3.9, `LocalStrong`, Cloud, …). The
//! `brain.plan` capability holds an ordered
//! `Vec<Arc<dyn PlannerBackend>>` and walks it until a confident,
//! well-formed plan emerges.

use async_trait::async_trait;
use harness_core::{NodeId, Plan};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub use harness_core::{CapabilityRef, Unsigned};

use crate::error::PlannerError;

/// One planner tier. Implementations MUST be cheap to clone behind an
/// `Arc` and MUST be `Send + Sync` so they can be invoked concurrently
/// from the brain.plan capability.
#[async_trait]
pub trait PlannerBackend: Send + Sync + std::fmt::Debug {
    /// Stable identifier for diagnostics. Examples: `"template"`,
    /// `"llm:llama3.1:8b"`.
    fn id(&self) -> &str;

    /// Try to produce a plan. The four-state outcome is intentional:
    /// [`PlanOutcome::Confident`] short-circuits the escalation chain;
    /// [`PlanOutcome::NoMatch`] silently advances to the next tier;
    /// [`PlanOutcome::MatchedButUnsupported`] advances **with a
    /// diagnostic** that the executor accumulates and surfaces in the
    /// final error message if no later tier succeeds.
    ///
    /// `Err(PlannerError)` is reserved for transport / protocol
    /// failures (HTTP, decode, timeout); the executor treats those as
    /// "this tier didn't work" and advances, accumulating the error
    /// message into the final diagnostic.
    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError>;
}

/// One of the four outcomes a backend can return. See
/// [`PlannerBackend::plan`] for the contract.
///
/// `Confident` boxes the response so the enum size stays bounded
/// regardless of how `PlanResponse` grows (clippy `large_enum_variant`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PlanOutcome {
    /// Backend stands behind this plan. Caller validates + dispatches.
    Confident(Box<PlanResponse>),

    /// Backend cannot help with this goal. Caller escalates silently.
    NoMatch,

    /// Backend's heuristic / LLM matched the goal but a referenced
    /// capability is not registered on the planning node. Caller
    /// escalates AND collects the diagnostic for the final error if
    /// no later tier succeeds.
    MatchedButUnsupported {
        matched_pattern: &'static str,
        missing_capability: String,
    },
}

/// Input to [`PlannerBackend::plan`]. Built once per `brain.plan`
/// invocation and reused across the escalation chain so every backend
/// sees the same `available_capabilities` snapshot (no race window with
/// concurrent capability registration).
#[derive(Debug, Clone)]
pub struct PlanRequest {
    pub goal: String,
    pub available_capabilities: Vec<CapabilityRef>,
    pub constraints: PlanConstraints,
    pub context: Option<JsonValue>,
    /// Issuer of the `brain.plan` task — surfaced for templates and
    /// `LocalFast` (3.9) which inlines the issuer into the system
    /// prompt for traceability.
    pub issuing_node: NodeId,
}

/// Backend output. The plan is wrapped in [`Unsigned`] so the
/// dispatcher (3.10+) is type-required to sign before fan-out.
#[derive(Debug, Clone)]
#[must_use = "PlanResponse contains an unsigned plan; the dispatcher must sign it before fan-out"]
pub struct PlanResponse {
    pub plan: Unsigned<Plan>,
    pub confidence: f64,
    pub rationale: String,
    pub estimated_cost_usd: f64,
    pub estimated_duration_ms: u64,
    pub fallback_plan: Option<Unsigned<Plan>>,
}

/// Constraints the caller sets on the requested plan. Phase 3.8 only
/// uses `plan_max_nodes` as a soft hint (Template emits one-node plans
/// so there's nothing to constrain); 3.9 enforces `max_cost_usd`,
/// `must_be_local`, and `allow_cloud` at validation time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct PlanConstraints {
    pub max_cost_usd: Option<f64>,
    pub allow_cloud: bool,
    pub must_be_local: bool,
    pub plan_max_nodes: Option<u32>,
}

// Compile-time guarantees that the planner surface is shareable across
// async tasks. A future field that breaks this fails the build.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<std::sync::Arc<dyn PlannerBackend>>();
    assert_send_sync::<PlanRequest>();
    assert_send_sync::<PlanResponse>();
    assert_send_sync::<PlanOutcome>();
};
