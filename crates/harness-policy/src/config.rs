//! Typed shape of `~/.harness/policy.toml` (PRD §10.4).
//!
//! Every public struct/enum here is `#[serde(deny_unknown_fields)]` so a
//! typo in policy.toml fails loudly at parse time instead of silently
//! collapsing into the wrong rule. `DenyRule` is `#[serde(untagged)]`
//! because the PRD example mixes `{ pattern = "..." }` and
//! `{ cmd = "...", any_args = true }` shapes inside the same array.

use std::collections::{HashMap, HashSet};

use serde::Deserialize;

/// Top-level `policy.toml`. Missing sections fall back to `Default`,
/// which evaluates to deny-all for shell.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    #[serde(default)]
    pub shell: ShellPolicy,

    #[serde(default)]
    pub capability: CapabilityPolicy,

    #[serde(default)]
    pub planning: PlanningPolicy,

    /// `[llm]` section — `None` = section absent → default-allow,
    /// `Some(p)` = section present → matrix in `evaluate_llm`.
    /// (Deliberately `Option` to distinguish "absent" from "empty".)
    #[serde(default)]
    pub llm: Option<LlmPolicy>,

    /// `[mcp]` section — Phase 3.7. Default (section absent or empty)
    /// is **deny-all**, matching shell: MCP tools are arbitrary code
    /// provided by external servers, so an operator must opt in
    /// explicitly. Non-`Option` because absent and empty mean the
    /// same thing here (unlike `[llm]`).
    #[serde(default)]
    pub mcp: McpPolicy,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellPolicy {
    #[serde(default)]
    pub allow: Vec<ShellAllow>,

    #[serde(default)]
    pub deny: Vec<DenyRule>,

    #[serde(default)]
    pub from: HashMap<String, TrustLevel>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellAllow {
    pub cmd: String,

    #[serde(default)]
    pub any_args: bool,

    #[serde(default)]
    pub subcmds: Vec<String>,
}

/// Heterogeneous deny entry. `untagged` discriminates by which fields
/// are present: `pattern` → `Pattern`, `cmd` → `Cmd`. Each inner struct
/// carries `deny_unknown_fields` so a typo cannot silently match the
/// wrong variant.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum DenyRule {
    Cmd(DenyCmd),
    Pattern(DenyPattern),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DenyCmd {
    pub cmd: String,

    #[serde(default)]
    pub any_args: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DenyPattern {
    pub pattern: String,
}

/// Trust level for a source node, declared in `[shell.from]`.
///
/// 3.1 enforces only `Untrusted → deny-all-shell`. `Trusted` is the
/// hook for `require_2fa_for` bypass that 3.6 will wire up. `Default`
/// is the baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum TrustLevel {
    Trusted,
    Default,
    Untrusted,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityPolicy {
    #[serde(default)]
    pub default_local_only: bool,

    #[serde(default)]
    pub require_2fa_for: HashSet<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningPolicy {
    #[serde(default)]
    pub allow_cloud_escalation: bool,

    #[serde(default)]
    pub local_only_for_tags: HashSet<String>,

    /// Phase 3.9 — minimum confidence a `Confident(_)` planner outcome
    /// must meet before the `brain.plan` executor accepts it. Below
    /// this threshold the executor escalates to the next backend tier.
    /// PRD §15.2 default is 0.7; Template returns 0.6 (so 3.9
    /// `LocalFast` returning 0.85+ wins automatically).
    #[serde(default = "default_confidence_threshold")]
    pub confidence_threshold: f64,

    /// Phase 3.9 — preferred local LLM models in priority order. The
    /// daemon picks the first one that's locally registered (via
    /// `llm.local.*`); the rest are advisory. Empty → Template-only
    /// brain.plan lineup.
    #[serde(default)]
    pub prefer_local_models: Vec<String>,

    /// Phase 3.9 — default `max_cost_usd` cap applied to plans whose
    /// input omits the field. `None` = no cap. Defaults to `Some(1.0)`
    /// — a conservative starting point; operators raise per their tier.
    #[serde(default = "default_max_cost_usd")]
    pub default_max_cost_usd: Option<f64>,

    /// 5.2 (ADR-0031) — Anthropic model the tier-3 Cloud planner
    /// backend uses. Only consulted when `allow_cloud_escalation` is
    /// true (the cloud tier is not even registered otherwise). Empty
    /// string disables the cloud tier without flipping the policy
    /// bit. The PRD's `[mesh.planning.cloud] default_model` example
    /// names a model id that does not exist in the Anthropic API;
    /// this default tracks the current mid-tier model instead.
    #[serde(default = "default_cloud_planner_model")]
    pub cloud_planner_model: String,

    /// 5.3 (ADR-0032) — conditions under which the tier-3 Cloud
    /// planner may be attempted after an earlier LLM tier failed
    /// (PRD §15.2 `escalate_to_cloud_if`). Typed, so a typo fails
    /// policy load loudly (house rule) instead of silently narrowing
    /// escalation. The default covers every failure mode local tiers
    /// actually produce — the PRD's TOML example names only the
    /// first two; ADR-0032 documents why the exhaustive-gate reading
    /// would regress §15.3's "until one returns a confident,
    /// validated plan".
    #[serde(default = "default_escalate_to_cloud_if")]
    pub escalate_to_cloud_if: Vec<CloudTrigger>,

    /// 5.3 — how many REPAIR retries a tier gets after emitting a
    /// plan that failed validation (PRD §15.2 default 2; the cloud
    /// tier is further capped at 1 by the executor — paid retries).
    #[serde(default = "default_max_replanning_attempts")]
    pub max_replanning_attempts: u32,
}

/// Escalation-trigger vocabulary for `escalate_to_cloud_if` (5.3,
/// ADR-0032). Serde names are the PRD §15.2 strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudTrigger {
    /// An earlier tier emitted a `Confident` plan that failed
    /// `validate_plan`.
    PlanValidationFailed,
    /// An earlier tier matched but a capability was missing
    /// (`MatchedButUnsupported`), or validation failed with
    /// `UnknownCapability`/`UnknownSchema`.
    ToolNotFound,
    /// An earlier tier's `Confident` plan fell below the
    /// confidence threshold.
    LowConfidence,
    /// An earlier tier errored (transport / timeout / decode /
    /// internal).
    BackendError,
}

fn default_confidence_threshold() -> f64 {
    0.7
}
#[allow(clippy::unnecessary_wraps)]
fn default_max_cost_usd() -> Option<f64> {
    Some(1.0)
}
fn default_cloud_planner_model() -> String {
    "claude-sonnet-5".to_string()
}
fn default_escalate_to_cloud_if() -> Vec<CloudTrigger> {
    vec![
        CloudTrigger::PlanValidationFailed,
        CloudTrigger::ToolNotFound,
        CloudTrigger::LowConfidence,
        CloudTrigger::BackendError,
    ]
}
fn default_max_replanning_attempts() -> u32 {
    2
}

impl Default for PlanningPolicy {
    fn default() -> Self {
        Self {
            allow_cloud_escalation: false,
            local_only_for_tags: HashSet::new(),
            confidence_threshold: default_confidence_threshold(),
            prefer_local_models: Vec::new(),
            default_max_cost_usd: default_max_cost_usd(),
            cloud_planner_model: default_cloud_planner_model(),
            escalate_to_cloud_if: default_escalate_to_cloud_if(),
            max_replanning_attempts: default_max_replanning_attempts(),
        }
    }
}

/// `[llm]` policy section — Phase 3.4. Optional. When absent on the
/// parent `Policy`, `llm.local.*` actions default-allow. When present
/// but empty, default-deny (operator wrote the section; respect intent).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmPolicy {
    #[serde(default)]
    pub allow: Vec<LlmAllow>,

    #[serde(default)]
    pub deny: Vec<LlmAllow>,

    #[serde(default)]
    pub from: HashMap<String, TrustLevel>,
}

/// One `[llm].allow` (or `[llm].deny`) rule. Same untagged-with-
/// `deny_unknown_fields` discipline as `DenyRule` in shell, so a typo
/// surfaces at parse time instead of silently matching the wrong shape.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum LlmAllow {
    Model(LlmAllowModel),
    Prefix(LlmAllowPrefix),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmAllowModel {
    pub model: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmAllowPrefix {
    pub model_prefix: String,
}

/// `[mcp]` policy section — Phase 3.7 (`mcp.<server>.<tool>` proxy
/// capabilities). Deny pass runs first (declaration order, first match
/// wins), then the allow pass; anything unmatched is denied. An empty
/// section (or no section at all) therefore denies every MCP call —
/// same posture as shell (ADR-0018).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPolicy {
    #[serde(default)]
    pub allow: Vec<McpRule>,

    #[serde(default)]
    pub deny: Vec<McpRule>,

    #[serde(default)]
    pub from: HashMap<String, TrustLevel>,
}

/// One `[mcp].allow` / `[mcp].deny` rule. `server` is required; `tool`
/// is optional — omitted means "every tool on that server".
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpRule {
    pub server: String,

    #[serde(default)]
    pub tool: Option<String>,
}

impl Policy {
    /// Empty policy. Equivalent to `Policy::default()`. Documents intent:
    /// the empty policy denies all shell evaluation by virtue of having
    /// no allow rules.
    #[must_use]
    pub fn deny_all() -> Self {
        Self::default()
    }
}
