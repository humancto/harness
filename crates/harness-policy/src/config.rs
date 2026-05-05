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
}

fn default_confidence_threshold() -> f64 {
    0.7
}
#[allow(clippy::unnecessary_wraps)]
fn default_max_cost_usd() -> Option<f64> {
    Some(1.0)
}

impl Default for PlanningPolicy {
    fn default() -> Self {
        Self {
            allow_cloud_escalation: false,
            local_only_for_tags: HashSet::new(),
            confidence_threshold: default_confidence_threshold(),
            prefer_local_models: Vec::new(),
            default_max_cost_usd: default_max_cost_usd(),
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

impl Policy {
    /// Empty policy. Equivalent to `Policy::default()`. Documents intent:
    /// the empty policy denies all shell evaluation by virtue of having
    /// no allow rules.
    #[must_use]
    pub fn deny_all() -> Self {
        Self::default()
    }
}
