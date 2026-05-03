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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningPolicy {
    #[serde(default)]
    pub allow_cloud_escalation: bool,

    #[serde(default)]
    pub local_only_for_tags: HashSet<String>,
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
