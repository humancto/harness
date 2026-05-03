//! `PolicyEngine` — the runtime side. Holds the parsed `Policy` behind
//! `ArcSwap` so `evaluate` is a lock-free atomic load on the hot path,
//! and `reload` is a single atomic store with no readers blocked.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::{
    config::{DenyRule, Policy, ShellAllow, TrustLevel},
    error::PolicyError,
    validate,
};

/// Decision returned by `PolicyEngine::evaluate`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
#[non_exhaustive]
pub enum Decision {
    Allow,
    Deny { reason: String },
}

/// What the executor is asking permission for. New variants are added
/// alongside their evaluators.
///
/// **All variants must remain `Copy`.** `EvalContext` is cheap to share
/// across evaluator branches; a non-`Copy` variant would require a
/// rewrite of every match site. Use `&'a str` / `&'a [T]`, not
/// `String` / `Vec<T>`.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Action<'a> {
    /// Per-command shell gate, evaluated on the executing node before
    /// `shell.exec` invokes the command.
    Shell { cmd: &'a str, args: &'a [String] },

    /// Per-model LLM gate, evaluated on the executing node before
    /// `llm.local.<model>` (3.4) or `llm.cloud.<provider>` (3.6)
    /// invokes the underlying inference engine.
    Llm { model: &'a str },
}

/// Per-call evaluation context. Cheap to build; borrows everything.
#[derive(Debug, Clone)]
pub struct EvalContext<'a> {
    pub from_node: &'a str,
    pub local_node: &'a str,
    pub action: Action<'a>,
}

#[derive(Debug)]
pub struct PolicyEngine {
    inner: ArcSwap<Policy>,
    snapshot_path: Option<PathBuf>,
}

impl PolicyEngine {
    /// Build an engine from an in-memory policy. `reload()` will fail
    /// with a `PolicyError::Validate` because there is no backing file.
    pub fn new(policy: Policy) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(policy)),
            snapshot_path: None,
        }
    }

    /// Build an engine that remembers the path it was loaded from, so
    /// `reload()` can re-read it.
    pub fn from_policy_at(path: PathBuf, policy: Policy) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(policy)),
            snapshot_path: Some(path),
        }
    }

    /// Try the default `~/.harness/policy.toml`. The daemon-startup path
    /// catches `Err(Io)` with `NotFound` and falls back to
    /// `Self::new(Policy::deny_all())` — parse / validate errors are not
    /// swallowed, they abort startup.
    pub fn try_from_default_path() -> Result<Self, PolicyError> {
        let path = default_path()?;
        let policy = load_from_path(&path)?;
        Ok(Self::from_policy_at(path, policy))
    }

    /// Lock-free hot-path read.
    pub fn evaluate(&self, ctx: &EvalContext<'_>) -> Decision {
        let snapshot = self.inner.load();
        evaluate_against(&snapshot, ctx)
    }

    /// Re-read the snapshot path, parse + validate, then atomically
    /// publish. Concurrent `evaluate` callers are never blocked.
    pub fn reload(&self) -> Result<(), PolicyError> {
        let Some(path) = self.snapshot_path.as_ref() else {
            return Err(PolicyError::Validate {
                errors: vec![
                    "no snapshot path; engine was constructed without a backing file".to_string(),
                ],
            });
        };
        let policy = load_from_path(path)?;
        self.inner.store(Arc::new(policy));
        Ok(())
    }

    /// Cheap snapshot of the current policy. Useful for diagnostics
    /// and tests.
    #[must_use]
    pub fn snapshot(&self) -> Arc<Policy> {
        self.inner.load_full()
    }
}

/// Resolve `~/.harness/policy.toml`. Errors if `$HOME` cannot be
/// resolved (covered by `PolicyError::NoHome`).
pub fn default_path() -> Result<PathBuf, PolicyError> {
    let home = dirs::home_dir().ok_or(PolicyError::NoHome)?;
    Ok(home.join(".harness").join("policy.toml"))
}

/// Parse a `Policy` from a string and run post-parse validation.
pub fn load_from_str(s: &str) -> Result<Policy, PolicyError> {
    let policy: Policy = toml::from_str(s)?;
    validate::validate(&policy)?;
    Ok(policy)
}

/// Read, parse, and validate a `Policy` at the given path.
pub fn load_from_path(path: &Path) -> Result<Policy, PolicyError> {
    let text = std::fs::read_to_string(path)?;
    load_from_str(&text)
}

/// Convenience around `load_from_path(default_path()?)`.
pub fn load_default_path() -> Result<Policy, PolicyError> {
    load_from_path(&default_path()?)
}

fn evaluate_against(policy: &Policy, ctx: &EvalContext<'_>) -> Decision {
    match ctx.action {
        Action::Shell { cmd, args } => evaluate_shell(policy, ctx.from_node, cmd, args),
        Action::Llm { model } => evaluate_llm(policy, ctx.from_node, model),
    }
}

fn evaluate_shell(policy: &Policy, from_node: &str, cmd: &str, args: &[String]) -> Decision {
    // Step A: trust short-circuit.
    let trust = resolve_trust(&policy.shell.from, from_node);
    if matches!(trust, TrustLevel::Untrusted) {
        return Decision::Deny {
            reason: format!("untrusted source node {from_node:?}"),
        };
    }

    // Step B: deny pass (declaration order, first match wins).
    for rule in &policy.shell.deny {
        if deny_matches(rule, cmd, args) {
            return Decision::Deny {
                reason: deny_reason(rule, cmd),
            };
        }
    }

    // Step C: allow pass.
    for rule in &policy.shell.allow {
        if allow_matches(rule, cmd, args) {
            return Decision::Allow;
        }
    }

    Decision::Deny {
        reason: format!("no allow rule matched {cmd}"),
    }
}

fn resolve_trust(
    from: &std::collections::HashMap<String, TrustLevel>,
    from_node: &str,
) -> TrustLevel {
    if let Some(level) = from.get(from_node) {
        return *level;
    }
    if let Some(level) = from.get("*") {
        return *level;
    }
    TrustLevel::Default
}

fn deny_matches(rule: &DenyRule, cmd: &str, args: &[String]) -> bool {
    match rule {
        DenyRule::Cmd(d) => cmd == d.cmd && (d.any_args || args.is_empty()),
        DenyRule::Pattern(p) => {
            // Substring match over `cmd + " " + args.join(" ")`. NOT a
            // regex — we deliberately avoid ReDoS surface. NOT word-
            // anchored — `pattern = "rm"` matches `cmd = "charm"`. This
            // is documented and tested.
            let mut canonical = String::with_capacity(cmd.len() + 1);
            canonical.push_str(cmd);
            for a in args {
                canonical.push(' ');
                canonical.push_str(a);
            }
            canonical.contains(&p.pattern)
        }
    }
}

fn deny_reason(rule: &DenyRule, cmd: &str) -> String {
    match rule {
        DenyRule::Cmd(d) => format!("denied by shell.deny.cmd ({:?})", d.cmd),
        DenyRule::Pattern(p) => {
            format!(
                "denied by shell.deny.pattern ({:?}) for cmd {cmd}",
                p.pattern
            )
        }
    }
}

fn allow_matches(rule: &ShellAllow, cmd: &str, args: &[String]) -> bool {
    if cmd != rule.cmd {
        return false;
    }
    if !rule.subcmds.is_empty() {
        let Some(first) = args.first() else {
            return false;
        };
        return rule.subcmds.iter().any(|s| s == first);
    }
    if rule.any_args {
        return true;
    }
    args.is_empty()
}

// ─────────────────────────────────────────── LLM evaluation (3.4)

/// Evaluate `Action::Llm { model }` against `policy.llm` per the matrix:
///
/// | `[llm]` section | `allow` rules                  | Result          |
/// |-----------------|--------------------------------|-----------------|
/// | absent          | n/a                            | **Allow**       |
/// | present, empty  | `allow=[]` and `deny=[]`       | **Deny**        |
/// | present         | match-based                    | first match     |
///
/// Default-allow when `[llm]` is absent matches the PRD §10.4 example,
/// which assumes ollama works without explicit configuration.
fn evaluate_llm(policy: &Policy, from_node: &str, model: &str) -> Decision {
    let Some(llm) = policy.llm.as_ref() else {
        // Section absent → default-allow.
        return Decision::Allow;
    };

    // Trust short-circuit (symmetric with shell).
    let trust = resolve_trust(&llm.from, from_node);
    if matches!(trust, TrustLevel::Untrusted) {
        return Decision::Deny {
            reason: format!("untrusted source node {from_node:?}"),
        };
    }

    // Section present, both lists empty → default-deny (operator wrote
    // the section; respect the intent).
    if llm.allow.is_empty() && llm.deny.is_empty() {
        return Decision::Deny {
            reason: format!("[llm] section present but no rules; denying {model}"),
        };
    }

    // Deny pass first, declaration order.
    for rule in &llm.deny {
        if llm_rule_matches(rule, model) {
            return Decision::Deny {
                reason: format!("denied by [llm].deny rule for model {model:?}"),
            };
        }
    }

    // Allow pass.
    for rule in &llm.allow {
        if llm_rule_matches(rule, model) {
            return Decision::Allow;
        }
    }

    // No rule matched in a non-empty allow list.
    Decision::Deny {
        reason: format!("no [llm].allow rule matched {model}"),
    }
}

fn llm_rule_matches(rule: &crate::config::LlmAllow, model: &str) -> bool {
    match rule {
        crate::config::LlmAllow::Model(m) => m.model == model,
        crate::config::LlmAllow::Prefix(p) => model.starts_with(&p.model_prefix),
    }
}
