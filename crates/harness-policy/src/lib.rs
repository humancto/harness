//! Policy engine for capability allow/deny.
//!
//! See PRD §10.4. The engine parses `~/.harness/policy.toml` into a
//! typed `Policy`, evaluates `Action`s against it on the executing
//! node (brains cannot override worker policy), and supports lock-free
//! reload via `arc_swap::ArcSwap`.

pub mod config;
pub mod engine;
pub mod error;
mod validate;

pub use config::{
    CapabilityPolicy, DenyCmd, DenyPattern, DenyRule, LlmAllow, LlmAllowModel, LlmAllowPrefix,
    LlmPolicy, PlanningPolicy, Policy, ShellAllow, ShellPolicy, TrustLevel,
};
pub use engine::{
    default_path, load_default_path, load_from_path, load_from_str, Action, Decision, EvalContext,
    PolicyEngine,
};
pub use error::PolicyError;

// Compile-time guarantee that the engine + policy are safe to share
// across executor tasks.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PolicyEngine>();
    assert_send_sync::<Policy>();
    assert_send_sync::<Decision>();
};
