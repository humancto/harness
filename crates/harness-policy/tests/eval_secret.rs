//! Phase 3.6a evaluator stub: `Action::Secret` always returns
//! `Decision::Allow`. The test pins the contract so a future
//! 3.6-encrypted change explicitly updates this case.

use harness_policy::{Action, Decision, EvalContext, Policy, PolicyEngine};

#[test]
fn t11_action_secret_returns_allow() {
    let engine = PolicyEngine::new(Policy::deny_all());
    let decision = engine.evaluate(&EvalContext {
        from_node: "self",
        local_node: "self",
        action: Action::Secret {
            tag: "secret/claude-api-key",
        },
    });
    assert_eq!(
        decision,
        Decision::Allow,
        "Phase 3.6a: Action::Secret must be Allow even under deny-all (ADR-0012)"
    );
}

#[test]
fn t11b_action_secret_unknown_tag_returns_allow() {
    // Defense-in-depth: even an unknown tag returns Allow in 3.6a.
    // The actual unknown-tag handling happens at the SecretsStore
    // (returns None), not the policy gate.
    let engine = PolicyEngine::new(Policy::deny_all());
    let decision = engine.evaluate(&EvalContext {
        from_node: "self",
        local_node: "self",
        action: Action::Secret {
            tag: "secret/never-configured",
        },
    });
    assert_eq!(decision, Decision::Allow);
}
