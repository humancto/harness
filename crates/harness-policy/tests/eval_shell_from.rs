//! `[shell.from]` lookup precedence and Untrusted short-circuit.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use harness_policy::{load_from_str, Action, Decision, EvalContext, PolicyEngine};

fn engine_from(toml: &str) -> PolicyEngine {
    PolicyEngine::new(load_from_str(toml).unwrap())
}

fn shell_ctx(from: &str) -> EvalContext<'_> {
    EvalContext {
        from_node: from,
        local_node: "self",
        action: Action::Shell {
            cmd: "ls",
            args: &[],
        },
    }
}

#[test]
fn t14_exact_match_wins_over_wildcard() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "ls" }]

[shell.from]
"macbook-archy" = "trusted"
"*"             = "default"
"#,
    );
    // macbook-archy → Trusted (allowed by allow list)
    assert_eq!(e.evaluate(&shell_ctx("macbook-archy")), Decision::Allow);
    // anyone-else → Default (also passes through to allow list)
    assert_eq!(e.evaluate(&shell_ctx("other-node")), Decision::Allow);
}

#[test]
fn t15_untrusted_short_circuits_to_deny() {
    // The allow list would otherwise permit `ls`, but Untrusted is
    // checked before the allow pass.
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "ls", any_args = true }]

[shell.from]
"hostile" = "untrusted"
"#,
    );
    let d = e.evaluate(&shell_ctx("hostile"));
    let Decision::Deny { reason } = d else {
        panic!("must deny untrusted source");
    };
    assert!(reason.contains("untrusted"));
}

#[test]
fn star_wildcard_alone_applies_to_unknowns() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "ls" }]

[shell.from]
"*" = "untrusted"
"#,
    );
    let d = e.evaluate(&shell_ctx("any-stranger"));
    assert!(matches!(d, Decision::Deny { .. }));
}

#[test]
fn no_from_section_means_default_trust() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "ls" }]
"#,
    );
    assert_eq!(e.evaluate(&shell_ctx("anyone")), Decision::Allow);
}
