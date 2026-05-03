//! Shell action evaluation: allow/deny ordering, semantics, footguns.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::needless_raw_string_hashes
)]

use harness_policy::{load_from_str, Action, Decision, EvalContext, Policy, PolicyEngine};

fn engine_from(toml: &str) -> PolicyEngine {
    PolicyEngine::new(load_from_str(toml).unwrap())
}

fn shell_ctx<'a>(from: &'a str, cmd: &'a str, args: &'a [String]) -> EvalContext<'a> {
    EvalContext {
        from_node: from,
        local_node: "self",
        action: Action::Shell { cmd, args },
    }
}

fn args(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

#[test]
fn t02_demo_gate_uname_with_any_args() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "uname", any_args = true }]
"#,
    );
    let a = args(&["-a"]);
    assert_eq!(
        e.evaluate(&shell_ctx("any", "uname", &a)),
        Decision::Allow,
        "Phase 3 demo gate must allow uname -a"
    );
}

#[test]
fn t03_deny_all_denies_anything() {
    let e = PolicyEngine::new(Policy::deny_all());
    let a = args(&[]);
    assert!(matches!(
        e.evaluate(&shell_ctx("any", "ls", &a)),
        Decision::Deny { .. }
    ));
}

#[test]
fn t04_any_args_allows_flags() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "ls", any_args = true }]
"#,
    );
    let a = args(&["-la", "/tmp"]);
    assert_eq!(e.evaluate(&shell_ctx("x", "ls", &a)), Decision::Allow);
}

#[test]
fn t05_subcmds_allow_only_listed() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "git", subcmds = ["status", "log"] }]
"#,
    );
    assert_eq!(
        e.evaluate(&shell_ctx("x", "git", &args(&["status"]))),
        Decision::Allow
    );
    let denied = e.evaluate(&shell_ctx("x", "git", &args(&["push"])));
    let Decision::Deny { reason } = denied else {
        panic!("git push must deny");
    };
    assert!(reason.contains("git"));
}

#[test]
fn t06_no_subcmds_no_any_args_means_zero_args_only() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "uname" }]
"#,
    );
    assert_eq!(
        e.evaluate(&shell_ctx("x", "uname", &args(&[]))),
        Decision::Allow
    );
    assert!(matches!(
        e.evaluate(&shell_ctx("x", "uname", &args(&["-a"]))),
        Decision::Deny { .. }
    ));
}

#[test]
fn t07_deny_runs_before_allow() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "rm", any_args = true }]
deny  = [{ pattern = "rm -rf /" }]
"#,
    );
    let a = args(&["-rf", "/tmp/foo"]);
    let d = e.evaluate(&shell_ctx("x", "rm", &a));
    let Decision::Deny { reason } = d else {
        panic!("must deny");
    };
    assert!(reason.contains("rm -rf /"));
}

#[test]
fn t08_pattern_substring_unanchored_footgun_pinned() {
    // Foot-gun: `pattern = "rm"` matches cmd = "charm". Pinned by test
    // so a future "fix" to glob/regex/word-boundary doesn't silently
    // change semantics. PRD §10.4 reads as substring; if we ever change
    // this, the PRD changes first and this test fails loudly.
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "charm", any_args = true }]
deny  = [{ pattern = "rm" }]
"#,
    );
    let a = args(&[]);
    assert!(
        matches!(
            e.evaluate(&shell_ctx("x", "charm", &a)),
            Decision::Deny { .. }
        ),
        "pattern is unanchored substring; this test pins the footgun"
    );
}

#[test]
fn t09_literal_dotstar_does_not_match_everything() {
    // A user pasting `pattern = ".*"` thinking it's a regex catches NOTHING
    // unless the literal sequence ".*" appears in the command line.
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "ls", any_args = true }]
deny  = [{ pattern = ".*" }]
"#,
    );
    let a = args(&["-l"]);
    assert_eq!(e.evaluate(&shell_ctx("x", "ls", &a)), Decision::Allow);
}

#[test]
fn t10_no_rules_yields_no_allow_rule_matched_reason() {
    let e = engine_from("[shell]\n");
    let a = args(&[]);
    let d = e.evaluate(&shell_ctx("x", "anything", &a));
    let Decision::Deny { reason } = d else {
        panic!("must deny");
    };
    assert!(reason.contains("no allow rule matched"));
    assert!(reason.contains("anything"));
}

#[test]
fn t18_rule_order_first_match_wins() {
    // Two allow rules that could both match `git status`. Allow with
    // any_args=true comes FIRST → wins. Verifies declaration-order
    // semantics is stable.
    let e = engine_from(
        r#"
[shell]
allow = [
  { cmd = "git", any_args = true },
  { cmd = "git", subcmds = ["push"] },
]
"#,
    );
    let a = args(&["status"]);
    assert_eq!(e.evaluate(&shell_ctx("x", "git", &a)), Decision::Allow);
}

#[test]
fn t19_empty_lists_default_deny() {
    let e = engine_from(
        r#"
[shell]
allow = []
deny  = []
"#,
    );
    let a = args(&[]);
    assert!(matches!(
        e.evaluate(&shell_ctx("x", "ls", &a)),
        Decision::Deny { .. }
    ));
}
