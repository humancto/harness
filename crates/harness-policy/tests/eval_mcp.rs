//! Phase 3.7 — `Action::Mcp` policy semantics (ADR-0018).
//!
//! MCP is default-deny like shell: no `[mcp]` section, or an empty
//! one, denies every `mcp.<server>.<tool>` call.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::needless_raw_string_hashes
)]

use harness_policy::{load_from_str, Action, Decision, EvalContext, PolicyEngine};

fn engine_from(toml: &str) -> PolicyEngine {
    PolicyEngine::new(load_from_str(toml).expect("policy parse"))
}

fn check(engine: &PolicyEngine, from: &str, server: &str, tool: &str) -> Decision {
    engine.evaluate(&EvalContext {
        from_node: from,
        local_node: "self",
        action: Action::Mcp { server, tool },
    })
}

#[test]
fn t01_no_mcp_section_default_denies() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "uname", any_args = true }]
"#,
    );
    let d = check(&e, "any", "fs", "read_file");
    let Decision::Deny { reason } = d else {
        panic!("must deny");
    };
    assert!(reason.contains("no [mcp].allow rule matched fs/read_file"));
}

#[test]
fn t02_empty_mcp_section_denies() {
    let e = engine_from(
        r#"
[mcp]
"#,
    );
    assert!(matches!(
        check(&e, "any", "fs", "read_file"),
        Decision::Deny { .. }
    ));
}

#[test]
fn t03_server_wide_allow_matches_every_tool() {
    let e = engine_from(
        r#"
[mcp]
allow = [{ server = "fs" }]
"#,
    );
    assert_eq!(check(&e, "any", "fs", "read_file"), Decision::Allow);
    assert_eq!(check(&e, "any", "fs", "write_file"), Decision::Allow);
    assert!(matches!(
        check(&e, "any", "gh", "read_file"),
        Decision::Deny { .. }
    ));
}

#[test]
fn t04_tool_scoped_allow_matches_only_that_tool() {
    let e = engine_from(
        r#"
[mcp]
allow = [{ server = "gh", tool = "search_code" }]
"#,
    );
    assert_eq!(check(&e, "any", "gh", "search_code"), Decision::Allow);
    assert!(matches!(
        check(&e, "any", "gh", "create_issue"),
        Decision::Deny { .. }
    ));
}

#[test]
fn t05_deny_wins_over_server_wide_allow() {
    let e = engine_from(
        r#"
[mcp]
allow = [{ server = "fs" }]
deny  = [{ server = "fs", tool = "delete_file" }]
"#,
    );
    assert_eq!(check(&e, "any", "fs", "read_file"), Decision::Allow);
    let d = check(&e, "any", "fs", "delete_file");
    let Decision::Deny { reason } = d else {
        panic!("must deny");
    };
    assert!(reason.contains("denied by [mcp].deny rule for fs/delete_file"));
}

#[test]
fn t06_untrusted_from_node_short_circuits() {
    let e = engine_from(
        r#"
[mcp]
allow = [{ server = "fs" }]

[mcp.from]
"laptop-evil" = "untrusted"
"#,
    );
    assert_eq!(check(&e, "laptop-ok", "fs", "read_file"), Decision::Allow);
    let d = check(&e, "laptop-evil", "fs", "read_file");
    let Decision::Deny { reason } = d else {
        panic!("must deny");
    };
    assert!(reason.contains("untrusted source node"));
}

#[test]
fn t07_wildcard_untrusted_from() {
    let e = engine_from(
        r#"
[mcp]
allow = [{ server = "fs" }]

[mcp.from]
"*" = "untrusted"
"#,
    );
    assert!(matches!(
        check(&e, "anyone", "fs", "read_file"),
        Decision::Deny { .. }
    ));
}

#[test]
fn t08_validation_rejects_empty_server() {
    let err = load_from_str(
        r#"
[mcp]
allow = [{ server = "" }]
"#,
    )
    .expect_err("must fail validation");
    assert!(err.to_string().contains("mcp.allow[0].server"));
}

#[test]
fn t09_validation_rejects_whitespace_tool() {
    let err = load_from_str(
        r#"
[mcp]
deny = [{ server = "fs", tool = "two words" }]
"#,
    )
    .expect_err("must fail validation");
    assert!(err.to_string().contains("mcp.deny[0].tool"));
}

#[test]
fn t10_unknown_field_in_rule_fails_parse() {
    assert!(load_from_str(
        r#"
[mcp]
allow = [{ server = "fs", tools = ["oops"] }]
"#,
    )
    .is_err());
}
