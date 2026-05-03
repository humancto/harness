//! Phase 3.4 — `Action::Llm` policy semantics.

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

fn check(engine: &PolicyEngine, from: &str, model: &str) -> Decision {
    engine.evaluate(&EvalContext {
        from_node: from,
        local_node: "self",
        action: Action::Llm { model },
    })
}

#[test]
fn t01_no_llm_section_default_allows() {
    let e = engine_from(
        r#"
[shell]
allow = [{ cmd = "uname", any_args = true }]
"#,
    );
    assert_eq!(check(&e, "any", "llama3:latest"), Decision::Allow);
}

#[test]
fn t02_explicit_allow_with_model_match() {
    let e = engine_from(
        r#"
[llm]
allow = [{ model = "llama3:latest" }]
"#,
    );
    assert_eq!(check(&e, "any", "llama3:latest"), Decision::Allow);
    let d = check(&e, "any", "mistral:7b");
    let Decision::Deny { reason } = d else {
        panic!("must deny");
    };
    assert!(reason.contains("no [llm].allow rule matched"));
}

#[test]
fn t03_explicit_allow_with_model_prefix() {
    let e = engine_from(
        r#"
[llm]
allow = [{ model_prefix = "mistral:" }]
"#,
    );
    assert_eq!(check(&e, "any", "mistral:7b-instruct"), Decision::Allow);
    assert!(matches!(
        check(&e, "any", "llama3:latest"),
        Decision::Deny { .. }
    ));
}

#[test]
fn t04_deny_precedence() {
    let e = engine_from(
        r#"
[llm]
allow = [{ model_prefix = "l"}]
deny  = [{ model = "leaked-secrets:v1" }]
"#,
    );
    let d = check(&e, "any", "leaked-secrets:v1");
    let Decision::Deny { reason } = d else {
        panic!("must deny");
    };
    assert!(reason.contains("denied by [llm].deny"));
}

#[test]
fn t05_untrusted_short_circuits() {
    let e = engine_from(
        r#"
[llm]
allow = [{ model_prefix = "l"}]

[llm.from]
hostile = "untrusted"
"#,
    );
    let d = check(&e, "hostile", "any-model");
    let Decision::Deny { reason } = d else {
        panic!("must deny untrusted");
    };
    assert!(reason.contains("untrusted"));
}

#[test]
fn t06_default_deny_when_section_present_and_empty() {
    let e = engine_from(
        r#"
[llm]
allow = []
deny  = []
"#,
    );
    let d = check(&e, "any", "llama3:latest");
    let Decision::Deny { reason } = d else {
        panic!("must deny when section is present-but-empty");
    };
    assert!(reason.contains("present but no rules"));
}

#[test]
fn t07_default_allow_when_section_absent() {
    let e = engine_from("");
    assert_eq!(check(&e, "any", "llama3:latest"), Decision::Allow);
}
