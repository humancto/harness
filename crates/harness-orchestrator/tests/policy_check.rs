//! Integration test for `harness_orchestrator::executor::policy_check`.
//! Exercises the full lifetime story: build owned strings, borrow into
//! `EvalContext`, call into the orchestrator's shim.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use harness_orchestrator::{policy_check, ExecutorError};
use harness_policy::{load_from_str, Action, EvalContext, PolicyEngine};

fn build_engine(toml: &str) -> PolicyEngine {
    PolicyEngine::new(load_from_str(toml).expect("policy must parse"))
}

#[test]
fn allowed_action_returns_ok() {
    let engine = build_engine(
        r#"
[shell]
allow = [{ cmd = "uname", any_args = true }]
"#,
    );
    // Owned strings constructed elsewhere — borrowed into EvalContext
    // as the dispatcher will when 3.2 lands.
    let from_node = String::from("macbook-archy");
    let local_node = String::from("server-01");
    let cmd = String::from("uname");
    let args = vec![String::from("-a")];

    let ctx = EvalContext {
        from_node: &from_node,
        local_node: &local_node,
        action: Action::Shell {
            cmd: &cmd,
            args: &args,
        },
    };

    assert_eq!(policy_check(&engine, &ctx), Ok(()));
}

#[test]
fn denied_action_returns_policy_denied_with_reason() {
    let engine = build_engine(
        r#"
[shell]
allow = [{ cmd = "ls", any_args = true }]
deny  = [{ pattern = "rm -rf" }]
"#,
    );
    let from_node = String::from("any");
    let local_node = String::from("self");
    let cmd = String::from("rm");
    let args = vec![String::from("-rf"), String::from("/tmp/x")];
    let ctx = EvalContext {
        from_node: &from_node,
        local_node: &local_node,
        action: Action::Shell {
            cmd: &cmd,
            args: &args,
        },
    };

    let err = policy_check(&engine, &ctx).expect_err("must deny");
    let ExecutorError::PolicyDenied { reason } = err else {
        panic!("expected PolicyDenied, got {err:?}");
    };
    assert!(reason.contains("rm -rf"), "got reason: {reason}");
}

#[test]
fn no_matching_allow_rule_denies_with_descriptive_reason() {
    let engine = build_engine("[shell]\n");
    let from_node = String::from("any");
    let local_node = String::from("self");
    let cmd = String::from("anything");
    let args: Vec<String> = vec![];
    let ctx = EvalContext {
        from_node: &from_node,
        local_node: &local_node,
        action: Action::Shell {
            cmd: &cmd,
            args: &args,
        },
    };
    let err = policy_check(&engine, &ctx).expect_err("must deny");
    let ExecutorError::PolicyDenied { reason } = err else {
        panic!("expected PolicyDenied, got {err:?}");
    };
    assert!(reason.contains("no allow rule matched"));
    assert!(reason.contains("anything"));
}
