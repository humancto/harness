//! Parse + validate behaviour. Tests numbered to match the plan.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::items_after_statements
)]

use harness_policy::{load_from_path, load_from_str, Policy, PolicyError};

const PRD_EXAMPLE: &str = r#"
[shell]
allow = [
  { cmd = "ls", any_args = true },
  { cmd = "git", subcmds = ["status", "log", "diff", "pull", "fetch"] },
  { cmd = "ollama", any_args = true },
  { cmd = "cargo", subcmds = ["build", "test", "check"] },
]
deny = [
  { pattern = "rm -rf /" },
  { cmd = "sudo", any_args = true },
]

[shell.from]
"macbook-archy" = "trusted"
"*"             = "default"

[capability]
default_local_only = false
require_2fa_for    = ["shell.exec", "fs.write"]

[planning]
allow_cloud_escalation = true
local_only_for_tags    = ["medical", "legal"]
"#;

#[test]
fn t01_prd_example_round_trips() {
    let p = load_from_str(PRD_EXAMPLE).expect("PRD §10.4 example must parse");
    assert_eq!(p.shell.allow.len(), 4);
    assert_eq!(p.shell.deny.len(), 2);
    assert_eq!(p.shell.from.len(), 2);
    assert_eq!(p.capability.require_2fa_for.len(), 2);
    assert!(p.capability.require_2fa_for.contains("shell.exec"));
    assert!(p.planning.allow_cloud_escalation);
    assert!(p.planning.local_only_for_tags.contains("medical"));
}

#[test]
fn t11_empty_cmd_aggregates_to_validate() {
    let toml = r#"
[shell]
allow = [{ cmd = "" }]
deny  = [{ pattern = "" }]
"#;
    let err = load_from_str(toml).unwrap_err();
    let PolicyError::Validate { errors } = err else {
        panic!("expected Validate, got {err:?}");
    };
    assert!(errors
        .iter()
        .any(|e| e.contains("shell.allow[0].cmd is empty")));
    assert!(errors
        .iter()
        .any(|e| e.contains("shell.deny[0].pattern is empty")));
}

#[test]
fn t12_whitespace_in_cmd_rejected_with_hint() {
    let toml = r#"
[shell]
allow = [{ cmd = "git push" }]
"#;
    let err = load_from_str(toml).unwrap_err();
    let PolicyError::Validate { errors } = err else {
        panic!("expected Validate, got {err:?}");
    };
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("whitespace"));
    assert!(errors[0].contains("subcmds"));
}

#[test]
fn t13_unknown_trust_level_is_parse_error() {
    let toml = r#"
[shell.from]
"node-a" = "semi-trusted"
"#;
    let err = load_from_str(toml).unwrap_err();
    assert!(
        matches!(err, PolicyError::Parse(_)),
        "expected Parse, got {err:?}"
    );
}

#[test]
fn t20_aggregated_validation_errors() {
    let toml = r#"
[shell]
allow = [
  { cmd = "" },
  { cmd = "ls", subcmds = [""] },
  { cmd = "git push" },
]
deny = [
  { pattern = "" },
]
"#;
    let err = load_from_str(toml).unwrap_err();
    let PolicyError::Validate { errors } = err else {
        panic!("expected Validate, got {err:?}");
    };
    assert!(
        errors.len() >= 4,
        "expected aggregated errors, got {errors:?}"
    );
}

#[test]
fn t21_typo_in_deny_field_surfaces_as_parse_error() {
    let toml = r#"
[shell]
deny = [{ patern = "rm -rf /" }]
"#;
    let err = load_from_str(toml).unwrap_err();
    assert!(
        matches!(err, PolicyError::Parse(_)),
        "expected Parse from deny_unknown_fields, got {err:?}"
    );
}

#[test]
fn deny_all_is_default() {
    // Explicit semantics: empty policy parses; carries no allow rules.
    let p = Policy::deny_all();
    assert!(p.shell.allow.is_empty());
    assert!(p.shell.deny.is_empty());
}

#[test]
fn t17_reload_picks_up_new_rule() {
    use std::io::Write;
    let dir = tempdir();
    let path = dir.path().join("policy.toml");

    std::fs::write(
        &path,
        r#"
[shell]
allow = [{ cmd = "ls", any_args = true }]
"#,
    )
    .unwrap();
    let policy = load_from_path(&path).unwrap();
    let engine = harness_policy::PolicyEngine::from_policy_at(path.clone(), policy);

    // Sanity: ls allowed, uname denied.
    use harness_policy::{Action, Decision, EvalContext};
    let ctx = |cmd: &'static str, args: &'static [&'static str]| {
        let owned: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        (cmd, owned)
    };
    let (c1, a1) = ctx("uname", &["-a"]);
    let d = engine.evaluate(&EvalContext {
        from_node: "x",
        local_node: "y",
        action: Action::Shell { cmd: c1, args: &a1 },
    });
    assert!(matches!(d, Decision::Deny { .. }));

    // Rewrite file and reload.
    let mut f = std::fs::File::create(&path).unwrap();
    write!(
        f,
        r#"
[shell]
allow = [
  {{ cmd = "ls", any_args = true }},
  {{ cmd = "uname", any_args = true }},
]
"#
    )
    .unwrap();
    drop(f);
    engine.reload().expect("reload must succeed");

    let (c2, a2) = ctx("uname", &["-a"]);
    let d = engine.evaluate(&EvalContext {
        from_node: "x",
        local_node: "y",
        action: Action::Shell { cmd: c2, args: &a2 },
    });
    assert_eq!(d, Decision::Allow);
}

#[test]
fn engine_no_path_reload_returns_validate_error() {
    let engine = harness_policy::PolicyEngine::new(Policy::deny_all());
    let err = engine.reload().unwrap_err();
    let PolicyError::Validate { errors } = err else {
        panic!("expected Validate, got {err:?}");
    };
    assert!(errors[0].contains("no snapshot path"));
}

// Tiny tempdir helper — kept here to avoid pulling in `tempfile` as a dev
// dep just for one test. We use $TMPDIR + a random pid+nanos suffix.
struct Tempdir(std::path::PathBuf);

impl Drop for Tempdir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl Tempdir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

fn tempdir() -> Tempdir {
    let base = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = base.join(format!(
        "harness-policy-test-{}-{}",
        std::process::id(),
        nanos
    ));
    std::fs::create_dir_all(&dir).unwrap();
    Tempdir(dir)
}
