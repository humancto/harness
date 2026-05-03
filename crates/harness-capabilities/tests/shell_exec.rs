//! Integration tests for `shell.exec`. Unix-only — the capability is
//! `#![cfg(unix)]` and Windows port is Phase 6.10.

#![cfg(unix)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::needless_raw_string_hashes,
    clippy::items_after_statements,
    clippy::len_zero
)]

use std::sync::Arc;

use harness_capabilities::traits::{Capability, CapabilityError, ExecutionContext};
use harness_capabilities::ShellExecCapability;
use harness_core::{NodeId, TaskId};
use harness_policy::{load_from_str, Policy, PolicyEngine};
use serde_json::Value as JsonValue;

fn ctx() -> ExecutionContext {
    ExecutionContext {
        local_node: NodeId::from_bytes([1; 16]),
        local_node_name: Arc::from("self"),
        issued_by: NodeId::from_bytes([2; 16]),
        issued_by_name: Arc::from("issuer"),
        task_id: TaskId::new_v7(),
    }
}

fn cap_with(toml: &str) -> ShellExecCapability {
    let p = load_from_str(toml).expect("policy must parse");
    ShellExecCapability::new(Arc::new(PolicyEngine::new(p)))
}

fn cap_deny_all() -> ShellExecCapability {
    ShellExecCapability::new(Arc::new(PolicyEngine::new(Policy::deny_all())))
}

fn input(cmd: &str, args: &[&str]) -> JsonValue {
    serde_json::json!({
        "cmd":  cmd,
        "args": args,
    })
}

// ───────────────────────────────────────────── Allowed-path

#[tokio::test]
async fn t01_allowed_uname_returns_zero_exit() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "uname", any_args = true }]
"#,
    );
    let out = cap
        .execute(&ctx(), input("uname", &[]))
        .await
        .expect("execute");
    assert_eq!(out["exit_code"], serde_json::json!(0));
    assert_eq!(out["timed_out"], serde_json::json!(false));
    assert!(
        out["stdout"].as_str().unwrap_or("").len() > 0,
        "uname stdout should be non-empty"
    );
}

#[tokio::test]
async fn t02_allowed_echo_args_passed_through() {
    // /bin/echo so we get a real binary, not a shell builtin.
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/bin/echo", any_args = true }]
"#,
    );
    let out = cap
        .execute(&ctx(), input("/bin/echo", &["hello", "world"]))
        .await
        .expect("execute");
    assert_eq!(out["exit_code"], serde_json::json!(0));
    assert_eq!(out["stdout"].as_str().unwrap(), "hello world\n");
}

// ───────────────────────────────────────────── Denied-path

#[tokio::test]
async fn t04_policy_deny_all_denies_uname() {
    let cap = cap_deny_all();
    let err = cap
        .execute(&ctx(), input("uname", &[]))
        .await
        .expect_err("must deny");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(msg.contains("policy denied"), "got: {msg}");
}

#[tokio::test]
async fn t05_policy_allows_uname_denies_cat() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "uname", any_args = true }]
"#,
    );
    let err = cap
        .execute(&ctx(), input("cat", &[]))
        .await
        .expect_err("must deny");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(msg.contains("policy denied"));
    assert!(msg.contains("cat"));
}

#[tokio::test]
async fn t06_pattern_deny_blocks_argv() {
    // We never actually spawn — policy gate fires first.
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "rm", any_args = true }]
deny  = [{ pattern = "rm -rf" }]
"#,
    );
    let err = cap
        .execute(
            &ctx(),
            input("rm", &["-rf", "/tmp/safe-not-actually-deleted"]),
        )
        .await
        .expect_err("must deny");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed");
    };
    assert!(msg.contains("policy denied"));
    assert!(msg.contains("rm -rf"));
}

// ───────────────────────────────────────────── Failure modes

#[tokio::test]
async fn t07_nonzero_exit_returned() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/usr/bin/false" }]
"#,
    );
    let out = cap
        .execute(&ctx(), input("/usr/bin/false", &[]))
        .await
        .expect("execute");
    assert_eq!(out["exit_code"], serde_json::json!(1));
    assert_eq!(out["timed_out"], serde_json::json!(false));
}

#[tokio::test]
async fn t08_command_not_found_is_failed() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/nonexistent-binary-abc123" }]
"#,
    );
    let err = cap
        .execute(&ctx(), input("/nonexistent-binary-abc123", &[]))
        .await
        .expect_err("must fail");
    let CapabilityError::Failed(msg) = err else {
        panic!("expected Failed, got {err:?}");
    };
    assert!(msg.contains("spawn failed"), "got: {msg}");
}

#[tokio::test]
async fn t09_timeout_kills_and_sets_flag() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/bin/sleep", any_args = true }]
"#,
    );
    let out = cap
        .execute(
            &ctx(),
            serde_json::json!({
                "cmd":  "/bin/sleep",
                "args": ["60"],
                "timeout_ms": 200,
            }),
        )
        .await
        .expect("execute");
    assert_eq!(out["timed_out"], serde_json::json!(true));
    assert_eq!(out["exit_code"], serde_json::json!(null));
    assert_eq!(out["signal"].as_str().unwrap(), "SIGKILL");
}

#[tokio::test]
async fn t09b_timeout_killed_pid_no_longer_alive() {
    let dir = tempdir();
    let pidfile = dir.path().join("pid");
    let pidfile_str = pidfile.to_str().unwrap().to_string();

    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/bin/sh", any_args = true }]
"#,
    );
    let script = format!("echo $$ > {pidfile_str}; sleep 60");
    let out = cap
        .execute(
            &ctx(),
            serde_json::json!({
                "cmd":  "/bin/sh",
                "args": ["-c", script],
                "timeout_ms": 300,
            }),
        )
        .await
        .expect("execute");
    assert_eq!(out["timed_out"], serde_json::json!(true));

    // Read PID, give the kernel a moment to reap, then assert it's dead.
    let pid_str = std::fs::read_to_string(&pidfile).expect("read pidfile");
    let pid: i32 = pid_str.trim().parse().expect("parse pid");
    // Brief settle for the kernel to finish reaping descendants — we only
    // SIGKILL the direct child but sh will exit too.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    match kill(Pid::from_raw(pid), None) {
        Err(nix::errno::Errno::ESRCH) => {} // dead, expected
        Ok(()) => panic!("pid {pid} still alive after timeout"),
        Err(e) => panic!("unexpected errno {e}"),
    }
}

#[tokio::test]
async fn t09c_partial_output_preserved_on_timeout() {
    // Regression fence for showstopper S1: dropping wait_with_output on
    // timeout would lose all stdout. This test proves we keep it.
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/bin/sh", any_args = true }]
"#,
    );
    let out = cap
        .execute(
            &ctx(),
            serde_json::json!({
                "cmd":  "/bin/sh",
                "args": ["-c", "echo line1; sleep 60"],
                "timeout_ms": 300,
            }),
        )
        .await
        .expect("execute");
    assert_eq!(out["timed_out"], serde_json::json!(true));
    assert_eq!(
        out["stdout"].as_str().unwrap(),
        "line1\n",
        "partial stdout must survive timeout"
    );
}

#[tokio::test]
async fn t10_cwd_must_exist() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "uname" }]
"#,
    );
    let err = cap
        .execute(
            &ctx(),
            serde_json::json!({
                "cmd": "uname",
                "cwd": "/nonexistent-dir-xyz-12345",
            }),
        )
        .await
        .expect_err("must reject");
    let CapabilityError::InvalidInput(msg) = err else {
        panic!("expected InvalidInput, got {err:?}");
    };
    assert!(msg.contains("cwd"));
}

#[tokio::test]
async fn t11_invalid_env_key_rejected() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/bin/echo", any_args = true }]
"#,
    );
    let mut env = std::collections::HashMap::new();
    env.insert("FOO=BAR".to_string(), "v".to_string());
    let err = cap
        .execute(
            &ctx(),
            serde_json::json!({
                "cmd":  "/bin/echo",
                "args": ["x"],
                "env":  env,
            }),
        )
        .await
        .expect_err("must reject");
    let CapabilityError::InvalidInput(msg) = err else {
        panic!("expected InvalidInput, got {err:?}");
    };
    assert!(msg.contains("FOO=BAR"));
}

// ───────────────────────────────────────────── Env hygiene

#[tokio::test]
async fn t12_env_clean_by_default() {
    // Set a leak var on the host process. The child should NOT see it.
    std::env::set_var("HARNESS_TEST_LEAK_T12", "leaked-value");

    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/usr/bin/printenv", any_args = true }]
"#,
    );
    let out = cap
        .execute(
            &ctx(),
            input("/usr/bin/printenv", &["HARNESS_TEST_LEAK_T12"]),
        )
        .await
        .expect("execute");
    // printenv exits non-zero (1) when var is missing, and prints nothing.
    assert_eq!(out["exit_code"], serde_json::json!(1));
    assert_eq!(out["stdout"].as_str().unwrap(), "");
}

#[tokio::test]
async fn t13_env_allowlist_passed_through() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/usr/bin/printenv", any_args = true }]
"#,
    );
    let mut env = std::collections::HashMap::new();
    env.insert("HARNESS_TEST_T13".to_string(), "bar".to_string());
    let out = cap
        .execute(
            &ctx(),
            serde_json::json!({
                "cmd":  "/usr/bin/printenv",
                "args": ["HARNESS_TEST_T13"],
                "env":  env,
            }),
        )
        .await
        .expect("execute");
    assert_eq!(out["exit_code"], serde_json::json!(0));
    assert_eq!(out["stdout"].as_str().unwrap(), "bar\n");
}

#[tokio::test]
async fn t13b_user_env_overrides_base() {
    // Spawn /usr/bin/printenv by absolute path so we don't need PATH
    // resolution. Override PATH via input.env; printenv should report
    // the overridden value.
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/usr/bin/printenv", any_args = true }]
"#,
    );
    let mut env = std::collections::HashMap::new();
    env.insert("PATH".to_string(), "/totally/fake".to_string());
    let out = cap
        .execute(
            &ctx(),
            serde_json::json!({
                "cmd":  "/usr/bin/printenv",
                "args": ["PATH"],
                "env":  env,
            }),
        )
        .await
        .expect("execute");
    assert_eq!(out["exit_code"], serde_json::json!(0));
    assert_eq!(out["stdout"].as_str().unwrap(), "/totally/fake\n");
}

// ───────────────────────────────────────────── Output limits

#[tokio::test]
async fn t14_stdout_truncates_at_max_bytes() {
    // Use the test-only constructor with a tiny cap. Generate ~4 KiB
    // of output via /bin/sh. 4096 bytes minus 256 cap = 3840 extra.
    let p = load_from_str(
        r#"
[shell]
allow = [{ cmd = "/bin/sh", any_args = true }]
"#,
    )
    .expect("policy");
    let cap = ShellExecCapability::with_max_output(Arc::new(PolicyEngine::new(p)), 256);

    let out = cap
        .execute(
            &ctx(),
            serde_json::json!({
                "cmd":  "/bin/sh",
                "args": ["-c", "head -c 4096 /dev/zero | tr '\\0' a"],
            }),
        )
        .await
        .expect("execute");
    let stdout = out["stdout"].as_str().unwrap();
    assert_eq!(stdout.len(), 256, "stdout should be exactly 256 bytes");
    assert_eq!(out["stdout_truncated_bytes"], serde_json::json!(3840));
    assert_eq!(out["stderr_truncated_bytes"], serde_json::json!(0));
}

// ───────────────────────────────────────────── Stdin

#[tokio::test]
async fn t15_stdin_null() {
    // /bin/cat with no args reads stdin until EOF. With Stdio::null()
    // it should see immediate EOF, exit 0, empty stdout.
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/bin/cat" }]
"#,
    );
    let out = cap
        .execute(&ctx(), input("/bin/cat", &[]))
        .await
        .expect("execute");
    assert_eq!(out["exit_code"], serde_json::json!(0));
    assert_eq!(out["stdout"].as_str().unwrap(), "");
}

// ───────────────────────────────────────────── Manifest

#[test]
fn t16_manifest_advertises_anyone_local_fast_shell_tag() {
    let cap = cap_deny_all();
    let m = cap.manifest();
    assert_eq!(m.id, "shell.exec");
    assert!(matches!(m.cardinality, harness_core::Cardinality::Anyone));
    assert!(matches!(
        m.cost_hint,
        harness_core::protocol::CostHint::LocalFast
    ));
    assert!(m.tags.iter().any(|t| t == "shell"));
}

// ───────────────────────────────────────────── fd-leak smoke (Phase 6)

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "enabled in Phase 6 hardening"]
async fn t17_fd_leak_smoke() {
    let cap = cap_with(
        r#"
[shell]
allow = [{ cmd = "/usr/bin/true" }]
"#,
    );
    let before = std::fs::read_dir("/proc/self/fd").unwrap().count();
    for _ in 0..100 {
        let _ = cap.execute(&ctx(), input("/usr/bin/true", &[])).await;
    }
    let after = std::fs::read_dir("/proc/self/fd").unwrap().count();
    assert!(
        after - before < 10,
        "fd leak: before={before} after={after}"
    );
}

// ───────────────────────────────────────────── Tempdir helper

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
        "harness-shell-test-{}-{}",
        std::process::id(),
        nanos
    ));
    std::fs::create_dir_all(&dir).unwrap();
    Tempdir(dir)
}
