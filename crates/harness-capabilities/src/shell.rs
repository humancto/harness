//! `shell.exec` — run a single command (no shell parsing) on the local
//! node, gated by the policy engine. PRD §10.4 + roadmap item 3.2.
//!
//! This is the synchronous form. The streaming form (line-frames over
//! QUIC) is the follow-up `3.2-stream` item — see ADR-0008. The
//! `read_capped` reader here is forward-compatible: replacing the
//! buffer with a frame emitter is a localized change.
//!
//! Disclosure surface: the host's `PATH` (basenames findable via PATH)
//! plus `HOME`, `USER`, `LANG`, `LC_*`, `TMPDIR`. Anything in
//! `input.env` overrides those.
//!
//! Process group: SIGKILL on timeout kills only the direct child, not
//! its descendants. Phase 6 adds `setsid` + `killpg`.

#![cfg(unix)]

use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, Cardinality, SemVer};
use harness_policy::{Action, Decision, EvalContext, PolicyEngine};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::traits::{Capability, CapabilityError, ExecutionContext};

pub const SHELL_EXEC_ID: &str = "shell.exec";

/// Per-stream cap. Enforced *during* read; a runaway child cannot OOM
/// the daemon because we drain-and-count past the cap.
pub const MAX_OUTPUT_BYTES: usize = 1 << 20; // 1 MiB

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 600_000;

/// Base env allowlist preserved when `env_clear()`-ing the child.
/// Anything in `input.env` overrides these.
const BASE_ENV_KEYS: &[&str] = &[
    "PATH", "HOME", "USER", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR",
];

/// `shell.exec` — `Anyone`, `LocalFast`, tag `"shell"`. Policy file is
/// the real gate (PRD §10.4).
///
/// Construct with `ShellExecCapability::new(policy)`. The capability
/// holds an `Arc<PolicyEngine>` (cheap clone, lock-free reads) so each
/// `execute` call is one atomic load + one linear-scan against the
/// allow/deny lists.
#[derive(Clone, Debug)]
pub struct ShellExecCapability {
    policy: Arc<PolicyEngine>,
    max_output_bytes: usize,
}

impl ShellExecCapability {
    #[must_use]
    pub fn new(policy: Arc<PolicyEngine>) -> Self {
        Self {
            policy,
            max_output_bytes: MAX_OUTPUT_BYTES,
        }
    }

    /// Test-only constructor that overrides the per-stream output cap.
    /// Production paths use the 1 MiB constant; tests use a small cap to
    /// keep test output tiny while still exercising the truncation path.
    #[doc(hidden)]
    #[must_use]
    pub fn with_max_output(policy: Arc<PolicyEngine>, max: usize) -> Self {
        Self {
            policy,
            max_output_bytes: max,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShellExecInput {
    cmd: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
struct ShellExecOutput {
    /// Process exit code. `None` if the process was killed by signal
    /// (e.g. SIGKILL on timeout).
    exit_code: Option<i32>,
    /// Captured stdout (UTF-8, lossy decoded if needed). Capped at
    /// `MAX_OUTPUT_BYTES`; consult `stdout_truncated_bytes` for whether
    /// truncation happened.
    stdout: String,
    stdout_truncated_bytes: u64,
    stderr: String,
    stderr_truncated_bytes: u64,
    duration_ms: u64,
    timed_out: bool,
    /// Signal name for unix processes killed by signal, e.g. `"SIGKILL"`.
    signal: Option<String>,
}

#[async_trait]
impl Capability for ShellExecCapability {
    fn id(&self) -> &str {
        SHELL_EXEC_ID
    }

    fn manifest(&self) -> ManifestEntry {
        ManifestEntry {
            id: SHELL_EXEC_ID.to_string(),
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            cardinality: Cardinality::Anyone,
            input_schema: serde_json::json!({
                "type": "object",
                "required": ["cmd"],
                "properties": {
                    "cmd":        { "type": "string", "minLength": 1 },
                    "args":       { "type": "array", "items": { "type": "string" } },
                    "timeout_ms": { "type": "integer", "minimum": 1 },
                    "cwd":        { "type": "string" },
                    "env":        { "type": "object", "additionalProperties": { "type": "string" } },
                },
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "required": [
                    "exit_code", "stdout", "stdout_truncated_bytes",
                    "stderr",    "stderr_truncated_bytes",
                    "duration_ms", "timed_out", "signal",
                ],
            }),
            cost_hint: CostHint::LocalFast,
            tags: vec!["shell".to_string()],
            rate_limit: Some(RateLimit {
                per_second: 5,
                burst: 10,
            }),
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: NetworkClass::None,
                disk_io_class: DiskIoClass::Light,
                estimated_duration_ms: None,
            },
        }
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        let input: ShellExecInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;

        if input.cmd.is_empty() {
            return Err(CapabilityError::InvalidInput("cmd is empty".to_string()));
        }

        // Policy gate. Inline — no orchestrator dep (would create a
        // layering cycle with capabilities). Translate Decision::Deny
        // directly into CapabilityError::Failed.
        let decision = self.policy.evaluate(&EvalContext {
            from_node: ctx.issued_by_name.as_ref(),
            local_node: ctx.local_node_name.as_ref(),
            action: Action::Shell {
                cmd: &input.cmd,
                args: &input.args,
            },
        });
        match decision {
            Decision::Allow => {}
            Decision::Deny { reason } => {
                return Err(CapabilityError::Failed(format!("policy denied: {reason}")));
            }
            // `Decision` is `#[non_exhaustive]`; future variants we
            // don't yet know how to translate must fail closed.
            _ => {
                return Err(CapabilityError::Failed(
                    "policy returned unknown decision (fail-closed)".to_string(),
                ));
            }
        }

        let output = self.execute_shell(input).await?;
        serde_json::to_value(&output)
            .map_err(|e| CapabilityError::Failed(format!("encode output: {e}")))
    }
}

impl ShellExecCapability {
    async fn execute_shell(
        &self,
        input: ShellExecInput,
    ) -> Result<ShellExecOutput, CapabilityError> {
        let timeout_ms = input
            .timeout_ms
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .min(MAX_TIMEOUT_MS);
        let deadline = Duration::from_millis(timeout_ms);

        let mut cmd = Command::new(&input.cmd);
        cmd.args(&input.args);
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        // kill_on_drop is the cancellation path (lease eviction drops
        // the future). start_kill on timeout is the deadline path.
        cmd.kill_on_drop(true);

        // Clean env, then re-inject base allowlist, then user env.
        // User env wins because we apply it last.
        cmd.env_clear();
        for k in BASE_ENV_KEYS {
            if let Some(v) = std::env::var_os(k) {
                cmd.env(k, v);
            }
        }
        if let Some(env) = &input.env {
            for (k, v) in env {
                validate_env_key(k)?;
                cmd.env(k, v);
            }
        }

        if let Some(cwd) = &input.cwd {
            let p = std::path::Path::new(cwd);
            let canonical = std::fs::canonicalize(p).map_err(|e| {
                CapabilityError::InvalidInput(format!("cwd {cwd:?} canonicalize failed: {e}"))
            })?;
            if !canonical.is_dir() {
                return Err(CapabilityError::InvalidInput(format!(
                    "cwd not a directory: {cwd}"
                )));
            }
            cmd.current_dir(canonical);
        }

        let started = Instant::now();
        let mut child = cmd
            .spawn()
            .map_err(|e| CapabilityError::Failed(format!("spawn failed: {e}")))?;

        // Read stdout/stderr concurrently with child.wait(). If we
        // serialized, the child could fill the pipe (~64 KiB on Linux)
        // and block on write while we wait — classic pipe deadlock.
        // We piped both streams above; `take()` returns `Some` here.
        // If somehow `None`, fall back to a no-op reader rather than
        // panic — reads complete immediately with empty bytes.
        let stdout_pipe = child.stdout.take().ok_or_else(|| {
            CapabilityError::Failed("stdout pipe missing after spawn".to_string())
        })?;
        let stderr_pipe = child.stderr.take().ok_or_else(|| {
            CapabilityError::Failed("stderr pipe missing after spawn".to_string())
        })?;
        let cap = self.max_output_bytes;
        let stdout_task = tokio::spawn(read_capped(stdout_pipe, cap));
        let stderr_task = tokio::spawn(read_capped(stderr_pipe, cap));

        let mut timed_out = false;
        let status = tokio::select! {
            biased;
            s = child.wait() => s.map_err(|e| {
                CapabilityError::Failed(format!("wait failed: {e}"))
            })?,
            () = tokio::time::sleep(deadline) => {
                timed_out = true;
                // start_kill returns Err(InvalidInput) if the child
                // already exited — we don't care.
                let _ = child.start_kill();
                child.wait().await.map_err(|e| {
                    CapabilityError::Failed(format!("wait-after-kill failed: {e}"))
                })?
            }
        };

        // The reader tasks complete naturally when the kernel closes
        // the pipe fds at child exit. No explicit cancellation needed.
        let (stdout_bytes, stdout_extra) = stdout_task
            .await
            .map_err(|e| CapabilityError::Failed(format!("stdout reader join: {e}")))??;
        let (stderr_bytes, stderr_extra) = stderr_task
            .await
            .map_err(|e| CapabilityError::Failed(format!("stderr reader join: {e}")))??;

        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

        let signal = status
            .signal()
            .and_then(|s| signal_name(s).map(str::to_string));
        let exit_code = status.code();

        Ok(ShellExecOutput {
            exit_code,
            stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
            stdout_truncated_bytes: stdout_extra,
            stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
            stderr_truncated_bytes: stderr_extra,
            duration_ms,
            timed_out,
            signal,
        })
    }
}

/// Read up to `cap` bytes from `r`. Past `cap`, drain-and-count so the
/// child doesn't block on a full pipe. Returns `(kept, extra_bytes)`.
async fn read_capped<R: tokio::io::AsyncRead + Unpin>(
    mut r: R,
    cap: usize,
) -> Result<(Vec<u8>, u64), CapabilityError> {
    let mut kept = Vec::with_capacity(std::cmp::min(cap, 8192));
    let mut buf = [0u8; 8192];
    let mut extra: u64 = 0;
    loop {
        let n = r
            .read(&mut buf)
            .await
            .map_err(|e| CapabilityError::Failed(format!("read: {e}")))?;
        if n == 0 {
            break;
        }
        if kept.len() < cap {
            let space = cap - kept.len();
            let take = std::cmp::min(space, n);
            kept.extend_from_slice(&buf[..take]);
            if take < n {
                extra += (n - take) as u64;
            }
        } else {
            extra += n as u64;
        }
    }
    Ok((kept, extra))
}

fn validate_env_key(k: &str) -> Result<(), CapabilityError> {
    let valid = !k.is_empty()
        && k.chars().enumerate().all(|(i, c)| {
            (i == 0 && (c.is_ascii_alphabetic() || c == '_'))
                || (i > 0 && (c.is_ascii_alphanumeric() || c == '_'))
        });
    if !valid {
        return Err(CapabilityError::InvalidInput(format!(
            "invalid env key {k:?}; must match [A-Za-z_][A-Za-z0-9_]*"
        )));
    }
    Ok(())
}

fn signal_name(sig: i32) -> Option<&'static str> {
    Some(match sig {
        1 => "SIGHUP",
        2 => "SIGINT",
        3 => "SIGQUIT",
        9 => "SIGKILL",
        13 => "SIGPIPE",
        14 => "SIGALRM",
        15 => "SIGTERM",
        _ => return None,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn validate_env_key_accepts_posix_names() {
        assert!(validate_env_key("FOO").is_ok());
        assert!(validate_env_key("_foo").is_ok());
        assert!(validate_env_key("FOO_BAR_42").is_ok());
    }

    #[test]
    fn validate_env_key_rejects_invalid() {
        assert!(validate_env_key("").is_err());
        assert!(validate_env_key("FOO=BAR").is_err());
        assert!(validate_env_key("FOO BAR").is_err());
        assert!(validate_env_key("FOO\0BAR").is_err());
        assert!(validate_env_key("1FOO").is_err()); // leading digit
    }

    #[test]
    fn signal_name_known_values() {
        assert_eq!(signal_name(9), Some("SIGKILL"));
        assert_eq!(signal_name(15), Some("SIGTERM"));
        assert_eq!(signal_name(99), None);
    }
}
