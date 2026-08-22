//! `harness` CLI subcommand surface.
//!
//! Defines the [`Cli`] struct (`clap` derive) plus a [`run`] dispatcher
//! the daemon invokes after parsing. Subcommands shipped in 1.9:
//!
//! - `init [--root PATH]` — generate identity if absent, print pairing
//!   code (10 min TTL).
//! - `join <addr> --code NNNN-NNNN [--root PATH]` — pair with a host.
//! - `peers [--root PATH]` — list peers from `peers.toml`.
//! - `status [--root PATH]` — print local node id + pubkey fingerprint
//!   + version.
//! - `leave [--root PATH]` — placeholder (Phase 6 hardening).
//!
//! `init` and `status` are wired end-to-end against
//! `harness_mesh::identity` and `harness_mesh::pairing`. `join` parses
//! the address and code; the actual transport dial is wired by
//! `harness-daemon` in this same PR. `peers` reads `peers.toml` via
//! `harness_mesh::TrustStore`. `leave` is a placeholder that prints
//! a "not implemented" message — the on-disk teardown is Phase 6.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use harness_mesh::pairing::PairingCode;
use harness_mesh::{identity, PendingPairings, TrustStore};

pub mod query;
pub mod run;
pub use run::{run_run, RunOutcome};

/// Top-level harness CLI.
#[derive(Debug, Parser)]
#[command(name = "harness", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// `harness <subcommand> ...`
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Generate identity (if absent) and print a fresh pairing code.
    Init(InitArgs),
    /// Join an existing mesh by dialing a host and presenting a pairing code.
    Join(JoinArgs),
    /// List peers in `~/.harness/peers.toml`.
    Peers(PeersArgs),
    /// Print local node info: `node_id`, pubkey fingerprint, version.
    Status(StatusArgs),
    /// Run the long-lived daemon: HTTP/WS API on 127.0.0.1:19198, mesh
    /// peers, embedded UI. Blocks until SIGINT/SIGTERM.
    Daemon(DaemonArgs),
    /// Admin commands (set-password, etc.).
    Admin(AdminArgs),
    /// Submit a task to the local daemon.
    Submit(SubmitArgs),
    /// List submitted tasks.
    Tasks(TasksArgs),
    /// Run a shell command on this node (Phase 3.3a).
    /// `harness run -- uname -a`. `--all`/`--on <node>`/`--where` are
    /// parsed; cross-node dispatch lands in 3.3-fanout.
    Run(RunArgs),
    /// Federated full-text search across every node's indexed scopes
    /// (`mesh.search`, Phase 3.11).
    Search(QueryArgs),
    /// Federated grep across every node's scopes (`mesh.grep`, 3.11).
    Grep(QueryArgs),
    /// Placeholder — clean teardown of `~/.harness/` is a Phase 6 concern.
    Leave(LeaveArgs),
}

/// Args shared by `harness search` and `harness grep`.
#[derive(Debug, clap::Args)]
pub struct QueryArgs {
    /// The query (search) or pattern (grep).
    pub term: String,
    /// Treat the grep pattern as a literal string, not a regex.
    #[arg(long)]
    pub literal: bool,
    /// Case-insensitive matching (grep).
    #[arg(long)]
    pub ignore_case: bool,
    /// Overall federated timeout in milliseconds (default 30000).
    #[arg(long)]
    pub timeout_ms: Option<u64>,
    /// Max ranked hits to return (search).
    #[arg(long)]
    pub limit: Option<u64>,
    #[arg(long)]
    pub root: Option<std::path::PathBuf>,
    /// Daemon API base URL.
    #[arg(long, default_value = "http://127.0.0.1:19198")]
    pub api: String,
}

#[derive(Debug, clap::Args)]
pub struct InitArgs {
    /// Override the default `~/.harness/` root.
    #[arg(long)]
    pub root: Option<PathBuf>,
    /// Pairing-code TTL (default 10 minutes per PRD §9.2).
    #[arg(long, default_value = "600")]
    pub ttl_seconds: u64,
}

#[derive(Debug, clap::Args)]
pub struct JoinArgs {
    /// Host's QUIC address (e.g. `192.168.1.42:19198`).
    pub addr: SocketAddr,
    /// Pairing code printed by `harness init` on the host.
    #[arg(long)]
    pub code: String,
    #[arg(long)]
    pub root: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct PeersArgs {
    #[arg(long)]
    pub root: Option<PathBuf>,
    /// Emit JSON instead of the human table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, clap::Args)]
pub struct StatusArgs {
    #[arg(long)]
    pub root: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct LeaveArgs {
    #[arg(long)]
    pub root: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct AdminArgs {
    #[command(subcommand)]
    pub command: AdminCommand,
}

#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// Set (or replace) the admin password. Prompts via stdin (no echo).
    SetPassword(AdminSetPasswordArgs),
}

#[derive(Debug, clap::Args)]
pub struct AdminSetPasswordArgs {
    #[arg(long)]
    pub root: Option<PathBuf>,
    /// Read the password from stdin instead of prompting (for scripts).
    /// Reads exactly one line, trims trailing newline.
    #[arg(long)]
    pub from_stdin: bool,
}

#[derive(Debug, clap::Args)]
pub struct SubmitArgs {
    #[arg(long)]
    pub root: Option<PathBuf>,
    /// Capability id, e.g. `echo`, `shell.exec`, `mesh.search`.
    pub capability: String,
    /// Inline JSON input, or `@<path>` to read from a file, or `@-` for
    /// stdin. Defaults to `{}`.
    #[arg(long, default_value = "{}")]
    pub input: String,
    /// API base URL. Defaults to `http://127.0.0.1:19198`.
    #[arg(long, default_value = "http://127.0.0.1:19198")]
    pub api: String,
}

#[derive(Debug, clap::Args)]
pub struct TasksArgs {
    #[arg(long)]
    pub root: Option<PathBuf>,
    /// API base URL.
    #[arg(long, default_value = "http://127.0.0.1:19198")]
    pub api: String,
}

/// `harness run [--on <node>] [--timeout-ms N] [--cwd P] -- <cmd> <args>...`
#[derive(Debug, clap::Args)]
#[command(group(
    clap::ArgGroup::new("target").args(["all", "on", "where_"]).multiple(false)
))]
pub struct RunArgs {
    /// Run on every reachable node. Lands in 3.3-fanout.
    #[arg(long)]
    pub all: bool,

    /// Run on a specific node by hostname (or `self`).
    #[arg(long)]
    pub on: Option<String>,

    /// Predicate over node tags. Lands in 3.3-fanout.
    #[arg(long = "where")]
    pub where_: Option<String>,

    /// Capability execution timeout in milliseconds. Default `60_000`.
    #[arg(long)]
    pub timeout_ms: Option<u64>,

    /// Working directory for the command. Canonicalized client-side so
    /// relative paths resolve from the user's cwd, not the daemon's.
    #[arg(long)]
    pub cwd: Option<PathBuf>,

    #[arg(long)]
    pub root: Option<PathBuf>,

    #[arg(long, default_value = "http://127.0.0.1:19198")]
    pub api: String,

    /// `--` then the command and args.
    #[arg(last = true, required = true, num_args = 1..)]
    pub argv: Vec<String>,
}

#[derive(Debug, clap::Args)]
pub struct DaemonArgs {
    #[arg(long)]
    pub root: Option<PathBuf>,
    /// Override `[api].bind_addr` from `config.toml`. Defaults to
    /// 127.0.0.1:19198. Bind 0.0.0.0:* explicitly to expose the UI on
    /// the LAN — by default only loopback is bound for safety.
    #[arg(long, default_value = "127.0.0.1:19198", env = "HARNESS_API_BIND")]
    pub bind: SocketAddr,
}

/// Resolve the harness root: explicit `--root` wins, otherwise
/// `~/.harness/`.
pub(crate) fn resolve_root(root: Option<PathBuf>) -> Result<PathBuf> {
    match root {
        Some(p) => Ok(p),
        None => identity::default_root().context("locate ~/.harness/"),
    }
}

/// Dispatch to a synchronous subcommand handler. Returns the
/// human-readable stdout. The `Daemon` subcommand returns
/// [`SyncOutcome::DaemonRequested`] so the binary can hand off to an
/// async runtime; all other variants resolve to a printable string.
pub fn run(cli: Cli) -> Result<SyncOutcome> {
    match cli.command {
        Command::Init(args) => cmd_init(args).map(SyncOutcome::Print),
        Command::Join(args) => cmd_join(args).map(SyncOutcome::Print),
        Command::Peers(args) => cmd_peers(args).map(SyncOutcome::Print),
        Command::Status(args) => cmd_status(args).map(SyncOutcome::Print),
        Command::Leave(args) => cmd_leave(args).map(SyncOutcome::Print),
        Command::Admin(args) => cmd_admin(args).map(SyncOutcome::Print),
        Command::Submit(args) => Ok(SyncOutcome::SubmitRequested(args)),
        Command::Tasks(args) => Ok(SyncOutcome::TasksRequested(args)),
        Command::Run(args) => Ok(SyncOutcome::RunRequested(args)),
        Command::Search(args) => Ok(SyncOutcome::SearchRequested(args)),
        Command::Grep(args) => Ok(SyncOutcome::GrepRequested(args)),
        Command::Daemon(args) => Ok(SyncOutcome::DaemonRequested(args)),
    }
}

/// Result of the synchronous CLI dispatch. The daemon subcommand returns
/// its parsed args so the caller can pull up the async runtime; all
/// other subcommands return their printable stdout.
#[derive(Debug)]
pub enum SyncOutcome {
    Print(String),
    DaemonRequested(DaemonArgs),
    SubmitRequested(SubmitArgs),
    TasksRequested(TasksArgs),
    RunRequested(RunArgs),
    SearchRequested(QueryArgs),
    GrepRequested(QueryArgs),
}

fn cmd_init(args: InitArgs) -> Result<String> {
    let root = resolve_root(args.root)?;
    let id = identity::init_or_load(&root).context("init/load identity")?;
    let pending = PendingPairings::new();
    let code: PairingCode = pending.issue(Duration::from_secs(args.ttl_seconds));
    Ok(format!(
        "Identity:    {}\nPubkey FP:   {}\nPairing code: {}\nTTL:          {}s\n\
         Run on the joiner: harness join <THIS_HOST_ADDR> --code {}",
        id.node_id(),
        id.public_key().fingerprint_hex(),
        code,
        args.ttl_seconds,
        code,
    ))
}

fn cmd_join(args: JoinArgs) -> Result<String> {
    let _root = resolve_root(args.root)?;
    let _code: PairingCode = args
        .code
        .parse()
        .context("pairing code must be 8 digits (NNNN-NNNN)")?;
    // The actual QUIC dial + PairingRequest send is wired in
    // `harness-daemon`'s integration of this CLI (the daemon owns the
    // tokio runtime and the Transport). Here we validate inputs so a
    // typo'd CLI invocation fails fast without spinning up the runtime.
    Ok(format!(
        "Validated join request for {} with code {}\n\
         (transport dial wiring lives in harness-daemon — Phase 1.9 ships the CLI surface)",
        args.addr, args.code
    ))
}

fn cmd_peers(args: PeersArgs) -> Result<String> {
    let root = resolve_root(args.root)?;
    let id = identity::load(&root).context("load identity")?;
    let store = TrustStore::open(&root, id.node_id()).context("open peers.toml")?;
    let peers = store.all_peers();
    if args.json {
        // Hand-roll a tiny JSON since serde_json on Peer needs the
        // PeerOnDisk wrapper which is pub(crate).
        let mut out = String::from("[");
        for (i, p) in peers.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&format!(
                "{{\"node_id\":\"{}\",\"hostname\":\"{}\",\"tier\":\"{:?}\"}}",
                p.node_id, p.hostname, p.tier
            ));
        }
        out.push(']');
        return Ok(out);
    }
    if peers.is_empty() {
        return Ok("(no peers yet — pair with `harness join` to add one)".to_string());
    }
    let mut out = String::from(
        "NODE_ID                          HOSTNAME              TIER\n\
         -------------------------------- --------------------- --------\n",
    );
    for p in peers {
        out.push_str(&format!(
            "{:<32} {:<21} {:?}\n",
            p.node_id, p.hostname, p.tier
        ));
    }
    Ok(out)
}

fn cmd_status(args: StatusArgs) -> Result<String> {
    let root = resolve_root(args.root)?;
    let id = identity::load(&root).context("load identity")?;
    Ok(format!(
        "Node ID:     {}\nPubkey FP:   {}\nVersion:     {}\nRoot:        {}\n",
        id.node_id(),
        id.public_key().fingerprint_hex(),
        env!("CARGO_PKG_VERSION"),
        root.display(),
    ))
}

fn cmd_leave(args: LeaveArgs) -> Result<String> {
    let root = resolve_root(args.root)?;
    Ok(format!(
        "Leave is not yet implemented (Phase 6 hardening).\n\
         For now, manually remove the root if you want to start fresh:\n\
           rm -rf {}",
        root.display()
    ))
}

fn cmd_admin(args: AdminArgs) -> Result<String> {
    match args.command {
        AdminCommand::SetPassword(a) => cmd_admin_set_password(a),
    }
}

fn cmd_admin_set_password(args: AdminSetPasswordArgs) -> Result<String> {
    let root = resolve_root(args.root)?;
    let password = if args.from_stdin {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)
            .context("read password from stdin")?;
        buf.trim_end_matches(['\r', '\n']).to_string()
    } else {
        rpassword::prompt_password("New admin password: ").context("read password")?
    };
    if password.is_empty() {
        anyhow::bail!("password cannot be empty");
    }
    if !args.from_stdin {
        let confirm = rpassword::prompt_password("Confirm: ").context("read confirm password")?;
        if confirm != password {
            anyhow::bail!("passwords did not match");
        }
    }
    harness_mesh::admin::set_password(&root, &password).context("save admin.toml")?;
    Ok(format!(
        "Admin password set. ({}/admin.toml)\nLog in with: harness submit ... (interactive)",
        root.display(),
    ))
}

/// Read or prompt for a password (CLI-side). Used by `submit` / `tasks`
/// to acquire a session before calling the API.
pub(crate) fn read_password_for_login() -> Result<String> {
    rpassword::prompt_password("Admin password: ").context("read password")
}

/// Path to the cached session file (`<root>/.session`).
pub(crate) fn session_path(root: &std::path::Path) -> std::path::PathBuf {
    root.join(".session")
}

/// Read the cached session token, if present + non-stale. Stale (file
/// mtime older than ~30d) is treated as absent so we re-login.
pub(crate) fn read_cached_session(root: &std::path::Path) -> Option<String> {
    let path = session_path(root);
    let bytes = std::fs::read(&path).ok()?;
    let token = String::from_utf8(bytes).ok()?.trim().to_string();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

/// Persist a freshly-issued session token. Mode 0600 on Unix.
pub(crate) fn write_cached_session(root: &std::path::Path, token: &str) -> Result<()> {
    let path = session_path(root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)?;
        std::io::Write::write_all(&mut f, token.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&path, token)?;
    }
    Ok(())
}

/// Run the `submit` command async — needs an HTTP client.
pub async fn run_submit(args: SubmitArgs) -> Result<String> {
    let root = resolve_root(args.root)?;
    let token = obtain_session_token(&root, &args.api).await?;

    // Resolve --input: inline JSON | @path | @-
    let input_str = match args.input.as_str() {
        "@-" => {
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)
                .context("read input from stdin")?;
            buf
        }
        s if s.starts_with('@') => std::fs::read_to_string(&s[1..])
            .with_context(|| format!("read input file {}", &s[1..]))?,
        s => s.to_string(),
    };
    let input: serde_json::Value =
        serde_json::from_str(input_str.trim()).context("input must be valid JSON")?;

    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/tasks", args.api.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "capability": args.capability,
            "input": input,
        }))
        .send()
        .await
        .context("submit POST failed")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("submit failed: {status} — {body}");
    }
    Ok(body)
}

/// Run the `tasks` command async — needs an HTTP client.
pub async fn run_tasks(args: TasksArgs) -> Result<String> {
    let root = resolve_root(args.root)?;
    let token = obtain_session_token(&root, &args.api).await?;

    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/tasks", args.api.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .context("tasks GET failed")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("tasks list failed: {status} — {body}");
    }
    Ok(body)
}

pub(crate) async fn obtain_session_token(root: &std::path::Path, api: &str) -> Result<String> {
    if let Some(cached) = read_cached_session(root) {
        return Ok(cached);
    }
    let password = read_password_for_login()?;
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/auth/login", api.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "password": password }))
        .send()
        .await
        .context("login POST failed")?;
    if !resp.status().is_success() {
        anyhow::bail!("login failed: HTTP {}", resp.status());
    }
    let body: serde_json::Value = resp.json().await.context("parse login response")?;
    let token = body["token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("login response missing token field"))?
        .to_string();
    if let Err(e) = write_cached_session(root, &token) {
        tracing::warn!(target: "harness.cli", ?e, "could not cache session");
    }
    Ok(token)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use tempfile::TempDir;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_init_with_no_args() {
        let cli = Cli::try_parse_from(["harness", "init"]).expect("parse");
        match cli.command {
            Command::Init(args) => {
                assert!(args.root.is_none());
                assert_eq!(args.ttl_seconds, 600);
            }
            _ => panic!("wrong subcommand"),
        }
    }

    #[test]
    fn parses_init_with_root_and_ttl() {
        let cli =
            Cli::try_parse_from(["harness", "init", "--root", "/tmp/x", "--ttl-seconds", "60"])
                .expect("parse");
        match cli.command {
            Command::Init(args) => {
                assert_eq!(args.root.as_deref(), Some(std::path::Path::new("/tmp/x")));
                assert_eq!(args.ttl_seconds, 60);
            }
            _ => panic!("wrong subcommand"),
        }
    }

    #[test]
    fn parses_join() {
        let cli =
            Cli::try_parse_from(["harness", "join", "127.0.0.1:19198", "--code", "1234-5678"])
                .expect("parse");
        match cli.command {
            Command::Join(args) => {
                assert_eq!(args.addr.to_string(), "127.0.0.1:19198");
                assert_eq!(args.code, "1234-5678");
            }
            _ => panic!("wrong subcommand"),
        }
    }

    #[test]
    fn parses_peers_with_json() {
        let cli = Cli::try_parse_from(["harness", "peers", "--json"]).expect("parse");
        match cli.command {
            Command::Peers(args) => assert!(args.json),
            _ => panic!("wrong subcommand"),
        }
    }

    #[test]
    fn parses_status() {
        let cli = Cli::try_parse_from(["harness", "status"]).expect("parse");
        assert!(matches!(cli.command, Command::Status(_)));
    }

    #[test]
    fn cmd_init_creates_identity_and_prints_pairing_code() {
        let tmp = TempDir::new().expect("tempdir");
        let out = cmd_init(InitArgs {
            root: Some(tmp.path().to_path_buf()),
            ttl_seconds: 60,
        })
        .expect("init");
        assert!(out.contains("Identity:"));
        assert!(out.contains("Pubkey FP:"));
        assert!(out.contains("Pairing code:"));
        assert!(tmp.path().join("identity.key").exists());
    }

    #[test]
    fn cmd_status_after_init_prints_node_id() {
        let tmp = TempDir::new().expect("tempdir");
        let _ = cmd_init(InitArgs {
            root: Some(tmp.path().to_path_buf()),
            ttl_seconds: 60,
        })
        .expect("init");
        let status = cmd_status(StatusArgs {
            root: Some(tmp.path().to_path_buf()),
        })
        .expect("status");
        assert!(status.contains("Node ID:"));
        assert!(status.contains("Version:"));
    }

    #[test]
    fn cmd_status_without_init_errors_clearly() {
        let tmp = TempDir::new().expect("tempdir");
        let err = cmd_status(StatusArgs {
            root: Some(tmp.path().to_path_buf()),
        })
        .expect_err("no identity yet");
        let msg = format!("{err:#}");
        assert!(msg.contains("identity"), "expected identity error: {msg}");
    }

    #[test]
    fn cmd_peers_empty_returns_friendly_message() {
        let tmp = TempDir::new().expect("tempdir");
        let _ = cmd_init(InitArgs {
            root: Some(tmp.path().to_path_buf()),
            ttl_seconds: 60,
        })
        .expect("init");
        let out = cmd_peers(PeersArgs {
            root: Some(tmp.path().to_path_buf()),
            json: false,
        })
        .expect("peers");
        assert!(out.contains("no peers yet"));
    }

    #[test]
    fn cmd_peers_json_returns_empty_array() {
        let tmp = TempDir::new().expect("tempdir");
        let _ = cmd_init(InitArgs {
            root: Some(tmp.path().to_path_buf()),
            ttl_seconds: 60,
        })
        .expect("init");
        let out = cmd_peers(PeersArgs {
            root: Some(tmp.path().to_path_buf()),
            json: true,
        })
        .expect("peers json");
        assert_eq!(out, "[]");
    }

    #[test]
    fn cmd_join_validates_code_format() {
        let tmp = TempDir::new().expect("tempdir");
        let err = cmd_join(JoinArgs {
            addr: "127.0.0.1:19198".parse().expect("addr"),
            code: "not-a-code".to_string(),
            root: Some(tmp.path().to_path_buf()),
        })
        .expect_err("bad code");
        let msg = format!("{err:#}");
        assert!(msg.contains("8 digits"), "expected hint: {msg}");
    }

    #[test]
    fn cmd_join_accepts_valid_code() {
        let tmp = TempDir::new().expect("tempdir");
        let out = cmd_join(JoinArgs {
            addr: "127.0.0.1:19198".parse().expect("addr"),
            code: "1234-5678".to_string(),
            root: Some(tmp.path().to_path_buf()),
        })
        .expect("validate");
        assert!(out.contains("Validated"));
    }

    #[test]
    fn cmd_leave_prints_helpful_message() {
        let tmp = TempDir::new().expect("tempdir");
        let out = cmd_leave(LeaveArgs {
            root: Some(tmp.path().to_path_buf()),
        })
        .expect("leave");
        assert!(out.contains("Phase 6"));
        assert!(out.contains("rm -rf"));
    }
}
