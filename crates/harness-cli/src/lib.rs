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
    /// Placeholder — clean teardown of `~/.harness/` is a Phase 6 concern.
    Leave(LeaveArgs),
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

/// Resolve the harness root: explicit `--root` wins, otherwise
/// `~/.harness/`.
fn resolve_root(root: Option<PathBuf>) -> Result<PathBuf> {
    match root {
        Some(p) => Ok(p),
        None => identity::default_root().context("locate ~/.harness/"),
    }
}

/// Dispatch to the subcommand handler. Returns the human-readable
/// stdout the daemon should print. Tests call this directly with
/// pre-configured roots; production calls it via `harness-daemon`'s
/// `main`.
pub fn run(cli: Cli) -> Result<String> {
    match cli.command {
        Command::Init(args) => cmd_init(args),
        Command::Join(args) => cmd_join(args),
        Command::Peers(args) => cmd_peers(args),
        Command::Status(args) => cmd_status(args),
        Command::Leave(args) => cmd_leave(args),
    }
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
