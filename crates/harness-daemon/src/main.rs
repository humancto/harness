//! Harness daemon entry point. Wires `harness-cli`'s subcommand
//! surface into a real `harness` binary.
#![forbid(unsafe_code)]

mod audit_wiring;
mod cost_gate;
mod dispatch;
mod executor;
#[cfg(test)]
mod fanout_tests;
mod federated;
mod gossip;
#[cfg(test)]
mod gossip_tests;
mod lifecycle;
mod mesh_exec;
mod partial_stream;
mod pause;
mod peer_net;
mod subtask;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use harness_cli::{run, run_run, Cli, DaemonArgs, RunArgs, RunOutcome, SyncOutcome};

use crate::lifecycle::{DaemonOrchestrator, DaemonRuntimeConfig};

fn main() -> Result<()> {
    let cli = Cli::parse();
    match run(cli)? {
        SyncOutcome::Print(stdout) => {
            #[allow(clippy::print_stdout)]
            {
                println!("{stdout}");
            }
            Ok(())
        }
        SyncOutcome::DaemonRequested(args) => run_daemon(args),
        SyncOutcome::SubmitRequested(args) => run_async_cli(harness_cli::run_submit(args)),
        SyncOutcome::TasksRequested(args) => run_async_cli(harness_cli::run_tasks(args)),
        SyncOutcome::RunRequested(args) => dispatch_run(args),
        SyncOutcome::SearchRequested(args) => {
            dispatch_outcome(harness_cli::query::run_search(args))
        }
        SyncOutcome::GrepRequested(args) => dispatch_outcome(harness_cli::query::run_grep(args)),
        SyncOutcome::PlanRequested(args) => dispatch_outcome(harness_cli::plan::run_plan(args)),
        SyncOutcome::ExecRequested(args) => dispatch_outcome(harness_cli::plan::run_exec(args)),
    }
}

/// Like [`dispatch_run`] for any future resolving to a [`RunOutcome`]
/// (`harness search` / `harness grep`).
fn dispatch_outcome<F>(fut: F) -> Result<()>
where
    F: std::future::Future<Output = Result<RunOutcome>>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    let outcome: RunOutcome = runtime.block_on(fut)?;
    emit_outcome(&outcome);
    if outcome.code != 0 {
        std::process::exit(outcome.code);
    }
    Ok(())
}

fn emit_outcome(outcome: &RunOutcome) {
    if !outcome.stdout.is_empty() {
        #[allow(clippy::print_stdout)]
        {
            print!("{}", outcome.stdout);
        }
    }
    if !outcome.stderr.is_empty() {
        #[allow(clippy::print_stderr)]
        {
            eprint!("{}", outcome.stderr);
        }
    }
}

/// Dispatch `harness run`. Like `run_async_cli` but emits to both stdout
/// and stderr and uses the outcome's exit code.
fn dispatch_run(args: RunArgs) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    let outcome: RunOutcome = runtime.block_on(run_run(args))?;
    if !outcome.stdout.is_empty() {
        #[allow(clippy::print_stdout)]
        {
            print!("{}", outcome.stdout);
        }
    }
    if !outcome.stderr.is_empty() {
        #[allow(clippy::print_stderr)]
        {
            eprint!("{}", outcome.stderr);
        }
    }
    if outcome.code != 0 {
        std::process::exit(outcome.code);
    }
    Ok(())
}

fn run_async_cli<F>(fut: F) -> Result<()>
where
    F: std::future::Future<Output = Result<String>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    let out = runtime.block_on(fut)?;
    #[allow(clippy::print_stdout)]
    {
        println!("{out}");
    }
    Ok(())
}

/// Parse `HARNESS_MAX_QUEUE_DEPTH`: a positive u16 (0 would latch the
/// auto-pause permanently — refused, not clamped).
fn parse_max_queue_depth(s: &str) -> Result<u16> {
    let depth: u16 = s
        .trim()
        .parse()
        .with_context(|| format!("HARNESS_MAX_QUEUE_DEPTH={s:?} is not a u16"))?;
    anyhow::ensure!(
        depth > 0,
        "HARNESS_MAX_QUEUE_DEPTH must be >= 1 (0 would pause the node permanently)"
    );
    Ok(depth)
}

fn run_daemon(args: DaemonArgs) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    runtime.block_on(daemon_main(args))
}

async fn daemon_main(args: DaemonArgs) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,harness=info")),
        )
        .init();

    let root = match args.root {
        Some(p) => p,
        None => harness_mesh::identity::default_root().context("locate ~/.harness/")?,
    };
    let identity = harness_mesh::identity::init_or_load(&root).context("init/load identity")?;
    let identity = Arc::new(identity);
    let trust =
        harness_mesh::TrustStore::open(&root, identity.node_id()).context("open peers.toml")?;

    let mesh_name = std::env::var("HARNESS_MESH_NAME").unwrap_or_else(|_| "harness".to_string());

    let mut config = DaemonRuntimeConfig {
        mesh_name,
        api_bind: args.bind,
        harness_root: root.clone(),
        ..DaemonRuntimeConfig::default()
    };
    if let Ok(peers) = std::env::var("HARNESS_STATIC_PEERS") {
        // Strict parsing — a typo in operator config means the node
        // boots in degraded mode. Refuse to start.
        config.static_peers = peers
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<std::net::SocketAddr>().with_context(|| {
                    format!("HARNESS_STATIC_PEERS entry {s:?} is not a valid SocketAddr")
                })
            })
            .collect::<Result<Vec<_>>>()?;
    }
    if let Ok(v) = std::env::var("HARNESS_MDNS_DISABLED") {
        // Treat empty / "0" / "false" as not-disabled.
        let on = !matches!(v.as_str(), "" | "0" | "false");
        if on {
            config.mdns_enabled = false;
        }
    }
    if let Ok(bind) = std::env::var("HARNESS_MESH_BIND") {
        config.mesh_bind = bind
            .parse()
            .with_context(|| format!("HARNESS_MESH_BIND={bind:?} is not a valid SocketAddr"))?;
    }
    // 4.7 (ADR-0029): the operator backpressure knob — auto-pause
    // latches at this WORK depth (resume at 3/4). Strict parsing, same
    // posture as HARNESS_STATIC_PEERS: a typo must not boot a node
    // with a silently-default bound.
    if let Ok(depth) = std::env::var("HARNESS_MAX_QUEUE_DEPTH") {
        config.max_queue_depth = parse_max_queue_depth(&depth)?;
    }

    let orchestrator = DaemonOrchestrator::build(identity.clone(), trust, config)
        .await
        .context("build daemon orchestrator")?;
    let api_addr = orchestrator.api_addr();

    #[allow(clippy::print_stdout)]
    {
        println!(
            "harness daemon\n  node_id: {}\n  pubkey:  {}\n  ui:      http://{}/\n  api:     http://{}/api/v1/",
            identity.node_id(),
            identity.public_key().fingerprint_hex(),
            api_addr,
            api_addr,
        );
        println!("press ctrl-c to stop");
    }

    orchestrator.run_until_signal().await
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::parse_max_queue_depth;

    #[test]
    fn max_queue_depth_env_parses_strictly() {
        assert_eq!(parse_max_queue_depth("64").unwrap(), 64);
        assert_eq!(parse_max_queue_depth(" 4 ").unwrap(), 4);
        assert!(
            parse_max_queue_depth("0").is_err(),
            "permanent pause refused"
        );
        assert!(parse_max_queue_depth("-1").is_err());
        assert!(parse_max_queue_depth("70000").is_err(), "u16 overflow");
        assert!(parse_max_queue_depth("lots").is_err());
    }
}
