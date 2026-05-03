//! Harness daemon entry point. Wires `harness-cli`'s subcommand
//! surface into a real `harness` binary.
#![forbid(unsafe_code)]

mod lifecycle;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use harness_cli::{run, Cli, DaemonArgs, SyncOutcome};

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
    }
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
        ..DaemonRuntimeConfig::default()
    };
    if let Ok(peers) = std::env::var("HARNESS_STATIC_PEERS") {
        config.static_peers = peers
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
    }
    if std::env::var("HARNESS_MDNS_DISABLED").is_ok() {
        config.mdns_enabled = false;
    }
    if let Ok(bind) = std::env::var("HARNESS_MESH_BIND") {
        if let Ok(addr) = bind.parse() {
            config.mesh_bind = addr;
        }
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
