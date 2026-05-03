//! Harness daemon entry point. Wires `harness-cli`'s subcommand
//! surface into a real `harness` binary.
#![forbid(unsafe_code)]

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use harness_cli::{run, Cli, DaemonArgs, SyncOutcome};

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

    let mesh_name = std::env::var("HARNESS_MESH_NAME").unwrap_or_else(|_| "harness".to_string());

    let state = harness_api::ApiStateBuilder::new(identity.clone(), mesh_name.clone())
        .with_capabilities(vec!["builtin.echo".to_string()])
        .build();

    let server = harness_api::serve(args.bind, state.clone())
        .await
        .context("bind harness-api")?;
    let bound = server.local_addr();
    tracing::info!(target: "harness.daemon", addr = %bound, "harness-api listening");
    #[allow(clippy::print_stdout)]
    {
        println!(
            "harness daemon\n  node_id: {}\n  pubkey:  {}\n  ui:      http://{}/\n  api:     http://{}/api/v1/",
            identity.node_id(),
            identity.public_key().fingerprint_hex(),
            bound,
            bound
        );
        println!("press ctrl-c to stop");
    }

    // Phase 1.10 ships the API + UI server. The mesh-side wiring
    // (Discovery + Transport + HeartbeatService + Election) is a
    // larger lift carried as a Phase 2 follow-up — without it, the UI
    // shows only the local node and a "no peers yet" hint.
    tokio::signal::ctrl_c().await.ok();
    tracing::info!(target: "harness.daemon", "shutdown requested");
    server.shutdown().await;
    Ok(())
}
