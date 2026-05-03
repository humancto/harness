//! Harness daemon entry point. Phase 0 implements `--version` only.
//! Phase 1 will replace `Cli` with real subcommands (init, join, status, peers, ...).
#![forbid(unsafe_code)]

use clap::Parser;

/// Harness — a LAN-native agent mesh.
#[derive(Debug, Parser)]
#[command(
    name = "harness",
    version = env!("CARGO_PKG_VERSION"),
    about,
    long_about = None,
)]
struct Cli {
    // Phase 1 will add subcommands: init, join, status, peers, ...
}

fn main() {
    let _cli = Cli::parse();
    // Phase 0: parsing the args is the entire program. With no subcommands,
    // a successful parse means the user asked for --help or --version
    // (which clap handles and exits) or invoked with no args (no-op success).
    // Phase 1 will introduce error returns when subcommands like `init` and
    // `join` start touching the filesystem and network.
}
