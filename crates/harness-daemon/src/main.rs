//! Harness daemon entry point. Wires `harness-cli`'s subcommand
//! surface into a real `harness` binary.
#![forbid(unsafe_code)]

use clap::Parser;
use harness_cli::{run, Cli};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let stdout = run(cli)?;
    // The CLI dispatcher returns a String; the daemon's job is to put
    // it on stdout for the operator. clippy::print_stdout would
    // normally fire; this is the one place a CLI binary legitimately
    // owns stdout.
    #[allow(clippy::print_stdout)]
    {
        println!("{stdout}");
    }
    Ok(())
}
