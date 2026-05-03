//! Integration test: `harness --version` prints `harness <semver>`.
//!
//! `assert_cmd` builds the actual binary and runs it as a subprocess, so this
//! is a real end-to-end test of the Phase 0 demo gate — not a unit test of clap.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use assert_cmd::Command;
use predicates::str;

#[test]
fn version_flag_prints_harness_and_pkg_version() {
    let pkg_version = env!("CARGO_PKG_VERSION");
    Command::cargo_bin("harness")
        .expect("binary `harness` should be built by `cargo test`")
        .arg("--version")
        .assert()
        .success()
        .stdout(str::starts_with("harness "))
        .stdout(str::contains(pkg_version));
}

#[test]
fn no_args_prints_help_and_exits_nonzero() {
    // With Phase 1.9 subcommands, calling `harness` with no args should
    // print clap's help (which includes "Usage:") and exit with the
    // standard help-on-missing-subcommand exit code.
    Command::cargo_bin("harness")
        .expect("binary `harness` should be built by `cargo test`")
        .assert()
        .failure() // clap exits non-zero when a required subcommand is missing
        .stderr(str::contains("Usage:"));
}

#[test]
fn status_subcommand_is_recognized() {
    // `--help` on a subcommand should succeed even without an existing
    // ~/.harness/ root.
    Command::cargo_bin("harness")
        .expect("binary `harness` should be built by `cargo test`")
        .args(["status", "--help"])
        .assert()
        .success()
        .stdout(str::contains("Print local node info"));
}

#[test]
fn init_subcommand_is_recognized() {
    Command::cargo_bin("harness")
        .expect("binary `harness` should be built by `cargo test`")
        .args(["init", "--help"])
        .assert()
        .success()
        .stdout(str::contains("pairing code"));
}

#[test]
fn peers_subcommand_is_recognized() {
    Command::cargo_bin("harness")
        .expect("binary `harness` should be built by `cargo test`")
        .args(["peers", "--help"])
        .assert()
        .success()
        .stdout(str::contains("peers.toml"));
}

#[test]
fn daemon_subcommand_is_recognized() {
    Command::cargo_bin("harness")
        .expect("binary `harness` should be built by `cargo test`")
        .args(["daemon", "--help"])
        .assert()
        .success()
        .stdout(str::contains("long-lived"));
}
