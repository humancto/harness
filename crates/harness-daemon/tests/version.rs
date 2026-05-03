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
fn no_args_succeeds() {
    Command::cargo_bin("harness")
        .expect("binary `harness` should be built by `cargo test`")
        .assert()
        .success();
}
