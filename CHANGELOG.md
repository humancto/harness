# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Cargo workspace per PRD §22 — 13 crates (`harness-core`, `harness-mesh`, `harness-store`, `harness-policy`, `harness-merge`, `harness-cost`, `harness-brain`, `harness-capabilities`, `harness-orchestrator`, `harness-api`, `harness-cli`, `harness-ui`, `harness-daemon`).
- `harness-daemon` binary (`[[bin]] name = "harness"`) with `--version` and `--help` via `clap` 4.5 derive.
- GitHub Actions CI: `cargo fmt --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features` on `ubuntu-latest` + `macos-latest`.
- Integration test in `crates/harness-daemon/tests/version.rs` verifies `harness --version` output via `assert_cmd`.
- Centralized `[workspace.dependencies]` and `[workspace.lints]` (`clippy::pedantic` warn-by-default, `unwrap_used = deny`, `dbg_macro = deny`).
- Dual MIT / Apache-2.0 license.

### Tooling

- Rust toolchain pinned to `1.85.0` in `rust-toolchain.toml` and `Cargo.toml` `rust-version`. CI mirrors the pin explicitly.
- `rustfmt.toml`: `max_width = 100`, no nightly-only options.
- Cargo `resolver = "2"`. Release profile uses `lto = "thin"`, `codegen-units = 1`, `panic = "abort"`.
