# Harness Implementation State

**Current phase:** 1 (mesh skeleton)
**Last updated:** 2026-05-02

## Done

- **Repo bootstrap** — PRDs imported, `CLAUDE.md`, `ROADMAP.md`, `STATE.md`, `README.md`, `.gitignore` (`693ea4d`).
- **Phase 0 — workspace bootstrap** (PR #1, squash-merged as `85551d3`):
  - `0.1` Cargo workspace with 13 crate stubs per PRD §22, `rust-toolchain.toml` pinned to `1.85.0`, MSRV `1.85` in workspace package, centralized `[workspace.dependencies]` + `[workspace.lints]` (`clippy::pedantic` warn, `unwrap_used = deny`).
  - `0.2` GitHub Actions CI on `ubuntu-latest` + `macos-latest`: `fmt --check`, `clippy --all-targets --all-features -- -D warnings`, `test --workspace --all-features`. Linux-only fast `fmt` job runs first. All 4 checks green on the merge commit.
  - `0.3` `harness-daemon` binary with `[[bin]] name = "harness"` so `cargo install --path crates/harness-daemon` produces `/usr/local/bin/harness`. `--version` prints `harness 0.0.0` via clap 4.5 derive. Real `assert_cmd` integration test in `crates/harness-daemon/tests/version.rs` asserts stdout `starts_with("harness ")` AND `contains(env!("CARGO_PKG_VERSION"))`. Dual MIT/Apache-2.0 license.

## In progress

- (between phases) — Phase 1 kickoff is queued; first item is `1.1` (identity generation).

## Next

- `1.1` — Identity: Ed25519 keypair, `~/.harness/` layout, `identity.key` mode 0600, `node_id = blake3(pubkey)[..16]`. Property test for sign/verify round-trip.

## Blocked

- (nothing)

## Open decisions / carried risks

- **Phase 0 review surfaced two Risks** to address before Phase 3 (not blocking now):
  1. `harness-capabilities` shape (single crate with feature flags, not sub-crates) is decided but not physically discoverable in the empty `lib.rs`. Add `pub mod registry;` + a `[features]` section in Phase 1 or early Phase 3 prep so a fresh contributor doesn't drift into spawning sub-crates.
  2. `tokio = ["full"]` in `[workspace.dependencies]` is free at Phase 0 (Cargo doesn't resolve unused) but will propagate heavy features to every consuming crate in Phase 1+ unless we override per-crate with `default-features = false` + minimal features.
- **`profile.release.panic = "abort"`** may need to flip to `"unwind"` for cost-tracker / brain-handover work in Phase 5. Revisit then.
- **PRD §27 open questions** remain deferred to their relevant phase (mDNS resilience → Phase 1, UI framework → Phase 6, CRDT vs Raft → Phase 2, etc.).

## How this file is updated

Every merged PR that closes a roadmap item updates this file in the same commit that flips the `ROADMAP.md` checkbox. No silent drift.
