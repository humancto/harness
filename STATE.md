# Harness Implementation State

**Current phase:** 0 (project setup)
**Last updated:** 2026-05-02

## Done

- (nothing yet — code has not started)
- Repo bootstrap: PRDs imported, `CLAUDE.md` written, `ROADMAP.md` derived from v2 §23.

## In progress

- Phase 0 kickoff: Cargo workspace scaffolding (item `0.1` on the roadmap).

## Next

- `0.1` — Cargo workspace + empty crate stubs + `rust-toolchain.toml` + `.gitignore` (Rust additions).
- `0.2` — GitHub Actions CI green: `cargo fmt --check`, `cargo clippy --all-targets -D warnings`, `cargo test --workspace` on macOS + Linux.
- `0.3` — `harness-daemon` binary prints `harness <version>` for `--version`. LICENSE + README. **This is the Phase 0 exit demo per v2 §23.**

## Blocked

- (nothing)

## Open decisions

- `Cargo.lock` is committed (binary repo convention). No ADR needed.
- `.planning/` is local-only (gitignored). Plans live with the developer, not on `main`.
- LICENSE choice (MIT vs Apache-2.0 vs dual) — decide at item `0.3`. ADR if non-obvious.
- v2 §27 open questions (mDNS resilience, UI framework, CRDT vs Raft, etc.) remain deferred to their relevant phase.

## How this file is updated

Every merged PR that closes a roadmap item updates this file in the same commit that flips the `ROADMAP.md` checkbox. No silent drift.
