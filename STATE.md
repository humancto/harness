# Harness Implementation State

**Current phase:** 1 (mesh skeleton)
**Last updated:** 2026-05-03 (post-1.2)

## Done

- **Repo bootstrap** — PRDs imported, `CLAUDE.md`, `ROADMAP.md`, `STATE.md`, `README.md`, `.gitignore` (`693ea4d`).
- **Phase 0 — workspace bootstrap** (PR #1, squash-merged as `85551d3`):
  - `0.1` Cargo workspace with 13 crate stubs per PRD §22, `rust-toolchain.toml` pinned to `1.85.0`, MSRV `1.85` in workspace package, centralized `[workspace.dependencies]` + `[workspace.lints]` (`clippy::pedantic` warn, `unwrap_used = deny`).
  - `0.2` GitHub Actions CI on `ubuntu-latest` + `macos-latest`: `fmt --check`, `clippy --all-targets --all-features -- -D warnings`, `test --workspace --all-features`. Linux-only fast `fmt` job runs first. All 4 checks green on the merge commit.
  - `0.3` `harness-daemon` binary with `[[bin]] name = "harness"` so `cargo install --path crates/harness-daemon` produces `/usr/local/bin/harness`. `--version` prints `harness 0.0.0` via clap 4.5 derive. Real `assert_cmd` integration test in `crates/harness-daemon/tests/version.rs` asserts stdout `starts_with("harness ")` AND `contains(env!("CARGO_PKG_VERSION"))`. Dual MIT/Apache-2.0 license.

## In progress

- Phase 1 — mesh skeleton.
  - `1.1` shipped (PR #2, `e395d9f`): identity primitives + `~/.harness/` filesystem layout.
  - `1.2` shipped (PR #3, `fa5d23b`): the §13.1–§13.2 wire types (`Heartbeat`, `NodeManifest`, `Capability`, `Cardinality`, `MergeStrategy`, `PartialPolicy`, `Scope`, `ResourceHints`, `Resources`, `TaskId`, `PlanId`, `SemVer`) + `Signable` trait (canonical-encoding-with-sig-zeroed, routes through `verify_strict`) + `ProtocolError`. ADR-0001 records the v1→v2 carry-forward of `Resources`/`CostHint`/`RateLimit`/`GpuInfo`. 17 unit + 4 property tests (256 cases each) + 2 insta wire-format fixtures (`heartbeat_wire_v0`, `node_manifest_wire_v0`) + size-budget regression gate.

## Next

- **`1.3` (mDNS)**, **`1.4` (QUIC transport)**, and **`1.8` (peers.toml trust file)** are now genuinely independent — three concurrent feature branches at the next loop iteration.
- After those: `1.5` (heartbeat broadcast loop) depends on 1.3+1.4; `1.6` (election) depends on 1.5; `1.7` (pairing) depends on 1.4; `1.9` (CLI peers/status) depends on 1.8; `1.10` (UI Mesh page) depends on 1.5.

## Blocked

- (nothing)

## Open decisions / carried risks

- **Phase 0 review surfaced two Risks** to address before Phase 3 (not blocking now):
  1. `harness-capabilities` shape (single crate with feature flags, not sub-crates) is decided but not physically discoverable in the empty `lib.rs`. Add `pub mod registry;` + a `[features]` section in Phase 1 or early Phase 3 prep so a fresh contributor doesn't drift into spawning sub-crates.
  2. `tokio = ["full"]` in `[workspace.dependencies]` is free at Phase 0 (Cargo doesn't resolve unused) but will propagate heavy features to every consuming crate in Phase 1+ unless we override per-crate with `default-features = false` + minimal features.
- **Phase 1.1 review surfaced two Risks** carried as follow-ups:
  1. `write_atomic` does not `fsync` the parent directory after rename. Crash-durability gap on Linux/macOS. Plan §7.3 #8 descoped this; file as a follow-up issue once the issue tracker is in active use.
  2. Windows ACL enforcement on `identity.key` (currently `tracing::warn` only). PRD §10.1 wants 0600-equivalent; needs `windows-acl` integration. Acceptable for the "two laptops" demo per plan §11 R1.
- **Phase 1.2 review surfaced four Risks** carried as follow-ups:
  1. Wire-format insta fixtures don't lock the externally-tagged `Cardinality` / `MergeStrategy` shape (the two `_v0` fixtures are a `Heartbeat` with no `Cardinality` field and a `NodeManifest` with empty `capabilities`). Add a `cardinality_wire_v0` fixture in 1.5 to pin the on-wire bytes.
  2. `getrandom` is pulled transitively by the `uuid` `v7` cargo feature regardless of whether we generate UUIDs (only deserialize). No functional impact (~20 KB, builds clean on Darwin/Linux/Windows); update plan/ADR comments when 2.1 lands `TaskId::new_v7()`.
  3. Heartbeat size budget at 512 B leaves only ~30 B headroom over the real-world ~480 B encoding. Acceptable now (well under any QUIC datagram); a future PR can swap struct field names for stable numeric IDs to drop ~60% — wire-format change requiring a separate ADR + version bump.
  4. `Heartbeat::leader_belief: NodeId` has no "no belief yet" sentinel (other than zeroed bytes). Consider `Option<NodeId>` in 1.6 if pre-election heartbeats are real. Wire-format change.
- **Missing in 1.2**, queued for 1.3+: `NodeManifest` property tests (no-capabilities form is mechanical), `ed25519` deterministic-signature property test (one line — catches a future swap to randomized signing).
- **`profile.release.panic = "abort"`** may need to flip to `"unwind"` for cost-tracker / brain-handover work in Phase 5. Revisit then.
- **PRD §27 open questions** remain deferred to their relevant phase (mDNS resilience → Phase 1, UI framework → Phase 6, CRDT vs Raft → Phase 2, etc.).

## How this file is updated

Every merged PR that closes a roadmap item updates this file in the same commit that flips the `ROADMAP.md` checkbox. No silent drift.
