# Harness Implementation State

**Current phase:** 1 (mesh skeleton)
**Last updated:** 2026-05-03 (post-1.8)

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
  - `1.8` shipped (PR #4, `6bb2474`): `harness_mesh::trust` with `Peer` / `TrustTier` / `AddedVia` / `TrustEvent` / `TrustError` / `TrustStore` (open / add / remove / tier / lookup_by_pubkey / all_peers / contains / subscribe). Hex-encoded TOML on disk (`format_version = 1`); hard-error on every inconsistency including self-add, self-in-loaded-file, mode≠0600, format-version mismatch, node_id/pubkey mismatch, duplicate node_ids. Persist-then-commit semantics in add/remove (cache stays at the prior state on persist failure) — review-driven correction with a regression test that locks the invariant. Refactor commit pulls `create_root_dir` / `write_atomic` / `enforce_mode_0600` out of `identity.rs` into `harness_mesh::fs_util` so 1.1 and 1.8 share one implementation; existing 1.1 tests pass unchanged. 5 fs_util unit + 12 trust-file unit + 14 integration + 1 property (64×32 ops). Workspace dep `parking_lot 0.12` added (also unlocks 1.5).

## Next

- **`1.4` (QUIC transport)** — the largest single PR in Phase 1 (5 commits per plan: TLS over Noise via rustls + custom `PinnedKeyVerifier` against `expected_pubkey`, cert deterministic from `Identity` via `rcgen`, cancel-safe `RecvFramer` state machine on `Connection<Mutex>`, per-channel replay protection via `Sequenced` trait on top of `Signable`, 0-RTT disabled, dedupe deferred to 1.5). Plan ready at `.planning/phase-1.4-quic.plan.md`.
- **`1.3` (mDNS)** — runs after 1.4 lands; advertises the QUIC port discovered by 1.4. Plan ready.
- After those: `1.5` (heartbeat broadcast loop) depends on 1.3+1.4; `1.6` (election) depends on 1.5; `1.7` (pairing) depends on 1.4; `1.9` (CLI peers/status) depends on 1.8 (just shipped); `1.10` (UI Mesh page) depends on 1.5.

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
- **Phase 1.8 review surfaced two carried Risks** (the cache-vs-disk drift was fixed in this PR; remaining items are minor):
  1. `add` / `remove` clone the entire cache on every successful mutation. O(N) per call. Fine at PRD scale (hundreds of peers); revisit with `im::HashMap` if Phase 6 multi-tenant pushes counts into the thousands.
  2. Lagged-subscriber test deferred (would require flooding the 256-event broadcast). The property test (random_add_remove × 64 cases × ≤32 ops × reopen) covers the more important file/cache invariant. Land a dedicated lag test before 1.5's gossip wires up subscribers.
- **`profile.release.panic = "abort"`** may need to flip to `"unwind"` for cost-tracker / brain-handover work in Phase 5. Revisit then.
- **PRD §27 open questions** remain deferred to their relevant phase (mDNS resilience → Phase 1, UI framework → Phase 6, CRDT vs Raft → Phase 2, etc.).

## How this file is updated

Every merged PR that closes a roadmap item updates this file in the same commit that flips the `ROADMAP.md` checkbox. No silent drift.
