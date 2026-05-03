# Harness Roadmap

Derived from `HARNESS_PRD_v2.md` §23. One checked item ≈ one merged PR. Phase N must demoably ship before Phase N+1 starts (PRD §29 rule 1). Every PR ships with tests — "trust CI" is not a test plan.

When an item lands, flip its checkbox **in the same PR** as `STATE.md` is updated, then merge to `main`.

---

## Phase 0 — Project setup

**Demo:** `cargo run --bin harness -- --version` prints a real version string. CI is green on macOS + Linux.

- [ ] **0.1** Cargo workspace + empty crate stubs per PRD §22 + `rust-toolchain.toml` (stable, MSRV declared) + `.gitignore` Rust additions
- [ ] **0.2** GitHub Actions CI: `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --workspace` matrix on `macos-latest` + `ubuntu-latest`
- [ ] **0.3** `harness-daemon` binary: `--version` output (clap or hand-rolled), LICENSE + README. Smoke test asserts version string format.

## Phase 1 — Mesh skeleton

**Demo:** install on two laptops, watch them discover each other, elect a brain, see the brain reweight when a stronger node joins.

- [ ] **1.1** Identity generation: Ed25519 keypair, `~/.harness/` layout, `identity.key` mode 0600, `node_id = blake3(pubkey)[..16]`. Property test: round-trip sign/verify.
- [ ] **1.2** Protocol types in `harness-core` (PRD §13): `Heartbeat`, `NodeManifest`, `Capability`, `Cardinality`, `Scope`, `ResourceHints`, `Resources`. CBOR round-trip + signature property tests.
- [ ] **1.3** mDNS discovery: advertise `_harness._tcp.local`, TXT record (`mesh_name`, `node_id`, `pubkey_fp`, `version`). Static peer list fallback.
- [ ] **1.4** QUIC transport via `quinn` with Noise/TLS, one connection per peer pair, signed message envelope. Replay protection via monotonic seq.
- [ ] **1.5** Heartbeat broadcast loop (every 2s) + signature validation + leader-belief field carried.
- [ ] **1.6** Weighted brain election (PRD §12.2): planner-aware, battery-aware, anti-flap. Property tests on convergence + no split-brain on connected LAN.
- [ ] **1.7** Pairing flow: `harness init` (creates mesh, prints pairing code, sets admin password) and `harness join` (scans LAN, pairing-code-approved exchange of pubkeys).
- [ ] **1.8** Trust file `~/.harness/peers.toml` (read/write/gossip on join).
- [ ] **1.9** CLI: `harness peers`, `harness status`.
- [ ] **1.10** Web UI Mesh page (read-only, live topology over WebSocket relay).

## Phase 2 — Tasks flow

**Demo:** submit `echo "hello"` from any node, watch it execute on another, view in UI.

- [ ] **2.1** Task / Result / Plan envelopes in `harness-core` (PRD §13.3–13.5). Sign + verify. Property tests.
- [ ] **2.2** Cardinality enforcement at dispatcher: `Anyone` / `Owner { scope_field }` / `Federated { merge, on_node_failure }`.
- [ ] **2.3** SQLite schema for tasks, capability index, scopes (in `harness-store`, WAL mode).
- [ ] **2.4** Round-robin dispatcher (no scoring yet) + lease-based claiming.
- [ ] **2.5** Task state replication via gossip (CRDT — `automerge` or custom).
- [ ] **2.6** HTTP submit API: `POST /api/v1/tasks` + WebSocket result stream (`harness-api`).
- [ ] **2.7** CLI: `harness submit <capability> [--input <json>|@file]`.
- [ ] **2.8** Built-in `echo` capability (the simplest possible `Anyone`).
- [ ] **2.9** Web UI Submit page (capability picker, JSON Schema → form) + Runs page.

## Phase 3 — Fleet exec, brain runtime, built-ins

**Demo:** `harness run --all -- uname -a`; `harness search "term sheet"` federates across nodes.

- [ ] **3.1** Policy engine: parse `~/.harness/policy.toml`, allow/deny matching, evaluated **on the executing node** (PRD §10.4).
- [ ] **3.2** `shell.exec` capability with streaming output (line-frames over QUIC) + policy check.
- [ ] **3.3** CLI: `harness run --all|--on <node>|--where <expr>`. UI Remote Shell mode with `[node-name]` interleaving.
- [ ] **3.4** `llm.local.<model>` auto-registration from `ollama list`.
- [ ] **3.5** Local-LLM micro-batcher (configurable `batch_window_ms`, default 50).
- [ ] **3.6** `llm.cloud.{claude,openai,gemini}` capabilities + secrets-by-tag reference.
- [ ] **3.7** `mcp.proxy` via `rmcp`: subprocess MCP servers, expose tools as `mcp.<server>.<tool>`.
- [ ] **3.8** `brain.plan` Template backend (hardcoded plan templates from PRD §15.6).
- [ ] **3.9** `brain.plan` LocalFast backend (tier 1) + plan validation (schema match + DAG acyclicity + cost cap).
- [ ] **3.10** `fs.list` / `fs.read` / `fs.search` / `fs.grep` (Owner cardinality, Tantivy or sqlite-FTS index).
- [ ] **3.11** `mesh.search` / `mesh.grep` federated wrappers (fan-out + Concat / Rerank merge).

## Phase 4 — Distribution patterns

**Demo:** "summarize 50 PDFs across 2 laptops" — wall-clock ~halves. Federated `mesh.search` shows per-node contribution in UI.

- [ ] **4.1** `FanoutController` with bounded streaming dispatch (PRD §14.7) — never materialize all sub-tasks.
- [ ] **4.2** Result streams: `Stream<TaskResult>` for callers, WebSocket for UI.
- [ ] **4.3** DAG executor (topological dispatch with dependency tracking).
- [ ] **4.4** Resource-aware scheduler (multidimensional load — CPU/RAM/GPU/network/disk; `fit_score` per PRD §14.3).
- [ ] **4.5** Federated execution lifecycle with `PartialResult` streaming + `provenance` per `NodeContribution`.
- [ ] **4.6** Lease extension + re-dispatch on lease expiry + idempotent retry by `task_id`.
- [ ] **4.7** Bounded channels everywhere + backpressure tested (paused heartbeat field).
- [ ] **4.8** Web UI DAG visualization + per-task live progress bars.

## Phase 5 — Planner intelligence + cost + external

**Demo:** WhatsApp message → mesh executes multi-step plan → text reply with cost summary. Crash brain mid-plan; new brain resumes from checkpoint.

- [ ] **5.1** `brain.plan` LocalStrong backend (tier 2, 32B–70B class).
- [ ] **5.2** `brain.plan` Cloud backend (tier 3) with policy-driven escalation rules.
- [ ] **5.3** Full plan validation ruleset (`plan_validation_failed` / `tool_not_found` escalation triggers).
- [ ] **5.4** Natural-language Submit mode in UI (textarea → planner → DAG preview → confirm).
- [ ] **5.5** WhatsApp webhook adapter (Twilio signature validation).
- [ ] **5.6** SMS webhook adapter (Twilio).
- [ ] **5.7** iOS Shortcuts adapter (signed JSON token).
- [ ] **5.8** `Budget` enforcement: `max_cost_usd`, `soft_limit_usd`, `on_exceed` (Pause / Cancel / Notify).
- [ ] **5.9** Real-time cost tracking in `harness-cost` (estimated + actual per task; running total per plan/user/day).
- [ ] **5.10** Cost dashboard UI page (live spend graph, projected total, big red stop button).
- [ ] **5.11** Checkpoint store (SQLite) with input-hash dedup (blake3 of canonical-encoded input).
- [ ] **5.12** Checkpoint resume on brain handover.
- [ ] **5.13** Audit log (append-only, hash-chained, replicated) + History UI page.

## Phase 6 — Hardening & polish

**Demo:** end-to-end story from PRD §9–§20 working without manual intervention. CI scaling benchmarks pass at the targets in §17.9.

- [ ] **6.1** Self-updater (rolling, version-negotiated, automatic rollback on health failure).
- [ ] **6.2** Speculative execution (`redundancy=2`, first wins).
- [ ] **6.3** Circuit breakers (5 consecutive failures → 60s bench).
- [ ] **6.4** `schedule.cron` capability (replicated state, brain-triggered).
- [ ] **6.5** Encrypted secrets store `~/.harness/secrets.enc` (replicated, tag-referenced; raw values never on the wire).
- [ ] **6.6** Settings UI tabs: Peers, Capabilities, Scopes, Secrets, Policy, Mesh, Schedules.
- [ ] **6.7** Mobile-responsive UI pass.
- [ ] **6.8** One-line installer (`curl|sh`) + signed binaries (`cosign` / `minisign`).
- [ ] **6.9** Homebrew tap.
- [ ] **6.10** `.deb` / `.rpm` packaging.
- [ ] **6.11** Benchmark suite in CI (PRD §17.9 N×X scaling targets — regressions fail the build).

## Phase 7 — v2 backlog (deferred)

Cross-LAN federation (Tailscale / iroh), WASM sandboxed third-party capabilities, multi-user RBAC UI, embedded inference (`mistral.rs` / `candle`), capability marketplace, cross-mesh sync. Broken into items when Phase 6 ships.

---

## Process notes

- **No item is started without an expert agent reviewing the plan in `.planning/<slug>.plan.md`.** Expert is `rust-expert` for everything in `crates/`.
- **No PR is merged without an expert `APPROVE` verdict on the merged diff.** Showstoppers and Bugs block; Nits are OK to ship.
- **No checkbox flips before the PR is actually merged.** Promotion-by-promise is forbidden.
- **No phase advances before its demo runs cleanly on real hardware.** Tests in CI are necessary but not sufficient — the demo is the gate.
