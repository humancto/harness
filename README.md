<h1 align="center">harness</h1>

<p align="center">
  <strong>A LAN-native agent mesh.</strong><br/>
  Single Rust binary on every machine you own. Auto-discovers peers. Elects a brain.<br/>
  Runs typed, capability-routed tasks across the fleet.
</p>

<p align="center">
  <a href="https://github.com/humancto/harness/actions">
    <img alt="CI" src="https://github.com/humancto/harness/actions/workflows/ci.yml/badge.svg" />
  </a>
  <a href="./LICENSE-MIT">
    <img alt="License: MIT/Apache-2.0" src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" />
  </a>
  <img alt="Rust 1.85" src="https://img.shields.io/badge/rust-1.85+-orange.svg" />
  <img alt="Status: Phase 2" src="https://img.shields.io/badge/status-phase%202%20complete-green.svg" />
</p>

<p align="center">
  <a href="https://humancto.github.io/harness/">Website</a> ·
  <a href="./HARNESS_PRD_v2.md">PRD</a> ·
  <a href="./ROADMAP.md">Roadmap</a> ·
  <a href="./docs/decisions/">ADRs</a> ·
  <a href="./STATE.md">State</a>
</p>

---

## What is this?

You have machines. A laptop. A desktop. Maybe a Mac mini you keep running in the closet. They sit idle 95% of the time. **harness turns them into a private agent fleet.**

A single Rust binary, installed on each machine. They find each other on your LAN, agree on which one is the **brain** (planner / dispatcher) using a weighted election that prefers nodes with local LLMs and AC power, and accept typed task submissions from any of them.

When you submit work, the brain figures out which node should run it based on the capability's **cardinality**:

- **`Anyone`** — stateless work; the dispatcher picks a fit (round-robin today, scoring in Phase 4).
- **`Owner { scope_field }`** — work scoped to a directory / repo / mailbox lives where the data lives.
- **`Federated { merge, on_node_failure }`** — fan out to every eligible node, merge the results.

Searches federate across every node that has data. Heavy work runs wherever there's headroom. Cloud LLMs are escalation, not baseline. Nothing leaves your network unless you ask it to.

## Why does this exist?

The default for "an AI workflow" today is to send your data to someone else's GPU. harness is the bet that **a small mesh of your own machines is a real alternative** for the 80% of tasks that don't need GPT-4-class capability — and that for the 20% that do, you should still control the routing, the cost, and the audit log.

Three properties are load-bearing and will not be weakened:

1. **Local-first brain.** The planner runs on a local LLM by default. Cloud is escalation, requires policy approval, surfaces a cost.
2. **Capability cardinality is first-class.** Every capability declares routing semantics; the dispatcher enforces them.
3. **N×X scaling.** Streaming dispatch (no full materialization), bounded channels (backpressure tested, not assumed), resource-aware scheduling, checkpoint/resume, batched local inference, real-time cost tracking.

## What works today

After Phases 1 + 2 (≈20 PRs over the last few weeks):

|                  |                                                                                                                                                                                                    |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Mesh**         | Two laptops on the same LAN auto-discover each other (mDNS), establish a signed QUIC mesh with mutual cert pinning, exchange heartbeats every 2s, elect a brain by `brain_score`.                  |
| **Web UI**       | Open `http://localhost:19198/` to see the live mesh as a card grid: node id, brain badge, heartbeat pulse, CPU/RAM/battery bars. Submit and Runs pages let you fire off tasks and watch them land. |
| **Auth**         | Argon2id-hashed admin password (set via `harness admin set-password`), bearer-cookie sessions, sliding TTL.                                                                                        |
| **CLI**          | `harness init` / `join` / `peers` / `status` / `daemon` / `admin set-password` / `submit <capability>` / `tasks`.                                                                                  |
| **Persistence**  | SQLite at `~/.harness/harness.db` (WAL, foreign-keys-on, busy-timeout). Tasks, leases, capability index, scope index, manifests, replicated state — all on disk.                                   |
| **Capabilities** | `echo` ships built-in. The `Capability` async-trait + registry are ready for Phase 3 to drop in `shell.exec`, `llm.local.*`, `mcp.proxy`, `fs.*`, `mesh.search/grep`.                              |
| **Replication**  | LWW map of task state across the mesh (ADR-0006 — custom CRDT, not automerge; deferred to Phase 5.13 audit log).                                                                                   |
| **Tests**        | 410 passing. Each PR has expert-reviewed atomic commits + CI gating on macOS + Linux.                                                                                                              |

What doesn't work yet: live cross-mesh task dispatch over QUIC (the wire transport for `harness.task.assign|claim|result` channels is the explicit follow-up that rolls into Phase 3.3 `harness run --all`). For now, submit + execute is local-only; the in-process loop is end-to-end working.

## Getting started

```bash
# Install (Phase 6 ships curl|sh + Homebrew tap; meanwhile, build from source):
git clone https://github.com/humancto/harness.git
cd harness
cargo build --workspace --release
cd ui && npm ci && npm run build && cd ..
cargo install --path crates/harness-daemon

# First-run on the host:
harness init                        # generates identity, prints a pairing code
harness admin set-password          # interactive prompt
harness daemon                      # blocks; UI on http://localhost:19198/

# On a second machine on the same LAN:
harness init
harness join <HOST_IP>:19199 --code NNNN-NNNN
harness daemon

# Submit a task:
harness submit echo --input '{"msg":"hi"}'
harness tasks
```

Both machines now appear on each other's `/` mesh page. Submit a task on either and watch it appear on the Runs page.

## Architecture (one screen)

```
                      ┌──────────────┐  mDNS  ┌──────────────┐
                      │   harness    │◀──────▶│   harness    │
                      │   on laptop  │  QUIC  │   on desktop │
                      │      A       │◀──────▶│      B       │
                      └───────┬──────┘        └──────────────┘
                              │ http://localhost:19198
                              ▼
                      ┌────────────────┐
                      │ axum API       │  POST /api/v1/tasks
                      │ + SvelteKit UI │  GET  /api/v1/tasks
                      │ (rust-embed)   │  WS   /api/v1/events
                      └────────────────┘
```

```
crates/
  harness-core/         Wire types (PRD §13) + Signable trait + Identity primitives
  harness-mesh/         Discovery (mDNS) · Transport (QUIC + mTLS) · Heartbeats · Election · Pairing · Trust store · Admin
  harness-store/        SQLite persistence (WAL) · Tasks · Leases · Capability/scope index · Replicated state
  harness-orchestrator/ Dispatcher (cardinality routing, round-robin, lease registry)
  harness-capabilities/ Capability trait + registry + built-ins (echo today)
  harness-api/          axum HTTP/WS API surface (peers, status, tasks, capabilities, events, auth)
  harness-ui/            rust-embed bundling the SvelteKit build for in-binary serving
  harness-cli/           clap subcommand surface
  harness-daemon/        the harness binary; orchestrator + lifecycle
ui/                       SvelteKit 2 + Svelte 5 + Tailwind 3 SPA
docs/                     ADRs (0001-0007) + GitHub Pages landing
```

## Roadmap

| Phase                                  | Status             | Demo gate                                                |
| -------------------------------------- | ------------------ | -------------------------------------------------------- |
| **0** — Workspace bootstrap            | ✅ shipped         | `harness --version` works on macOS + Linux CI            |
| **1** — Mesh skeleton                  | ✅ shipped (10/10) | Two laptops discover, elect, reweight                    |
| **2** — Tasks flow                     | ✅ shipped (9/9)   | Submit `echo` from CLI/UI, persist, list                 |
| **3** — Fleet exec + built-ins + brain | 🔨 next            | `harness run --all -- uname -a`; federated `mesh.search` |
| **4** — Distribution patterns          | ⏳                 | Summarize 50 PDFs across 2 laptops, wall-clock halves    |
| **5** — Planner + cost + external      | ⏳                 | WhatsApp → mesh → reply, with cost summary               |
| **6** — Hardening + installers         | ⏳                 | curl\|sh installer, signed binaries, Homebrew tap        |
| **7** — v2 backlog                     | ⏳                 | Cross-LAN federation, WASM caps, marketplace             |

See [`ROADMAP.md`](./ROADMAP.md) for the per-item checklist.

## Design decisions

Every load-bearing decision is captured as an ADR in [`docs/decisions/`](./docs/decisions/):

- [0001 — `Resources` and `CostHint` from v1](./docs/decisions/0001-resources-and-costhint-from-v1.md)
- [0002 — `Plan::edges` orientation and content-addressing deferral](./docs/decisions/0002-plan-edge-orientation-and-canonicalization.md)
- [0006 — Task state replication: custom LWW Map (not automerge)](./docs/decisions/0006-task-state-replication-strategy.md)
- [0007 — Admin authentication for the HTTP API](./docs/decisions/0007-admin-auth.md)

## Contributing

See [`CLAUDE.md`](./CLAUDE.md) for the operating model — it applies to humans too. TL;DR: tests with every PR (no exceptions), one roadmap item per branch, expert-agent review is the merge gate, ADRs for every load-bearing decision.

## License

Dual licensed under [MIT](./LICENSE-MIT) **OR** [Apache-2.0](./LICENSE-APACHE), at your option.
