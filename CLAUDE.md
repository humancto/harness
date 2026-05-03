# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository state — read this first

This repo is **PRD-only**. There is no Rust code, no `Cargo.toml`, no `ROADMAP.md`, no tests, no CI yet. The directory currently contains exactly three files:

- `HARNESS_PRD_v2.md` — **canonical spec**. v1 + addendum consolidated. Read this.
- `HARNESS_PRD.md` — v1, kept for diff/history. Superseded.
- `HARNESS_PRD_v2_addendum.md` — the design rationale that drove v1 → v2. Useful for _why_ decisions were made.

Treat v2 as authoritative. When v1 and v2 disagree, v2 wins. Do not edit v1 or the addendum to "fix" inconsistencies — they are historical.

The repo structure described in v2 §22 (`Cargo.toml` workspace, `crates/harness-*`, `ui/`, `installers/`, etc.) is the **target** structure. None of it exists yet. Phase 0 (project setup) hasn't been done.

## What "the project" actually is

Harness is a single Rust binary that installs on every machine on a LAN, auto-discovers peers via mDNS, forms a signed mesh over QUIC, elects a brain (planner/dispatcher) using a weighted election that prefers nodes with local LLMs and AC power, and runs typed, capability-routed tasks across the fleet. Read v2 §1 for the pitch and §8 for the layered architecture.

Three pillars from v2 — these define the product, do not weaken them:

1. **Local-first brain** — the planner is a local LLM by default; cloud is escalation, not baseline. (§15)
2. **Capability cardinality is first-class** — every capability declares `Anyone | Owner | Federated`; routing is automatic. (§13.2, §14.2)
3. **N×X scaling thesis** — streaming dispatch, bounded channels/backpressure, resource-aware scheduling, checkpoint/resume, batched local inference, real-time cost tracking. (§17)

## Operating principles (from v2 §29, with project additions)

When you do start implementing:

1. **Never skip phases.** v2 §23 defines Phase 0 → Phase 6 (+ v2 backlog). Each phase ends with a working, demoable artifact. Complete Phase N's demo before starting Phase N+1.
2. **Tests ship with every PR. No exceptions.** "Trust CI" is not a test plan. Config and plumbing PRs must spell out their verification strategy. The user has been emphatic about this — a PR with no test and no justification will be rejected.
3. **When the spec is silent**, propose a resolution in `docs/decisions/NNNN-title.md` (ADR format) and proceed. Do not silently invent.
4. **Maintain `STATE.md`** at the repo root: current phase, what's done, what's next, what's blocked. Update it as you go.
5. **Always implement streaming dispatch** — never materialize all sub-tasks for a fan-out. `FanoutController` keeps a bounded window in flight (v2 §14.7).
6. **Bounded channels everywhere.** Per-node task queue, result aggregation, log streams. Backpressure is tested, not assumed (v2 §14.10).
7. **Always set `Cardinality` on new capabilities.** Default to `Anyone` if uncertain, but document the choice in the capability's doc-comment.
8. **Never bypass plan validation**, even for "trusted" planner backends. Invalid plans are not executed (v2 §15.4).
9. **`brain.plan` runs locally first.** Cloud escalation requires `cloud_ok` tag and policy approval. Don't quietly default to cloud.
10. **Policy is evaluated on the executing node, not the dispatcher.** Brains cannot override worker policy (v2 §10.4).
11. Prefer simple, small modules. Move concerns into the right crate (v2 §22) rather than letting `harness-core` accrete.

## Stopping conditions — stop and ask the user if

- A protocol-level change would break wire compatibility with already-shipped nodes.
- A new external dependency is required (especially anything that changes the "single binary, no broker, no DB server" property).
- A security/privacy implication arises that isn't covered by the PRD (e.g., a new capability that crosses LAN, a new secret-handling path).
- Brain election or planner validation would be weakened.
- `must_be_local` semantics or the `local-only` tag would be loosened.

## Roadmap-driven workflow

The user's global `~/.claude/CLAUDE.md` enforces a roadmap-driven loop: pick the next unchecked item from `ROADMAP.md`, draft a plan in `.planning/<slug>.plan.md`, get it reviewed by the matching expert agent (here: `rust-expert`), branch, implement with atomic commits, open a PR, get the diff reviewed by `rust-expert` again, merge on `APPROVE`, flip the checkbox.

For this repo specifically:

- `ROADMAP.md` does not exist yet. Phase 0 is to **derive `ROADMAP.md` from v2 §23** (one checklist item per phase deliverable, fine-grained enough that each item is a single PR).
- The matching expert is **`rust-expert`** for everything in `crates/`. UI work in `ui/` may pull in a frontend expert if/when SvelteKit-vs-Leptos is decided (v2 §27 open question #2).
- Until `Cargo.toml` exists, there is no `cargo test` to run; the test gate doesn't apply yet — but it kicks in the moment Phase 0 lands a workspace. Do not let it slip.

## Build / test / lint commands

**None exist yet.** Once the workspace is scaffolded (Phase 0), the standard commands per v2 §29 will be:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
cargo bench --bench scaling     # required before completing Phase 6 (v2 §17.9 / §24)
```

Single-test invocation (after workspace exists):

```bash
cargo test -p <crate-name> <test_name> -- --nocapture
```

Do not invent a `make`/`just`/`cargo xtask` runner before the workspace is real. Just call cargo directly.

## Tech stack reference (from v2 §21)

Rust + tokio · quinn (QUIC) · mdns-sd · axum (HTTP+WS) · ed25519-dalek · blake3 · rusqlite (WAL) · serde + ciborium (CBOR on wire) · automerge (CRDT) · rmcp (MCP SDK) · tracing + opentelemetry · SvelteKit (or Leptos) embedded via rust-embed.

Single-binary outputs target ~20–40 MB stripped, cross-compiled via `cross` / `cargo-dist` for {linux, macos, windows} × {x86_64, aarch64}.

## Persistent memory (ICM) is mandatory

Per `~/CLAUDE.md`, this project uses [ICM](https://github.com/rtk-ai/icm) for cross-session memory. You **must** call `icm store` immediately when:

- An error is resolved → `icm store -t errors-resolved -i high`
- An architecture/design decision is made → `icm store -t decisions-harness -i high`
- A user preference is discovered or corrected → `icm store -t preferences -i critical`
- A significant task completes → `icm store -t context-harness -i high`
- A conversation hits ~20 tool calls without a store → progress summary

Recall before starting work: `icm recall "query" -t decisions-harness`. Do not store trivial state, build logs, or anything already in this file or the PRDs.

## What not to do

- Do not edit the PRDs to "align with code." If code drifts from spec, fix the code or write an ADR explaining the deviation.
- Do not start Phase 1 (mesh skeleton) before Phase 0 (workspace + CI green + `harness --version` works) is demoably done.
- Do not introduce a broker, message bus, or external DB. The "single binary, no broker, no DB server" property is load-bearing.
- Do not add `--no-verify` to git commands or weaken hooks to push code.
- Do not work on multiple roadmap items in one branch unless `ROADMAP.md` explicitly groups them.
