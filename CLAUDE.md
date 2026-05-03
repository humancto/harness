# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## How to operate in this repo (read this first — no exceptions)

**This is a serious product, not a toy. Ship every phase the PRD describes. Do not stop at Phase 1 / "laptops see each other".** The user has been emphatic, repeatedly: do the full Phases 1→6 march, then Product Hunt. You are authorized to proceed without asking.

### Default behavior — autopilot

1. **Don't ask permission to continue.** Pick the next unchecked `ROADMAP.md` item, plan it, get rust-expert review, branch, implement, PR, expert-review the diff, merge, flip the checkbox, move on. Loop until either (a) the roadmap is fully checked, (b) a Hard Gate stops you (see `~/.claude/CLAUDE.md`), or (c) the user explicitly says stop.
2. **No analysis paralysis.** If a decision is reasonable and reversible, make it and proceed. Document the choice in the plan file or an ADR. Examples of decisions you should make on your own: bind addresses, default ports, log paths, file naming, CSS framework choice (Tailwind), serialization detail (camelCase vs snake_case in DTOs), test count.
3. **External-service items are not blockers — call them out, scaffold the integration, move on.** When you hit Phase 5.5/5.6/5.7 (WhatsApp/SMS/iOS Shortcuts), you implement the webhook handler + signature verification + tests using mock signatures, document in the PR description that production cutover requires a Twilio/Apple-Developer account. You do not stop and ask.
4. **Use parallel agents when items are independent.** Phase 3 capability implementations (3.2/3.4/3.6/3.7/3.10), Phase 5 external adapters (5.5/5.6/5.7), Phase 6 packaging (6.8/6.9/6.10) — these are leaves and can be drafted/implemented by parallel rust-expert agents. The spine (Phase 2 → 3.1 → 4 → 5.8-5.13) is sequential.
5. **Tests with every PR. Real tests. No `#[ignore]` to hide bugs.** "Trust CI" is not a test plan. The user has been explicit about this in all caps.
6. **Production-grade quality. No fake successes, no half-implementations, no `unimplemented!()` left in `main` paths.** External reviewers (Codex, etc.) read this code.
7. **One PR per roadmap item** unless ROADMAP groups them. Squash-merge with `gh pr merge --squash --delete-branch`. Flip the checkbox in `ROADMAP.md` on `main` after merge.
8. **STATE.md is the running log.** Update it on every merge. Carry forward unfinished sub-items as explicit "Open decisions / carried risks" — never silently drop work.

### What you should NOT do

- Do **not** repeatedly check in with the user asking "should I continue?" / "any more blockers?" / "should I do phase X?". The answer is always **yes, keep going**.
- Do **not** propose to ship a reduced scope ("just the MVP", "just laptops seeing each other") and call it done. The full product per PRD is the bar.
- Do **not** stop the loop because a phase is "multi-day work." Multi-PR is fine; one PR per item lets the loop progress visibly. Sleep / context exhaustion will end the session naturally; until then, keep merging.
- Do **not** treat external-account-required items as blockers. Build the code, mock the external. Document the production cutover.
- Do **not** edit the PRDs to align with code. If code drifts, fix the code or write an ADR.

### When you genuinely must stop

Only the **Hard Gates** in `~/.claude/CLAUDE.md` step 11 stop the loop:

- All ROADMAP items checked.
- No matching expert agent exists for the next item's stack — propose one.
- Tests persistently fail (after good-faith fixes).
- Merge conflicts you cannot cleanly resolve.
- A Hard Gate from the PRD's "Stopping conditions" section below (protocol break, security regression, etc.).

In any of those cases, write the blocker into `STATE.md`, then stop and surface the question. Otherwise: keep going.

## Repository state — historical

This repo started PRD-only. As of 2026-05 Phase 1 is complete (mesh skeleton: identity, protocol, mDNS, QUIC, heartbeats, election, pairing, peers.toml, CLI). Phase 1.10 (UI) and Phases 2–6 are in flight.

Source documents (do not edit):

- `HARNESS_PRD_v2.md` — **canonical spec**. v1 + addendum consolidated. Read this.
- `HARNESS_PRD.md` — v1, kept for diff/history. Superseded.
- `HARNESS_PRD_v2_addendum.md` — the design rationale that drove v1 → v2.

Treat v2 as authoritative. When v1 and v2 disagree, v2 wins.

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
