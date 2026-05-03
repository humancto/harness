# Harness

> A LAN-native agent mesh. A single Rust binary on every machine you own; auto-discovers peers; elects a brain; runs typed, capability-routed tasks across the fleet.

**Status: Phase 0 — workspace bootstrap.** The full design lives in [`HARNESS_PRD_v2.md`](./HARNESS_PRD_v2.md). The shipping plan is in [`ROADMAP.md`](./ROADMAP.md). Where we are right now is in [`STATE.md`](./STATE.md). Operating instructions for AI agents working in this repo are in [`CLAUDE.md`](./CLAUDE.md).

## The pitch

Harness is a local-first agent mesh for the machines you already own. A small Rust binary on each laptop, desktop, and server auto-discovers its peers, elects a brain (powered by your local LLM, with cloud as escalation), and turns your idle compute into a private agent fleet. Tasks are typed, capability-routed, and parallelized — searches federate across every node that has data, while stateless work runs wherever there's headroom. Intensive workloads (embed 100k docs, grade 5k LLM outputs, triage 50 issues) scale linearly with node count: N machines × X each. Nothing leaves your network unless you say so.

## Three load-bearing properties

1. **Local-first brain.** The planner runs on a local LLM by default. Cloud is escalation.
2. **Capability cardinality is first-class.** Every capability declares `Anyone | Owner | Federated`. Routing is automatic.
3. **N×X scaling.** Streaming dispatch, bounded channels, resource-aware scheduling, checkpoint/resume, batched local inference, real-time cost tracking.

## Building

Requires Rust `1.85.0` (pinned in [`rust-toolchain.toml`](./rust-toolchain.toml); `rustup` will auto-install on first `cargo` invocation).

```bash
cargo build --workspace
cargo test --workspace --all-features
cargo run --bin harness -- --version   # → harness 0.0.0
```

CI runs `fmt --check`, `clippy -D warnings`, and the full test suite on macOS + Linux.

## Contributing

See `CLAUDE.md` for the operating model (it applies to humans too — the workflow is the same). TL;DR: tests with every PR, no exceptions; one roadmap item per branch; expert-agent review is the merge gate.

## License

Dual licensed under [MIT](./LICENSE-MIT) **OR** [Apache-2.0](./LICENSE-APACHE), at your option. This matches the Rust ecosystem default and lets downstream consumers pick whichever fits their project.
