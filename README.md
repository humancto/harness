# Harness

> A LAN-native agent mesh. A single Rust binary on every machine you own; auto-discovers peers; elects a brain; runs typed, capability-routed tasks across the fleet.

**Status: pre-implementation.** The full design lives in [`HARNESS_PRD_v2.md`](./HARNESS_PRD_v2.md). The shipping plan is in [`ROADMAP.md`](./ROADMAP.md). Where we are right now is in [`STATE.md`](./STATE.md). Operating instructions for AI agents working in this repo are in [`CLAUDE.md`](./CLAUDE.md).

## The pitch

Harness is a local-first agent mesh for the machines you already own. A small Rust binary on each laptop, desktop, and server auto-discovers its peers, elects a brain (powered by your local LLM, with cloud as escalation), and turns your idle compute into a private agent fleet. Tasks are typed, capability-routed, and parallelized — searches federate across every node that has data, while stateless work runs wherever there's headroom. Intensive workloads (embed 100k docs, grade 5k LLM outputs, triage 50 issues) scale linearly with node count: N machines × X each. Nothing leaves your network unless you say so.

## Three load-bearing properties

1. **Local-first brain.** The planner runs on a local LLM by default. Cloud is escalation.
2. **Capability cardinality is first-class.** Every capability declares `Anyone | Owner | Federated`. Routing is automatic.
3. **N×X scaling.** Streaming dispatch, bounded channels, resource-aware scheduling, checkpoint/resume, batched local inference, real-time cost tracking.

## Building

There is nothing to build yet. Phase 0 is in flight — see `ROADMAP.md` item `0.1`. Once it lands:

```bash
cargo build --release
cargo run --bin harness -- --version
```

## Contributing

See `CLAUDE.md` for the operating model (it applies to humans too — the workflow is the same). TL;DR: tests with every PR, no exceptions; one roadmap item per branch; expert-agent review is the merge gate.

## License

To be decided in roadmap item `0.3`. Default expectation: dual MIT / Apache-2.0.
