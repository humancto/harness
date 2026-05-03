# ADR-0006 — Task state replication: custom LWW Map (not `automerge`)

**Status:** Accepted
**Date:** 2026-05-03 (Phase 2.5)

## Context

PRD §21.8 names `automerge` as the default CRDT library, and §27 lists "CRDT vs Raft" as an open question. Phase 2.5 closes that question for **task state replication** specifically.

The state model we need to replicate is:

- **Per-task lifecycle** (PRD §14.1): `submitted → planned → dispatched → claimed → running → done|failed|expired|cancelled`. Forward-only state machine.
- **Per-task terminal data**: `output_preview` (first 256 bytes for the UI), `cost_summary`, `finished_at`, `completed_by`. Set once on terminal transition.
- **Update frequency**: ~one update per task lifecycle event. With Phase 1's 2 nodes and a workload of dozens of tasks/sec, that's <100 updates/sec mesh-wide.

## Decision

**Phase 2.5 ships a custom Last-Writer-Wins map** (`HashMap<TaskId, ReplicatedTaskState>`) with `(state, finished_at, NodeId)` as the merge tiebreak. **Defer adopting `automerge` until Phase 5.13** (audit log) where its general-purpose document model + replay-from-changes property is load-bearing.

The merge function for two `ReplicatedTaskState` values is:

```
merge(a, b):
    1. compare (state.priority, finished_at, source_node_id) lexicographically
    2. higher value wins
```

Where `state.priority` is a fixed total order (`done` > `failed` > `cancelled` > `expired` > `running` > `claimed` > `dispatched` > `planned` > `submitted`). This guarantees a forward-only monotonic state — concurrent transitions converge to the more-advanced state, never going backwards.

`finished_at` (unix milliseconds) is the secondary tiebreak so two nodes that observe the same final state at different wall-clock times still converge deterministically.

`source_node_id` (the node that emitted the transition) is the final tiebreak — `NodeId` is a stable 16-byte identifier with deterministic byte ordering.

## Why not `automerge`?

1. **Dependency weight.** `automerge` 0.5 pulls ~2MB of compiled binary (with WASM-style binary changes infrastructure). Our state machine is monotonic and tiny; we don't need OT-style change replication.

2. **Conflict semantics.** Automerge's default conflict resolution for register fields uses actor-id Lamport ordering. Our state machine has a _natural_ total order (`state.priority`) that we want as the primary tiebreak — overriding automerge's default would mean writing a custom merge anyway.

3. **Phase 2.5 scope.** Replicating one struct per task with monotonic semantics doesn't justify a general-purpose CRDT library. The custom approach is ~200 LOC including tests.

4. **Phase 5.13 (audit log) is where automerge earns its keep.** The audit log is an append-only chained list with cross-node ordering and replay requirements — exactly what automerge's `Sequence<T>` is for. We add `automerge` then.

## Consequences

- 2.5 ships a tiny, dep-free `ReplicatedTaskState` + LWW merge in `harness-store`.
- The wire format is a signed `ReplicaSyncEnvelope { entries: Vec<(TaskId, ReplicatedTaskState)> }` — gossiped over a new `harness.gossip.state` QUIC channel (wired in 2.6 alongside HTTP submit).
- Heartbeats grow a `replica_head: Option<[u8; 32]>` field (blake3 of canonical-encoded state map) for anti-entropy. **Wire-format change**, gated by ADR-0007 (carried as a separate ADR in 2.6).
- 5.13 introduces `automerge` for the audit log. At that point we re-evaluate whether to migrate task-state replication to automerge for consistency, or keep the custom layer because it's cheaper.

## Alternatives considered

- **Raft.** Rejected per PRD §11.4 partition-tolerance requirement: a 2-node mesh with one node offline cannot make progress under quorum-based protocols, and harness explicitly wants workers to keep executing claimed tasks during a partition.
- **`automerge` with default conflict resolution.** Loses the natural state-machine total order; we'd write a custom merge on top of automerge anyway.
- **`yrs`.** Yjs port is text/CRDT-tree-shaped, the wrong shape for our state machine.
- **`crdts` crate.** Unmaintained.
