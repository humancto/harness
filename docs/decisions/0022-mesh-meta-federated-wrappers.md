# ADR-0022 — mesh.grep / mesh.search federated wrappers (3.11)

**Status:** Accepted (2026-08-22)
**Context:** Roadmap 3.11, PRD §16.8 (mesh meta-capabilities), §14.6 (federated lifecycle).

## Shape

`mesh.grep` / `mesh.search` are ordinary capabilities (`Cardinality::Anyone` — any node can
coordinate; the data access happens on owning nodes through `fs.*`, which stay `Owner`).
The fan-out unit is a **(node, scope) pair**, discovered from the stored `NodeManifest`s
(self included) filtered to live nodes advertising the wrapped `fs.*` capability.

## Execution split and its limits

A wrapper executes inside the local executor (holding one of its 2–8 permits) while its
sub-tasks need executor capacity too — self-targeted sub-tasks could starve behind
concurrent wrappers. Resolution: **self-owned scopes execute in-process** through
`WeakCapabilityRegistry::get` (no task row, no second permit, no policy delta — `fs.*`
carries no policy hook by design, ADR-0015), bounded to 4 concurrent scans per wrapper
call; **remote scopes become real pinned sub-tasks** (`Task.parent` = wrapper id,
`retry.max_attempts = 1`) routed by the `DispatchRuntime`, so they consume the *remote*
node's executor capacity only.

**What this does NOT eliminate (review MAJOR-1): a bounded cross-node wedge.** Remote
executor capacity may itself be held by wrappers: with N ≥ permit-count concurrent mesh
queries on A awaiting sub-tasks on B while B's permits are held by wrappers awaiting
sub-tasks on A, both executors stall until the wrappers' timeouts (default 30 s, max
120 s) fire — during which all local execution queues behind coordination-idle wrappers.
Nothing deadlocks permanently (the timeout always breaks the cycle), but the window is
real. Follow-up (Phase 4.5 `FanoutController`): release the executor permit while a
wrapper is purely awaiting remote results, or introduce a separate coordination permit
class.

## Orphaned sub-tasks

The wrapper's await deadline equals the sub-task's `timeout_ms`, while the sub-task's
lease TTL is `timeout_ms + slack` — so on a wrapper timeout the remote keeps executing
and its result lands later as an unconsumed (but `parent`-linked, identifiable) Done row
on the coordinator. `max_attempts = 1` on sub-tasks exists precisely so lease expiry
cannot schedule *additional* posthumous work.

## Merge semantics

- `mesh.grep` → **Concat** (PRD default): per-scope result blocks annotated
  `{node, node_name, scope}`.
- `mesh.search` → flatten hits, annotate origin, **sort by bm25 score descending**, truncate
  to `limit`. The PRD's *Rerank* default requires a reranker capability — Phase 5; score-sort
  is the documented degradation until then.
- Failures never hide successes: per-target errors land in `failures[]` while the merged
  results return (`ReturnPartial` semantics); `provenance` carries
  `{targets_ok, targets_failed, truncated_targets}`.

## Bounds

≤ 64 (node, scope) pairs per call — the excess count is **reported** in
`truncated_targets`, never silently dropped. Global `timeout_ms` (default 30 s, clamp
1–120 s) applies to local sub-calls (via `tokio::time::timeout`) and remote awaits alike.
Recursion guard at both layers: the wrappers only ever name `fs.*` sub-capabilities, and
`StoreMeshExec::run_local` refuses `mesh.*` ids outright.

## Liveness caveat

Targets require a recorded heartbeat (`PeerTable`), not just a stored manifest — a node
whose manifest arrived but whose first heartbeat hasn't lands outside the fan-out for up to
one heartbeat interval (~2 s) after connect. Deliberate: dispatching to a node we cannot
see alive converts into a slow failure instead of a skip.

## CLI

`harness search "<query>"` / `harness grep "<pattern>"` (PRD §19) submit one wrapper task
to the local daemon and render `[node/scope]`-prefixed results; per-target failures and
truncation go to stderr; empty result set exits 1.

## Deferred

Streaming partial results to the caller while the fan-out drains (PRD §14.6 steps 3) —
Phase 4.5's `FanoutController` + `PartialResult` plumbing. `mesh.find_file` /
`mesh.embed_lookup` / `mesh.stat` (PRD §16.8) — later phases alongside their underlying
capabilities.
