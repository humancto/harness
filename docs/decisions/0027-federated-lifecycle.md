# ADR-0027 — Federated execution lifecycle (4.5)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 4.5, PRD §13.2/§13.4 (cardinality + provenance),
§14.6 (federated stages), §14.8 (result streams). Closes ADR-0017's
"Federated → first eligible node" stopgap, ADR-0022's cross-node permit
wedge (debt (a)) and ADR-0024's deferred spawned-driver bridge (debt
(d)).

## What ships

`Cardinality::Federated` dispatch is real: `DispatchPlan::Federated`
parents are claimed by a per-daemon `FederatedCoordinator`
(harness-daemon `federated.rs`), fanned out to every eligible node as
pinned signed sub-tasks over the 4.1 `FanoutController`, streamed as
`Progress` frames through the 4.2 partial pipe, merged by the new
**pure `harness-merge` engine**, and terminalized with per-node
`NodeContribution` provenance persisted in a new store column (V0006)
and served through `FinalResult.provenance` (an existing signed wire
field — **zero wire changes**) and `GET /tasks/:id`.

First real federated capability: **`mesh.info`** (fleet inventory,
`Concat` + `ReturnPartial`). Its per-node payload (hostname, os, arch)
is already broadcast in every signed `NodeManifest` — no new exposure.

## Decisions

1. **Atomic coordination claim** (review BLOCKER-1). The coordinator
   claims the parent with one CAS — `UPDATE tasks SET state='running',
   assigned_node=self WHERE id=? AND state='submitted'`
   (`Store::try_start_coordination`). The parent skips
   `dispatched`/`claimed` (a documented synthetic hop, precedent
   ADR-0017): it is never observable at `Dispatched(self)`, so the
   local executor cannot double-claim it. A losing claimant releases
   its coordinator slot and does nothing.

2. **Merge-time failure-policy semantics.** The controller drives
   `Wait` like `ReturnPartial` (never end early; the global deadline —
   the parent's own `timeout_ms` — always binds). Differentiation
   happens when the fan-out settles:
   - `FailFast`: first non-Ok ends the fan-out (in-flight dropped);
     parent Failed; unsettled nodes are `Skipped` (the coordinator
     chose not to wait — they didn't run out of time).
   - `Wait`: any non-Ok ⇒ parent Failed with **full provenance** and
     per-node errors flattened into the bounded error string
     (`"federated wait: k/N nodes ok; <node>: <err>; …"`, ≤5 errors,
     ≤160 chars each — review MAJOR-1).
   - `ReturnPartial`: Ok outputs merge; zero Ok ⇒ Failed; non-Ok nodes
     ride the merged output's `merge.failures` block
     (`[{node, status, error}]`, mesh_meta precedent).
   Honest limitation: sub-tasks carry `max_attempts: 1` (ADR-0022
   posthumous rule), so `Wait` cannot wait through a retry —
   retry-aware Wait is 4.6's backoff work. At the deadline,
   pulled-but-unsettled nodes are `TimedOut`; never-pulled are
   `Skipped`.

3. **Execution classes** (the ADR-0022 wedge fix). `Capability` gains a
   defaulted `execution_class() -> ExecutionClass` (`Work` |
   `Coordination`); `mesh.grep`/`mesh.search`/`plan.execute` override
   to `Coordination`. The executor holds a separate 16-permit
   `coord_sem`: coordinators never hold a work permit, so awaited
   sub-tasks always find executor capacity — the cross-node wedge cycle
   cannot form. The class is peeked **before** the CAS ladder with
   `try_acquire_owned` (no TOCTOU), and skipped rows don't consume
   POLL_BATCH slots (review MAJOR-2), so a full coordination pool never
   starves `Work` rows. A release-guard alternative (drop the work
   permit mid-execute) was rejected: it threads permit state through
   the panic boundary and makes accounting dynamic and unauditable.

4. **Merge engine conventions** (`harness-merge`, pure). Item
   extraction: JSON array = its items; object with `"items"` array =
   that array; anything else = one opaque item. Input order (nodes
   sorted by `NodeId`) is the Dedupe first-wins and TopK stability
   order. `Rerank` degrades to `TopK{score_field: "score"}` until a
   reranker capability exists (Phase 5); `Custom` returns a typed
   `CustomUnsupported` error. `Aggregate` over zero numeric items
   errors (`AggregateNoData`) for `Min`/`Max`/`Mean` alike — a silent
   `0.0` extremum is indistinguishable from a real one (diff review);
   `Sum`/`Count` legitimately yield 0. Merged items cap at 10 000 with
   **reported** truncation. `NodeContribution.item_count` counts items
   CONTRIBUTED (pre-merge) — `Dedupe`/`TopK` may surface fewer than
   the sum.

5. **Provenance persistence** (store V0006). `task_results.provenance
   TEXT` holds JSON `Vec<NodeContribution>`; NULL for single-node
   results. `build_final_result` fills the existing signed wire field
   from the row. Old binaries reading a V0006 database are unaffected
   (explicit column lists everywhere). A plain (non-provenance) retry
   write clears the column — the replacing terminal owns the row.

6. **Bounds.** ≤8 coordinators/daemon (no slot ⇒ the task stays
   `Submitted` in the dispatch batch's WAITING class — behind fresh
   work, so a burst of queued federated parents can't starve other
   Submitted tasks — and retries next poll: queueing, never failure,
   the ResourceGated doctrine); ≤64 nodes/fan-out (reported truncation);
   window = `default_per_workers` ⇒ ≥N for N ≤ 64 (PRD's "all eligible
   in parallel") with remaining-budget timeouts (1 s floor) for late
   pulls; the 4.2 `into_channel` bridge (bounded mpsc = window) is the
   detached consumer ADR-0024 waited for.

7. **Pin rule.** A pinned Federated task yields `DispatchPlan::Single`
   — it executes, it does not re-coordinate (otherwise every sub-task
   would recursively fan out). `Anyone`/`Owner` + pin are untouched.
   This is also the mixed-version wire path: an old issuer routing a
   federated task to one worker gets a plain single-node execution.

8. **Stages are frames, not states.** PRD §14.6's
   DISCOVERED/STREAMING/MERGING ride as `Progress` frames
   (`{"federated": {"stage": …}}`) through the existing ring — not new
   persisted `TaskState`s (schema + LWW churn for display-only info).

9. **`ExecutionPolicy.on_partial` vs capability `on_node_failure`:**
   the capability's declaration is authoritative for federated parents;
   the task-level knob is ignored (task-level override deferred).

10. **Driver panic boundary.** The spawned coordination task wraps
   `drive()` in `catch_unwind` (the executor precedent): a panic in
   the sink closure, merge, or terminal writes terminalizes the parent
   `Failed` ("federated coordinator panicked: …") instead of stranding
   it at `Running` until reboot (diff review). The coordination slot is
   released either way.

11. **Timing anchors.** The global budget (`timeout_ms`, 1 s floor) is
   anchored at coordination START, not submit — a parent that queued
   for a slot keeps its full budget; `constraints.deadline` stays a
   dispatch-loop concern. Terminal-state → result-row write gaps are
   absorbed by `await_terminal`'s bounded grace polls (10 × 100 ms)
   rather than surfacing as false sub-task failures.

## Known carries (owner: 4.6 unless noted)

- **Leaseless `Running` parent**: a daemon crash mid-coordination
  leaves the parent `Running` with no lease. Pre-existing shape (the
  executor has the same window); 4.6's supervisor sweeps
  `Running(assigned=self)` orphans at startup and adds parent leases.
- Federated sub-task dispatch writes the RR `dispatcher_cursors` row
  once per pinned sub-task, though Federated routing never consults RR
  — N harmless store writes per coordination (cosmetic; clean up with
  the 4.6 dispatch work).
- Retry-aware `Wait`; lease extension for long coordinations;
  send-failure backoff.
- Federated fan-outs are pressure-blind (bypass `eligible_scored` —
  all eligible nodes fan out); ADR-0026's federated-scoring follow-up
  re-deferred to 4.6.
- Coordinator self-load double-count: the parent (Running, assigned
  self) and its self-pinned sub-task both count in `count_inflight_by_node`
  while a coordination runs — conservative, bounded by slot count.
- `must_be_local` remains the documented Phase-2 stub in
  `apply_constraints` (never enforced for any cardinality); when 5.x
  wires tag-based locality, federated candidate narrowing inherits it
  automatically because constraints run before the cardinality match.
- `mesh.grep`/`mesh.search` keep their ADR-0022 JSON provenance shape;
  only their merge helpers converge on `harness-merge` (Phase 5 may
  rewrite them onto the engine).
- Cost aggregation into merged `FinalResult.cost` — Phase 5 cost
  tracking. UI rendering of provenance/stages — 4.8 (types.ts contract
  shipped here).
- `ExecutionContext` FrameSink promotion (ADR-0024 debt (e)) — cut to
  4.6's first commit; the coordinator uses the streamer sink directly.
