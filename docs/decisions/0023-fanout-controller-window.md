# ADR-0023 — FanoutController: window sizing, refill, cancellation (4.1)

**Status:** Accepted (2026-08-22)
**Context:** Roadmap 4.1, PRD §14.7 ("never materialize all sub-tasks; `FanoutController`
keeps `2 × N_workers` in flight; refill on completion; memory O(window), not O(total)").

## Shape

`harness_orchestrator::fanout::FanoutController` is a **pure, pull-based
`Stream`** — no store, no leases, no runtime handle, no spawned driver task.
The caller supplies a lazy item source (`BoxStream`), a runner closure that
turns one item into one sub-task future, a window policy, a live-worker
probe, an optional deadline future, and a `PartialPolicy`. Choosing a
`Stream` over a spawned-task-plus-channel makes backpressure structural
(a consumer that stops polling stops refill — the 4.7 pre-work) and makes
cancellation `Drop` (dropping the stream drops all in-flight futures).

The controller bounds **futures**; bounding *store rows* follows from the
runner contract: side effects (row insertion, dispatch) happen inside the
returned future, never in the closure body. An unpulled item therefore has
no row, no lease, no QUIC traffic.

## Window sizing

`WindowPolicy::PerWorkers { factor: 2, min: 4, max: 64 }` is the §14.7
default: `clamp(2 × N_live_workers, 4, 64)`. min = 4 keeps single-worker
fan-outs from serializing; max = 64 matches the per-peer outbound queue
bound (ADR-0017) and the mesh-meta target cap. `live_workers` is re-read on
every refill attempt, so the window tracks nodes joining/leaving; shrink is
lazy (nothing in flight is cancelled — refill just pauses until the drain
falls under the new window), growth applies on the next refill.
`WindowPolicy::Fixed(n)` exists for callers with their own bound (and
floors at 1 — a zero window could never progress).

## Refill and termination

"Refill on completion" is literally the consumer's next poll after an
`Item` event: the poll re-checks the window and pulls the source until full
or `Pending`. Termination: source drained + in-flight empty →
`End(SourceDrained)`; deadline resolved → in-flight dropped (counted in
`dropped_in_flight`) and `End(DeadlineExceeded)`; under
`PartialPolicy::FailFast`, the first `Failed`/`TimedOut` item drops all
in-flight work and queues `End(FailedFast)`. A queued terminal `End` is
emitted **before** the deadline is polled, so a deadline firing in the gap
between the fatal `Item` and its `End` cannot rewrite the reason or the
drop count. `PartialPolicy::Wait` is aliased to `ReturnPartial` at the
controller until 4.5 defines federated wait semantics.

## Cancellation and orphans

Cancellation is `Drop` — of the whole stream, or of in-flight futures on
FailFast/deadline. Runner futures must therefore be drop-tolerant. A runner
dropped after `submit_remote` leaves an orphan sub-task row; that is the
pre-existing, bounded ADR-0022 orphan rule (`max_attempts = 1`, sub-task
`timeout_ms` ≤ the wrapper deadline) — no new unbounded work is possible.

## First consumer: mesh_meta (rewired in this PR)

`mesh_meta::fan_out` previously submitted **all** remote sub-task rows
eagerly (`join_all`) before awaiting — O(total) row materialization, the
§14.7 violation in miniature. It now drives **two controller instances**,
polled concurrently:

- **local pairs** through `Fixed(LOCAL_SCAN_CONCURRENCY = 4)` — ADR-0022's
  "bounded to 4 concurrent scans per wrapper call" stands unchanged;
- **remote pairs** through `PerWorkers` over the distinct remote node
  count — sub-task rows now exist O(window) at a time.

Two instances rather than one shared window because the pairs list is
self-first: a single window would let up to 64 concurrent local disk walks
(breaking ADR-0022's bound) and would starve remote submission behind local
scans (losing the deliberate local/remote overlap). Failure provenance
(`failures[]` needs `{node, node_name, scope}`) is resolved by looking up
`FanoutEvent::Item.index` in the wrapper's already-materialized (≤64) pairs
list. Pairs never pulled or dropped mid-flight when a deadline fires get
explicit `deadline exceeded` failure entries — failures never hide
successes, and nothing disappears silently. Merged block order becomes
completion-ordered; the wrapper sorts successes by `(node, scope)` before
merging to restore determinism.

## Deferred

- Public `Stream<TaskResult>` + WS bridge → 4.2 (maps this stream).
- Federated dispatch path, `PartialResult` progress, per-`NodeContribution`
  provenance, real `Wait` semantics → 4.5.
- Queue-depth-aware refill (heartbeat `paused`) → 4.7 — a gate in the
  refill loop.
- Checkpoint/resume of the pull cursor → 5.11 (the `index`-ordered pull is
  exactly the cursor it will persist).
