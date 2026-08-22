# ADR-0024 — Result streams: Stream<TaskResult> + live progress (4.2)

**Status:** Accepted (2026-08-22)
**Context:** Roadmap 4.2, PRD §14.8 (callers see `Stream<TaskResult>`; "UI
consumes same stream over WebSocket"), building on 4.1's `FanoutController`
(ADR-0023) and the 3.2-stream partial pipeline (ADR-0020).

## The caller stream

`harness_orchestrator::results::task_results(stream, ctx, mapper)` maps a
`FanoutStream` into the §14.8 `Stream<TaskResult>`: one
`TaskResult::Partial` per completed item (monotonic `seq` from 0,
`progress = completed / expected`), then exactly one summary `Partial` at
`progress = 1.0`, then termination. Pure combinator — backpressure and
Drop-cancellation are inherited from the pull-based inner stream.

Two deliberate limits:

- **Never `TaskResult::Final`.** The signed `FinalResult` written by the
  executor (`write_task_result_done`) stays the single terminal authority
  per task; the adapter's partials are telemetry. `sig` on emitted
  partials is zeroed — signing happens at wire boundaries, as in 3.2.
- **No spawned-driver + bounded-mpsc bridge.** The 4.1 planning notes
  anticipated one "where a detached consumer requires one"; no detached
  consumer exists (the WS reads the `PartialBuffers` ring, below), so the
  bridge is **deliberately not built**. Owner: 4.5, whose federated
  lifecycle is the first detached consumer.

The mapper is a single recoverable object (`ResultMapper::{on_item,
on_end}`, taken back via `into_mapper()`), not two closures — a driver
accumulates provenance across both callbacks without shared-state locks.

## Progress plumbing: ride ADR-0020, no new channels

Per-target fan-out progress becomes a third `StreamKind::Progress` whose
`line` is a small JSON chunk. It enters through `MeshExec::progress_sink()`
(defaulted `None`; the daemon wires `PartialStreamer::sink()`), and from
there every hop is an existing bounded 3.2-stream structure: local wrapper
→ straight into the issuer's ring; dispatched wrapper → bounded queue →
coalesced `PartialResult` on `harness.task.partial` → issuer-side
`on_partial` (allowlist widened to accept `"progress"`; unknown kinds
still drop, so mixed-version meshes degrade gracefully) → ring.

The sink rides `MeshExec` rather than `ExecutionContext` (37 construction
sites) or a breaking `Capability` change; promotion to `ExecutionContext`
is deferred to 4.5 when a non-mesh consumer exists.

**Reconciliation with the 4.5 deferral:** ROADMAP 4.5 and ADR-0023 defer
"`PartialResult` streaming" — that means the *federated lifecycle's*
dispatcher-side streaming with per-`NodeContribution` provenance in
`FinalResult.provenance`. 4.2 ships wrapper-side telemetry riding the
existing pipe; 4.5's scope is unchanged.

## Chunk shapes (mirrored as `MeshProgressChunk` in `types.ts`)

Per-target: `{ target: {node, node_name, scope}, outcome: ok|failed|
timed_out, items?, error?, completed, total, ok, failed, timed_out }`.
Summary (exactly one per wrapper call, emitted after both controllers
settle): `{ summary: { total, ok, failed, timed_out, truncated_targets } }`.
`failed` excludes timeouts. Counter atomics are for `Send` bounds only —
both controllers are polled by one `tokio::join!` on the wrapper's task,
so frames are self-consistent and `completed` is monotone.

## Surfaces

- **WS `/api/v1/runs/<id>`** pushes `{"partials": [{seq, stream, line}]}`
  frames (all kinds — stdout/stderr ride along) interleaved with state
  frames, seq-deduped per socket, with a final sweep guaranteed before
  the terminal frame. Bounded by the ring (500/task) per 250 ms tick.
- **`GET /api/v1/tasks/:id`** already served the ring; `TaskDetailDto`
  in `types.ts` now declares it.
- **CLI `harness grep|search`** renders per-target lines to stderr as
  frames arrive (`[3/9] peer/notes: ok (12 matches)`), TTY-gated so
  pipes stay clean, deduped across polls by ring seq. The merged
  terminal output remains the product; no WS client dependency.

## Accepted losses (fire-and-forget, per ADR-0020)

A remote wrapper's terminal `TaskResultMsg` can outrun the final 50 ms
partial flush — the WS may close with the last progress frames
undelivered. The merged output's `provenance`/`failures` are
authoritative. Ring eviction under >256 concurrent tasks loses telemetry
only. A ≤64-target fan-out emits ≤65 frames (~200–300 B each) — inside
every existing bound.

## Deferred

UI live per-node progress bars consuming `RunStreamFrame` → **4.8** (the
typed contract ships here). CLI WS attach → Phase 6. Store-side
notification bus replacing the 250 ms WS poll → Phase 6 hardening.
