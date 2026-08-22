# ADR-0020 — 3.2-stream: shell.exec line-frame streaming over `harness.task.partial`

**Status:** Accepted (2026-08-22)
**Context:** Roadmap item 3.2-stream — the second half of 3.2 that ADR-0008
deferred. PRD §13.6 / §14.8.
**Supersedes:** — (completes ADR-0008; refines its per-task-channel sketch
per ADR-0017's aggregate-channel model)

## Shape of the feature

`shell.exec` gains a streaming form (`ShellExecCapability::with_frame_sink`)
that emits `LogFrame { stream: stdout|stderr, line }` through a `FrameSink`
callback as child output arrives, while preserving every synchronous-form
property: per-stream 1 MiB cap, timeout + `start_kill`, env hygiene,
`kill_on_drop`, partial-output-on-timeout (t09c), and the unchanged terminal
JSON envelope — frames still accumulate into the final buffers. The default
constructor stays synchronous; existing users see no behavior change.

The daemon routes frames via one `PartialStreamer`:

- **Locally-issued task** → append directly into the shared in-memory ring
  (`PartialBuffers`), no wire hop. `harness run` against self sees streaming
  data through the same API surface as remote runs.
- **Remotely-issued task** (we are the worker; the `DispatchRuntime` holds a
  reply obligation) → coalesce onto the new **aggregate** wire channel
  `harness.task.partial` (one stream per peer pair per direction, `task_id`
  in the payload — ADR-0017's model, not ADR-0008's per-task
  `harness.task.partial.<id>` sketch). Payload is the existing signed
  `harness_core::PartialResult` (`progress: 0.0`,
  `output_chunk: {"frames": [{"stream","line"},…], "dropped"?: n}`), stamped
  with the per-stream seq and signed by the `PeerNet` sender task at wire
  time exactly like assign/claim/result. Frame cap: 64 KiB.

The issuer's `TaskChannelHandlers::on_partial` (default no-op, so handler
impls without streaming keep compiling) validates that the sender is the
node the task is assigned to, unpacks the batch, and appends individual
frames into its `PartialBuffers`. `GET /api/v1/tasks/{id}` serves the ring
as a `partials` array (chosen over a dedicated `/partials` endpoint: the
CLI already polls this route, and the array is omitted when empty so
non-streaming responses are byte-identical). CLI live-tail and UI
consumption are 3.3-ui / later — this item exposes the data.

## Bounds and their reasons

- **Line size ≤ 8 KiB (`MAX_LINE_BYTES`)**: an unbroken run with no `\n`
  (e.g. `head -c 10M /dev/zero`) is chunked at 8 KiB, so the pending-line
  buffer and every frame stay bounded.
- **Frames mirror the kept bytes only**: nothing past the 1 MiB per-stream
  cap is framed. The frame stream can never leak output the terminal
  envelope would truncate.
- **Worker-side pending queue: 256 frames per task, drop-oldest**: newest
  output is the most useful progress signal; drops are counted and reported
  in the next batch's `dropped` field. Beyond the queue nothing blocks —
  the capability's reader tasks are never backpressured by the wire.
- **Flush cadence 50 ms, one coalesced send per task per tick** → ≤ 20
  wire sends/second/task, per the rate bound. Per-batch raw-byte budget
  4 KiB (checked before adding a frame; a batch tops out < 12 KiB raw),
  so worst-case JSON-escaped payloads stay far inside the 64 KiB frame cap.
  Over-budget remainders stay queued for later ticks.
- **Issuer-side ring: 500 frames per task (`RING_CAPACITY`), 256 task
  entries (`MAX_TRACKED_TASKS`), oldest-evicted**: bounded daemon memory
  regardless of task count or chattiness.

## Per-task seq

`PartialResult::seq` on the wire is the **per-stream** counter (shared
across tasks multiplexed on the aggregate channel) — it exists for
transport replay protection (`Sequenced` + the `ChannelStream` replay
slot), mirroring `TaskResultMsg`. Per-task ordering is inherited from
stream order (one QUIC stream is FIFO). The *per-task* seq consumers see
is assigned by the ring buffer at append time: monotonic from 0 per task,
still counting across ring eviction so a reader can detect the gap
(first retained frame's seq > 0 ⇒ that many frames were evicted).

## Fire-and-forget — no replay/recovery

Partials are progress telemetry, not state. A full outbound queue, dead
connection, dropped batch, evicted ring entry, or daemon restart loses
frames **by design**: there is no ack, no persistence, no re-send, and the
issuer never requests missed ranges. The terminal `TaskResultMsg` /
`FinalResult` — which carries the complete (capped) stdout/stderr — is the
authoritative record. Recovery machinery would duplicate the result path
for data whose value is its immediacy.

## Local-path buffering

The local path bypasses the coalescer entirely and appends straight to the
ring: no wire hop means no reason to batch or rate-limit (the ring bounds
memory), and the API sees local frames with the lowest possible latency.
Consequence: a local `partials` array can be fresher than a remote one by
up to one flush tick — acceptable for an observability surface.

## References

- ADR-0008 (the deferral + forward-compat contract this fulfills).
- ADR-0017 (aggregate channels, per-stream seq/sign discipline, outbound
  queue rules).
- `crates/harness-capabilities/src/shell.rs`, 
  `crates/harness-daemon/src/partial_stream.rs`,
  `crates/harness-api/src/partials.rs`.
