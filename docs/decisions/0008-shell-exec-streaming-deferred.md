# ADR-0008 — `shell.exec` streaming form deferred to follow-up PR

**Status:** Accepted (2026-05-03)
**Context:** Phase 3.2 of HARNESS_PRD_v2.md / ROADMAP.md.
**Supersedes:** —
**Superseded by:** —

## Decision

The Phase 3.2 roadmap text reads:

> shell.exec capability with streaming output (line-frames over QUIC) + policy check.

We are shipping the capability in two PRs:

1. **PR #24 — synchronous form.** The capability spawns the process, captures stdout/stderr into bounded 1 MiB buffers, returns a single JSON envelope when the child exits or the deadline fires. Policy gate via `harness-policy::PolicyEngine` runs first.
2. **PR follow-up — streaming form (3.2-stream).** Adds `harness.task.partial.<task_id>` over QUIC and a frame emitter callback that replaces the in-memory buffers in `read_capped`. Consumer side lands in PR #24's CLI follow-up (item 3.3).

The roadmap checkbox `[ ] 3.2` remains **unchecked** when PR #24 merges. A new sub-item `[ ] 3.2-stream` is added immediately below `3.2`. Only when both ship does `3.2` get ticked.

## Why split?

- **Demo gate doesn't need streaming.** Phase 3 demos `harness run --all -- uname -a`. The output is < 100 bytes; a single JSON envelope is sufficient. 3.3 (the CLI side) can render the synchronous form today.
- **Streaming is a real protocol surface.** `harness.task.partial.<task_id>` (PRD §13.6 / §14.8) is a wire envelope we don't have a producer for yet. Co-shipping it with the capability doubles the diff and conflates two concerns:
  - capability authoring (this PR): how does a Rust function become a tool the mesh can dispatch to?
  - protocol authoring (follow-up): how do partial results get from a worker to the caller's WebSocket?
- **Atomic-PR rhythm is load-bearing.** The user has been emphatic about one PR per roadmap item — the rhythm makes expert review tractable. A 1500-line PR mixing two new layers gets worse review than two 800-line PRs each focused on one layer.
- **Forward compatibility is preserved.** The synchronous form's `read_capped` reader is structured so the streaming form replaces the in-memory `Vec<u8>` accumulator with a `mpsc::Sender<Frame>` callback. The cap, the timeout, the policy gate, and the manifest are all unchanged. Streaming becomes a localized refactor of `read_capped` plus a new constructor `ShellExecCapability::with_frame_sink(...)`.

## What's the same in both PRs

- Manifest (id, version, cardinality, cost, tags, rate limit, resource hints).
- Policy gate (`Action::Shell { cmd, args }` evaluated on the executing node).
- Input shape (`cmd`, `args`, `timeout_ms`, `cwd`, `env`).
- Env hygiene (`env_clear()` + `BASE_ENV_KEYS` allowlist, user env wins).
- Process spawn discipline (`kill_on_drop(true)`, no shell parsing, `Stdio::null()` stdin, `start_kill()` on timeout, concurrent reader tasks to avoid pipe deadlock).
- Per-stream cap (1 MiB) — in synchronous form it bounds the JSON; in streaming form it's a final-buffer cap that triggers a `truncated` frame.

## What changes in 3.2-stream

- `read_capped` becomes `read_streaming(reader, cap, frame_sink)`. Each `read` call emits a `LogFrame { stream: Stdout|Stderr, line: ... }` over the sink.
- New constructor `ShellExecCapability::with_frame_sink(policy, sink)` for streaming callers.
- New wire envelope `harness.task.partial.<task_id>`. Builder + parser + signature + replay protection — same shape as `harness.task.result.<task_id>` minus the terminal field.
- API gateway forwards partial frames to `WS /api/v1/events` for UI consumers.

## What is _not_ changed by this split

- The Phase 3 demo gate. `harness run --all -- uname -a` works after PR #24 lands plus the 3.3 CLI piece. Streaming makes `harness run --all -- tail -f /var/log/syslog` work but that's not the demo gate.
- Roadmap discipline. We do not tick a half-implementation. The `3.2-stream` line is the explicit owner of the missing half.

## Test that pins the synchronous form

`tests/shell_exec.rs::t09c_partial_output_preserved_on_timeout` — child writes `line1\n` then sleeps 60s; capability times out at 300ms; expected stdout is `"line1\n"` (not empty, not lost). This regression fence guards the `read_capped`/`tokio::select!` shape so the future refactor to streaming preserves the property.

## References

- HARNESS_PRD_v2.md §10.4 (policy), §13.6 (channels), §14.7-14.8 (streaming dispatch + result streaming).
- ROADMAP.md item 3.2.
- `.planning/phase-3.2-shell-exec.plan.md` (rust-expert round-2 review).
- PR #23 (Phase 3.1 — policy engine, the gate `shell.exec` consumes).
