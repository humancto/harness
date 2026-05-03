# ADR-0011 — LLM micro-batcher: honest scope and forward-compat

**Status:** Accepted (2026-05-03)
**Context:** Phase 3.5 of HARNESS_PRD_v2.md / ROADMAP.md.
**Supersedes:** —
**Superseded by:** —

## Decision

Ship dedup-within-window + per-model serialization gate + interactive-tag bypass + forward-compat hook for true batched backends. PRD §16.2's "~3-8× throughput on batched-friendly hardware" claim is aspirational and depends on backend support that Ollama does not offer; revisit when a vLLM / TGI / llama.cpp-batched backend lands.

## Why scope reduced

PRD §16.2:

> Built-in micro-batcher: requests for same model wait up to batch_window_ms (default 50) for siblings, dispatch in batched call. ~3–8× throughput on batched-friendly hardware.

The phrase "**batched-friendly**" is doing real work — Ollama is not batched-friendly. Its `/api/generate` endpoint accepts one prompt per call and serializes per-model server-side. To deliver the throughput claim we need a backend that accepts `prompts: [...]` in a single call (vLLM, TGI, llama.cpp's batched-inference mode).

Implementing the full claim against Ollama requires either:

1. A different backend (out of scope for 3.5; explicitly Phase 3.6+).
2. A multi-prompt gateway in front of Ollama that simulates batching by issuing parallel single-prompt requests (no throughput benefit).

Neither is the right place for 3.5.

## What we ship instead

1. **Dedup within window:** identical `(model, fingerprint)` requests within `batch_window_ms` coalesce into one backend call. Real win for `brain.plan` (3.8/3.9) re-issuing identical planning prompts (template-based planning, retries, fan-out validation).
2. **Forward-compat hook:** dispatch closure today is `FnOnce() → Fut` for one prompt. When a vLLM backend lands, the closure's signature grows to `FnOnce(Vec<Fingerprint>) → Fut<Vec<Result>>` and the same batcher becomes a true batcher. Migration is contained.
3. **`tag:interactive` bypass** (PRD §16.2 explicit): `ExecutionContext::tags` containing `"interactive"` skips the batcher entirely.
4. **`HARNESS_LLM_BATCH_WINDOW_MS=0` disables** the batcher entirely (operator off-switch).

## Architectural choices pinned in code

### Spawned timer task owns drain-and-dispatch

The first caller's `submit()` future returns its `oneshot::Receiver`. The window timer is a `tokio::spawn`-ed task with its own `Arc<Mutex<HashMap>>` clone — independent of the first caller's lifetime. **First-caller cancellation does NOT strand siblings.** `tests/llm_batcher.rs::t07_first_caller_cancellation_does_not_strand_siblings` is the regression fence.

### Slot removed before `dispatch().await`

The spawned task removes the slot from the map _before_ awaiting the dispatch closure. Siblings arriving during a slow inference start a fresh batch — they don't attach to a slot that's about to be drained. Long-running inferences (30+ seconds) don't extend the original batch's window.

### Errors fan out as `CapabilityError::Failed(String)`

`CapabilityError` is not `Clone` (some variants hold non-Clone types). The fanout uses `clone_result` which projects errors via `to_string` into a `Failed` variant. This loses `Failed`/`InvalidInput`/`Cancelled` discrimination for siblings. Acceptable for 3.5 because the only realistic dispatch error is `Failed` (HTTP / decode). When retry-policy lands and needs the discrimination, switch to `Arc<CapabilityError>` fanout. TODO marker on `clone_result`.

### Fingerprint is hand-listed, with `// FINGERPRINT_FIELDS` discipline

The fingerprint over `(model, prompt, system, temperature, max_tokens)` is hand-listed in `fingerprint_for`. Adding a new output-affecting field to `LlmLocalInput` (e.g. `seed`, `top_p`, `top_k`) **requires** updating the helper. The `LlmLocalInput` struct carries a `// FINGERPRINT_FIELDS` block-comment that flags the contract for any future modifier.

Auto-deriving from the struct via canonical serialization is the better long-term answer — but requires a stable canonicalizer that doesn't depend on the workspace's `serde_json` feature flags. For 4 fields the discipline is sufficient. Migration target when more fields land.

### Mutex contention envelope

`parking_lot::Mutex` over a small `HashMap`. Critical section is HashMap lookup + `Vec::push` (~1µs). At 1000 req/s that's 1ms aggregate per second — invisible. **Operating envelope:** correct up to ~10k submits/s. Above that, switch to `DashMap` or per-shard locking.

## Tags moved to `ExecutionContext`

Phase 3.4 N2 dropped `tags` from `LlmLocalInput` because the capability didn't consume them. 3.5 needs `interactive` for the bypass. Rather than re-add a per-capability input field, **`tags` now lives on `ExecutionContext`** — Task-level routing/scheduling/policy metadata, populated by the executor from `Task::tags`. This is the right abstraction long-term:

- Tags affect _routing_ and _scheduling_, not the LLM's reasoning.
- Future capabilities (`brain.plan` 3.8/3.9, `mcp.proxy` 3.7, `fs.write` 3.10) will read tags too — putting them on every input field would balloon every capability's schema.
- Old clients that don't set `tags` get `Vec::new()` everywhere via `#[serde(default)]`.

Wire-format evolution is graceful in mixed-version meshes:

- New daemon, old-encoded task → `tags = Vec::new()` (no hint applied; runs through the batcher).
- Old daemon, new-encoded task → ciborium ignores the unknown field; the bypass hint is silently dropped, and the task runs through the batcher.

Both cases are slower-but-correct. Best-effort across versions.

## What we don't ship

- **True multi-prompt batching to Ollama** (impossible).
- **Adaptive window sizing** — Phase 6 cost-tuning.
- **Cancellation propagation to in-flight backend calls** — when all siblings cancel, the dispatch still runs to completion. Worth a future improvement; not 3.5.
- **Cost-tracking integration** — Phase 5 cost ledger doesn't exist yet. When it lands, the dispatch closure's signature can grow `Vec<TaskId>` so cost is split N ways. Forward-compat hook documented here.
- **Shutdown-signal plumbing** — spawned timer tasks complete their windows after daemon shutdown signals. Bounded: `window + max_inference_duration`. Phase 6 hardening.

## PRD amendment marker

PRD §16.2's throughput claim is aspirational pending a true-batching backend. When 3.6 / vLLM / TGI lands, this ADR is the migration target.

## References

- HARNESS_PRD_v2.md §16.2 (LLM caps + micro-batcher).
- HARNESS_PRD_v2.md §17 (N×X scaling thesis — bounded channels, micro-batching is one of the levers).
- ROADMAP.md item 3.5.
- `.planning/phase-3.5-llm-batcher.plan.md` (rust-expert round-2 review).
- ADR-0008 (3.2 / 3.2-stream split — same pattern of honest scope vs aspirational backend support).
- ADR-0010 (`Action::Llm` extensibility — we extend the same enum for batcher-aware policy in the future).
