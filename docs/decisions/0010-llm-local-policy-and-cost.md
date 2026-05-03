# ADR-0010 — `llm.local.*` policy default-allow + DoS mitigations + Action extensibility

**Status:** Accepted (2026-05-03)
**Context:** Phase 3.4 of HARNESS_PRD_v2.md / ROADMAP.md.
**Supersedes:** —
**Superseded by:** —

## Decision

Three related design choices for the `llm.local.<model>` capability:

1. **Default-allow when `[llm]` is absent in `policy.toml`; default-deny when the section is present-but-empty.**
2. **DoS mitigations:** `RateLimit { per_second: 1, burst: 2 }` in the manifest, `max_tokens` capped at 4096 (input schema), `timeout_ms` capped at 600s.
3. **`Action::Llm { model }` is `#[non_exhaustive]`** and all `Action` variants must remain `Copy`. Future variants (e.g. tags, prompt_hash, role) extend the enum without breaking callers.

## Why default-allow when `[llm]` is absent

`shell.exec` is default-deny because it runs arbitrary code — wrong rule and the worker is owned. `llm.local.*` is default-allow because:

- **The output is text, not arbitrary code.** The worker never executes the model's response.
- **The PRD §10.4 example policy doesn't mention `[llm]`** but clearly expects ollama to work (`[shell] allow = [{ cmd = "ollama", any_args = true }]` is right there). Operators don't write `[llm]` to enable it; they write it to restrict it.
- **The cost surface is compute, not security.** Compute DoS is mitigated by rate limits + max_tokens, not by allowlists.

## Why default-deny when `[llm]` is present-but-empty

| `[llm]` section | `allow` rules                   | Result            |
| --------------- | ------------------------------- | ----------------- |
| absent          | n/a                             | **Allow**         |
| present, empty  | `allow=[]`+`deny=[]`            | **Deny**          |
| present, allow  | match-based                     | first match       |
| present, deny   | match → deny; else default-deny | match deny → deny |

The matrix respects operator intent: writing `[llm]\nallow = []\ndeny = []` is an explicit "I want to restrict this." A `tracing::warn!` would have been invisible on a long-running daemon — the type-level distinction (`Option<LlmPolicy>` on `Policy`) carries the absent-vs-present signal natively.

## DoS mitigations (R1)

`llm.local.<model>` is `Cardinality::Anyone` and default-allow. Without bounds, a `Default`-trust peer could submit 1000 prompts at `max_tokens: 2048` each and pin the GPU for hours. Three caps make this safe:

- **`RateLimit { per_second: 1, burst: 2 }`** in the manifest. Tighter than `shell.exec` (`5/s, burst 10`) because LLM inference is expensive.
- **`max_tokens` schema cap at 4096.** Ollama's largest context windows are 8K-128K, but 4K is a sane default for cost-bounded runs. Operator who needs more can fork the input schema later.
- **`timeout_ms` schema cap at 600_000` (10 min).** Same as `shell.exec`. Prevents runaway prompts.

These are non-optional. The "default-allow because output is text" argument needs the cost-side counterpart: "and bounded by rate + tokens + time."

## `Action::Llm { model }` extensibility

```rust
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Action<'a> {
    Shell { cmd: &'a str, args: &'a [String] },
    Llm   { model: &'a str },
}
```

Two invariants pinned:

1. **All variants are `Copy`** — `EvalContext` is cheap to share across evaluator branches. A future variant adding `Vec<String>` would require rewriting every match site. Use `&'a str` / `&'a [T]`, never `String` / `Vec<T>`.
2. **`#[non_exhaustive]` on the enum** — adding `Llm { model, tags, prompt_hash, role, ... }` later is non-breaking. Today's `Action::Llm { model }` is the right granularity for 3.4; we don't pre-bake fields we don't have evaluators for.

Specifically deferred:

- Tags-as-policy-input (e.g. deny `tag:medical` from external models). Useful when 3.6's cloud LLMs land. Add as a non-breaking variant change.
- Prompt-content matching. Real prompt-injection defense is its own design — `shell.exec`'s pattern-substring approach doesn't translate well.

## HTTP for both discovery and execution

We use HTTP `/api/tags` for discovery and HTTP `/api/generate` for execution. Subprocess-based discovery (`ollama list`) was rejected:

- One transport, one `OLLAMA_HOST` config, one set of failure modes.
- `/api/tags` returns structured JSON with the canonical model names that `/api/generate` expects.
- Daemon launched via launchd may not have `ollama` on PATH but will still reach `127.0.0.1:11434`.
- `ollama list`'s text format isn't a stable contract.

## `gpu_required: false`

PRD §14.9 says capabilities declare `ResourceHints`. Ollama auto-falls-back to CPU when no GPU is present. Declaring `gpu_required: true` would mis-route the scheduler in 3.5+ (GPU-having nodes only) when CPU-only nodes can run the model fine. We declare:

```rust
ResourceHints {
    cpu_class: CpuClass::Heavy,
    memory_mb: Some(8192),
    gpu_required: false,
    ...
}
```

GPU-affinity routing — when 3.5+ wants to prefer GPU nodes — should land via tag-aware routing (capability tags + scheduler-side weighting), not via `gpu_required: true`.

## Forward-compat with the unified `LlmRequest` / `LlmResponse` shape (PRD §16.2)

PRD §16.2 mentions a unified shape but doesn't fully spec it. We define a minimal compatible output shape now:

```json
{
  "text": "string",
  "model": "string",
  "duration_ms": "integer",
  "prompt_tokens": "integer | null",
  "completion_tokens": "integer | null"
}
```

The Ollama field is `response`; we rename to `text` here so the cloud LLM caps in 3.6 (`llm.cloud.claude` etc.) can return the same shape without introducing a separate field convention.

## References

- HARNESS_PRD_v2.md §10.4 (policy), §14.6 (cardinality), §14.9 (resource hints), §16.2 (LLM caps).
- ROADMAP.md item 3.4.
- `.planning/phase-3.4-llm-local.plan.md` (rust-expert round-2 review).
- ADR-0009 (executor lifecycle ladder — same pattern: pin invariants in code + ADR, defer extensions).
