# ADR-0014 — `brain.plan` LocalFast tier + schema-aware plan validation

**Status:** Accepted (2026-05-04)
**Context:** Phase 3.9 of `HARNESS_PRD_v2.md` / `ROADMAP.md`. PRD §15 defines the planner architecture; this ADR records the design decisions that shaped the LocalFast backend, the JSON-Schema validator dep, and the validation surface.
**Supersedes:** —
**Superseded by:** —

## 1. Why LocalFast + validation ship together

LocalFast emits LLM-shaped plans that _can_ fail validation; Template emits hand-coded shapes that always pass. Without a schema validator the executor has no way to reject a malformed LocalFast plan, and an operator running `brain.plan` with an Ollama model behind it would get garbage at execute time. Test t24 in `crates/harness-capabilities/tests/brain_plan.rs` is the load-bearing artifact: a fake backend emits a plan referencing a never-registered cap, the executor's `validate_plan` returns `UnknownCapability`, and the diagnostic propagates into the final `Failed("...")` message. The two pieces reinforce each other. ADR-0011 sets the precedent for "ship the honest pair."

## 2. Sync `/api/generate`, not streaming

`llm.local.*` (3.4) calls `/api/generate` with `stream: false`. LocalFast inherits the same primitive. Streaming planner output is a §3.2-stream-style follow-up — the prompt-cache + flush semantics need their own design.

## 3. Edge orientation contract

The harness wire convention for `Plan.edges` is `(from, to)` = "from depends on to" (per `crates/harness-core/src/protocol/plan.rs:74-86` and ADR-0002). The natural reading for LLMs is the inverse: given "first do A, then B," the model emits `[["A","B"]]` meaning "A runs before B." LocalFast asks the LLM for the natural reading in the system prompt and **flips** the orientation server-side in `local_fast::build_response`:

```rust
// LLM (A, B) = "A runs before B"
// harness (from, to) = "from depends on to" = "to runs first"
edges.push((*b, *a));
```

`t14b_edge_orientation_flipped` in `crates/harness-brain/tests/local_fast.rs` is the regression guard: a 2-node chain with LLM `[["a","b"]]` materializes as harness edges `[(b_taskid, a_taskid)]`.

## 4. JSON extraction is brace-balanced, not fence-regex

Real LLM output shapes:

- `Here's the plan:\n\`\`\`json\n{...}\n\`\`\``
- `\`\`\`\n{...}\n\`\`\`\nLet me know if you need anything else.`
- Bare `{...}`
- Two JSON blocks (LLM hallucinated two plans)

A fence-stripping regex would fail half of these. `local_fast::extract_json_object` is a brace-balanced state machine that walks bytes tracking string-literal state (escape-aware) and brace depth, returning the slice from the first `{` to its matching `}`. Subsequent JSON blocks are ignored with a `tracing::warn!`. Trailing commas → strict `serde_json` rejects → executor escalates to Template. JSON5 / loose decoding is a 3.9-followup if quality demands it. 7 unit tests in `local_fast::unit_tests` lock the extractor's contract; t15a-t15e cover the wire-shape tolerance.

## 5. Server-mint `TaskId`s

The LLM emits `"id": "step_1"`-style strings. We mint a fresh `TaskId::new_v7()` per node and rewrite both nodes and edges through a `String → TaskId` map. Security: prevents an LLM-hallucinated id from colliding with a legitimate inflight task. UX: the LLM's labels are a useful diagnostic, captured in tracing fields (see §6) but never on-wire.

## 6. LLM step labels go to `tracing`, not `PlanNode.input`

Every `shell.exec`/`echo`/`llm.local.*`/`llm.cloud.claude`/`brain.plan` schema in the codebase uses `additionalProperties: false`. Stashing the LLM's label in `PlanNode.input` as `_step_label` would fail every cap. Adding `PlanNode.label: Option<String>` is a wire-format change out of 3.9 scope. Instead, `local_fast::build_response` emits an `info!` event per rewritten node with `llm_step_label`, `minted_task_id`, and `capability` fields. Operators correlating a planning failure see the labels in their logs without paying a wire cost.

## 7. JSON Schema dep choice — `jsonschema = "0.26"`

`jsonschema` 0.26 is a stable line; the latest is 0.46 but the API churned across the 0.27-0.46 range. The 0.26 surface (`validator_for(&schema) -> Validator`, `Validator::iter_errors`) is sufficient for our use (compile + validate, no remote refs). `default-features = false` drops the optional `cli` and `reqwest` features. ~17 transitive deps; acceptable.

`CapabilitySchemaIndex::from_pairs` compiles every schema **eagerly**; per-`PlanNode` validation pays only the validate cost, not the compile cost. A schema that fails to compile is dropped (with a `tracing::warn!`) — the cap stays in `available_capabilities` so the LLM can list it, but plans referencing it surface `UnknownSchema` at validate time. Adversarial / malformed schemas cannot crash the index build.

## 8. 8KB prompt cap (bytes, not tokens)

Bytes is the only sane unit for a byte-stable prompt-cache key. ~2300 tokens at 3.5 chars/token; reasonable for an 8B model with 8K context. Capabilities list pins `shell.exec, http.fetch, doc.summarize, mesh.search` first (the always-include set), then sorts the rest by id and truncates the tail when the budget is reached. PRD §15.7's "retrieval if >50" is deferred to 3.9-retrieval.

## 9. Confidence threshold lives at the executor

Backends report raw confidence; the brain.plan executor applies the threshold from the merged `PlanConstraints.confidence_threshold`. Per-call override flows through the input; per-mesh default flows from `harness-policy::PlanningPolicy.confidence_threshold` (PRD §15.2 default 0.7) via `BrainPlanConfig.default_constraints`. Below-threshold `Confident(_)` becomes an escalation diagnostic (`"<backend>: confidence 0.6 < threshold 0.7"`), never a hard failure.

This factoring means a future LocalStrong / Cloud tier returning the same confidence convention works without code changes — and a CLI tester debugging a plan can pass `confidence_threshold: 0.0` to bypass the gate entirely.

## 10. `must_be_local` / `cloud_ok` / replanning loop deferred

3.9 does NOT enforce `must_be_local` or `cloud_ok` consistency — that's the dispatcher's job once 3.6-encrypted introduces tag-aware routing. PRD §15.2's `max_replanning_attempts` and `escalate_to_cloud_if` (the retry-with-stricter-prompt loop) are 3.9-replan territory. Documented here so future maintainers don't re-litigate.

## 11. `validate_plan_well_formed` deprecated, not deleted

Downstream callers that only need the structural-sanity subset (acyclic + caps-exist + non-empty + dangling-edge) get a compile warning + the migration target in the deprecation message. Internal `BrainPlanCapability::execute` migrates to `validate_plan` because the cost-cap and schema gates are 3.9's whole point — theatre deprecation does not catch a regression. `PlanNode.capability_version_major` field is intentionally NOT added in 3.9 (ADR-0013 §10 carries the gap forward); JSON-Schema validation transitively pins shape per registered (id, version_major).

## 12. `localfast` feature flag in `harness-brain`

`harness-brain` was previously dep-free of HTTP. Adding `reqwest` + `url` unconditionally would mean every consumer of `harness-brain` (e.g. a future minimal CLI binary that only wants the trait + Template + validate) compiles a network stack. The `localfast` feature gates the `LocalFastBackend` module and its deps. `harness-capabilities`'s `brain` feature enables `harness-brain/localfast` so the daemon's default build gets the full surface. Verified:

```
cargo build -p harness-brain --no-default-features  # no reqwest in deps
cargo build -p harness-brain --features localfast  # full surface
```

## 13. Cost cap is a floor, not a ceiling

`max_cost_usd` is enforced at plan-time as `response.estimated_cost_usd <= max_cost_usd`. The LLM's self-reported estimate can lie; this validator catches the case where the LLM emits a plan IT thinks is too expensive (asks for 50 cloud calls and reports a $25 estimate against a $5 cap). Real-time cost tracking + budget enforcement during execution is the dispatcher's job (PRD §17.x, out of 3.9 scope). A $5 cap doesn't protect against a plan that lies about cost — it protects against a plan whose own estimate is honest and over-budget.

## Consequences

- The mesh has its first LLM-driven planner. A node with `llama3.1:8b` registered and `prefer_local_models = ["llama3.1:8b"]` in its policy now gets multi-step plans for goals more complex than `run: ls`.
- Plan validation is enforced at `brain.plan` exit. A capability whose `input_schema` is `{"type": "object"}` accepts anything (effectively no validation); a schema with `additionalProperties: false` and required fields catches LLM hallucination.
- Operators can debug via `harness submit brain.plan --input '{"goal":"...","constraints":{"confidence_threshold":0.0}}'` to bypass the threshold and see what each backend produced (the diagnostic accumulator names every backend that failed).
- `Action::Plan` policy variant (analogous to `Action::Llm`) is intentionally NOT added in 3.9. Planning is policy-blind by design; the executing node enforces.
