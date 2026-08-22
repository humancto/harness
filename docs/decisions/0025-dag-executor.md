# ADR-0025 — DAG executor: plan.execute, $task_output refs, failure policy (4.3)

**Status:** Accepted (2026-08-22)
**Context:** Roadmap 4.3, PRD §13.5 (Plan envelope), §14.4 (DAG pattern),
§19 (`harness plan` / `harness exec`). Builds on ADR-0002 (edge
orientation), ADR-0013/0014 (planner + validation), ADR-0023/0024
(FanoutController + result streams).

## Shape

Three layers, mirroring 4.1: a **pure `DagScheduler`**
(`harness_orchestrator::dag`) tracks per-step state, computes ready sets
incrementally (Kahn-style over the deduplicated ADR-0002 edges), cascades
failure to transitive dependents, and retains step outputs only while an
unsettled dependent may reference them; a **driver capability
`plan.execute`** (`Cardinality::Anyone`) runs the loop; the daemon's
existing `StoreMeshExec` grows a `PlanExec` impl for its services.

Steps become **ordinary signed, unpinned task rows** (`parent` = the
plan.execute task, `plan_id` = the plan's id — the column's first real
writer) routed by the untouched `DispatchRuntime`: placement by
cardinality over the live mesh, leases, and rule-10
policy-on-the-executing-node all come for free. Row insertion happens
inside the pulled future of a channel-fed `FanoutController`
(`WindowPolicy::default_per_workers`), so at most O(window) step rows
exist at once — never O(plan). Late-pulled steps get the remaining plan
budget (floor 1 s) as their row timeout (the 4.1 rule). Step timeout =
`node.timeout_ms.unwrap_or(30_000)` clamped to that remaining budget.
`MAX_PLAN_STEPS = 64`.

## $task_output references

No output→input syntax existed anywhere (PRD §13.5 is silent; both
planner backends emit literal inputs), so 4.3 introduces the minimal one:

```json
{"$task_output": "<step TaskId uuid>", "pointer": "/optional/json/pointer"}
```

recognized anywhere inside `PlanNode.input`; the object is replaced by
the referenced step's output (or the RFC 6901 pointer into it) before
dispatch. `$task_output` is a **reserved key** — an object containing it
is always a reference; a collision with a capability's legitimate literal
input is accepted and implausible (exact-shape match; no registered
schema uses it). Rules:

- A reference is only legal to a **direct declared dependency** — data
  dependencies ⊆ control dependencies, checked by `validate_plan` and
  again by the executor.
- Ref-bearing nodes **defer plan-time schema validation** (the resolved
  shape is unknown); the driver re-validates the resolved input against
  the entry schema index immediately before minting the step row — a
  recheck failure fails the step and cascades, without dispatching.
  Ref-free nodes keep full plan-time validation.
- A pointer that doesn't resolve in the referenced output fails the step
  (and cascades).
- `validate_plan` also now enforces `tasks[k].id == k`.

## Entry validation (rule 8 — never bypass)

`plan.execute` re-runs `validate_plan` on every plan it receives, however
trusted the caller. The capability set and schema index for that check
are built from the **local registry ∪ stored `NodeManifest`s** (manifests
carry full `Capability` entries including `input_schema`), so plans using
remote-only capabilities validate for real — `validate_plan` is not
weakened. A peer-advertised schema that fails to compile is dropped with
a warning by `CapabilitySchemaIndex::from_pairs`, which surfaces as
`UnknownSchema` → the plan is rejected at entry (strict by design).
Recursion guard: step capabilities `plan.execute` and `brain.plan` are
rejected — no nested plans in 4.3. `mesh.*` steps are allowed (their
local scans run in-process per ADR-0022; no extra executor permit).

Two accepted looseness notes (diff review MINOR-5/6): the manifest
union has **no liveness filter** — a capability advertised only by a
departed peer validates, and its step then fails at the dispatch
eligibility window (carried risk 11; a liveness filter is the 4.4
follow-up), with first-seen-wins on schema conflicts across manifests;
and step rows **drop the parent task's tags** (matching `submit_remote`
precedent) — an interactive plan's LLM steps lose the batcher-bypass
tag until tag propagation is designed (fail-closed for policy tags).

**`Plan.sig` is NOT verified in 4.3.** `brain.plan` emits the unsigned
inner plan; trust rides the signed `plan.execute` **Task** envelope that
carries the plan as input, exactly as every other capability input is
trusted. Plan-level signatures become meaningful when plans travel
without a task envelope (brain handover, 5.12).

## Concurrency and permits

`plan.execute` holds one executor permit while its self-routed steps need
permits; the daemon floor of 2 permits guarantees progress (steps
serialize through the remaining permits — slow, never stuck). To prevent
two coordinators wedging a small node, `plan.execute` `try_acquire`s a
per-node one-plan permit and **fails immediately** ("another plan is
already executing on this node") when contended — it never queues while
holding an executor permit.

## Failure policy — single-layered

The controller always runs `ReturnPartial`; the **driver** owns policy,
keyed by the `plan.execute` **input field `on_failure`**
(`"fail_fast"` default | `"continue"`). This deviates from the plan
review's original "key off the envelope's `execution.on_partial`":
capabilities never see the task envelope (`ExecutionContext`
deliberately carries no execution policy — promoting it is 37-site
churn owned by 4.5), so the knob rides the input, mirroring
`timeout_ms`. The CLI maps `--keep-going` → `"continue"`.

- `fail_fast` (default): the first `Failed`/`TimedOut` step — including
  a **feed-time** failure (`$task_output` resolution or resolved-input
  schema recheck) — stops feeding ready steps, drops the stream
  (cancelling in-flight runner futures), and `skip_remaining()`s; the
  plan task terminal-izes `Failed`.
- `continue` (and any future `Wait`, aliased per ADR-0023): independent
  branches continue; only transitive dependents of a failed step are
  skipped.

`DagSummary` is authoritative; the controller's `FanoutSummary` is
discarded. In-flight rows dropped on abort keep executing to their own
timeouts and may terminate `Done` after the plan reported them `Skipped`
(ADR-0022 orphan rule; `max_attempts = 1` on step rows; real cancellation
is the 5.x cancel API).

## Identity and output

Step rows mint fresh `TaskId`s. The aggregate output's `steps` map is
keyed by **plan-node id**, each entry carrying `task_id` (the row id),
`capability`, `state`, and `output`/`error` — 4.8's DAG view maps rows to
nodes through it, and progress frames carry both ids. Terminal rule: all
steps `Done`, or `ReturnPartial` with ≥1 success → plan task `Done` with
the aggregate; `FailFast` abort, deadline, zero successes, or entry
validation failure → plan task `Failed` with a compact summary error.
Per-step 4.2 progress frames (`{"step": {...}}`) plus one final plan
summary frame feed the WS/CLI live view.

## Deferred (owners)

Resource-aware placement/fit_score → 4.4 · federated provenance + real
`Wait` → 4.5 · lease extension/retry backoff (steps ship
`max_attempts = 1`) → 4.6 · backpressure gating → 4.7 · UI DAG viz
(frames + `plan_id` rows are its data source) → 4.8 · budget enforcement
→ 5.8 · checkpoint/resume (`CheckpointConfig` accepted, ignored) →
5.11 · step cancellation → 5.x · speculative redundancy → 6.2 · nested
plans → revisit with 5.3 · retained-output size cap/spill → Phase 6
hardening.
