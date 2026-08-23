# ADR-0036 — Runtime Budget enforcement (5.8)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 5.8, PRD v2 §17.8. The
`Budget { max_cost_usd, soft_limit_usd, on_exceed }` wire type has
ridden every `Plan` since Phase 2; this is the first code that reads
it. Division of labor: ENFORCEMENT lives in
`harness-orchestrator::budget` (it must sit inside the exec loop);
PRICING and per-plan/user/day aggregation are 5.9's `harness-cost`.

## What ships

1. **Cost attribution convention.** A step's actual cost is the
   top-level `cost_usd` number in its result JSON, else $0 (local
   execution is free by the product thesis; cloud caps emit token
   counts today and start emitting `cost_usd` when 5.9 lands
   pricing). Enforcement acts on ACTUALS only — estimates never
   abort work; projection is deferred to 5.9 where pricing gives it
   teeth. Negative/NaN values clamp to $0 with a warn (a worker
   cannot "refund" a budget).
2. **`BudgetTracker`** (sync, lock-free, inside the single exec
   loop): per-completion `record` returning fires-exactly-once
   verdicts — `SoftCrossed` at `soft_limit_usd`, `Exceeded` at
   `max_cost_usd` with the resolved action.
3. **Effective-budget resolution.**
   - The plan's own `Budget` wins — carrying one IS §17.8's
     "explicit approval". The planner CANNOT self-approve: the LLM
     response schema has no budget field (`deny_unknown_fields`
     makes one a parse error) and both backends hardcode
     `budget: None` — pinned by test.
   - No plan Budget → policy `[execution].default_plan_budget_usd`
     (serde default **$5**, per §17.8) with `on_exceed: Cancel`.
   - Policy `[execution].plan_budget_ceiling_usd` (default None)
     hard-caps EVERYTHING when set — including a plan-carried
     `max_cost_usd: null` waiver (cap = min(cap, ceiling); a waiver
     under a ceiling gets cap = ceiling with the plan's own action).
   - **Trust model, stated plainly:** without a ceiling, any
     authenticated `plan.execute` submitter can waive the default by
     attaching `Budget { max_cost_usd: null }` — the same trust
     surface as being allowed to submit a plan at all. Operators who
     need a hard wall set the ceiling. (A serde-defaulted `Option`
     cannot be set back to `None` from TOML, so the $5 default is
     disabled only by those two mechanisms.)
4. **Actions** (wildcard arm for the `#[non_exhaustive]` enum
   defaults to Cancel with a warn):
   - **Notify** — warn + a budget Progress frame; execution
     continues.
   - **Cancel** — stop dispatching immediately; remaining steps
     settle Skipped. In-flight runner futures are dropped exactly
     like fail-fast (their rows orphan-complete under their own
     timeouts, ADR-0022).
   - **Pause** — the driver raises a pause flag consulted by the
     fan-out SOURCE (so the window cannot refill from `ReadyStep`s
     already buffered in the channel — Codex P1 on #59) and drops
     the sender to wake the stream: genuinely in-flight steps FINISH
     (their costs record), the stream ends with `SourceDrained`, and
     both buffered and never-dispatched steps settle Skipped. Without 5.11/5.12 there is nothing to
     resume FROM — "pause" is stop-scheduling with a resumable
     record; 5.12 upgrades it. The budget object lists the
     `unscheduled` step ids so resume can tell budget-parked from
     failure-cascade skips.
5. **Surfaces.** The task-envelope `TaskState` set is UNTOUCHED
   (consumers hard-match it). Plan-level outcome rides the aggregate
   output: new `status` field (`done` | `paused_budget` |
   `aborted_budget`) + a `budget` object (`spent_usd`, `cap_usd`,
   `soft_limit_usd`, `action`, `triggered`, `unscheduled?`) whenever
   a budget was in effect. Budget stops return **Ok** — a policy
   verdict over meaningful partial results must not discard the
   aggregate into an error string; existing fail-fast keeps its Err.
   The `status` discriminator is whether the stop actually PARKED
   work (`unscheduled` non-empty) — a stop that fires after the last
   step, or after only continue-mode failures, leaves
   `status: "done"` (`triggered: true` records the event; Codex P2
   on #59). Budget events also ride
   the Progress frame stream (`{"budget": {"event": ...}}`) for
   5.10's Costs UI; today's DAG view ignores unknown keys. The
   webhook reply and CLI exec renderer read `status` (a paused plan
   reports "⏸ paused at budget", never "✅ done").
6. **Validation strengthened** (never weakened): new
   `BudgetInconsistent` rule — a plan whose own `budget.max_cost_usd`
   is below its `estimated_cost_usd` is rejected. Future-proofing:
   planner backends emit no budget today.

7. **Limit hygiene** (Codex P2 on #59): policy load rejects
   non-finite or negative `[execution]` dollar knobs (TOML parses
   `nan`, which would silently disable every `spent > cap`
   comparison); a plan-carried nonsense limit sanitizes to the
   STRICTEST reading ($0), never "unlimited".

8. **Precedence:** deadline expiry and fail-fast aborts WIN over a
   budget stop (the settled plan decision) — a pause racing either
   keeps the Err semantics; only a clean budget stop returns Ok.

## Recorded limitations

- **Overshoot bound:** budget checks are per-completion, so
  concurrent in-flight steps can overshoot by up to the dispatch
  window × the max single-step cost — which is unbounded until 5.9
  prices steps. Inherent to streaming dispatch.
- **Failed steps contribute $0** (outcomes carry only an error
  string): a cost-then-fail cloud call undercounts, and a retry loop
  of expensive failures never trips the cap. Frozen by test; 5.9
  fixes it by costing the result row rather than worker JSON.
- **`unscheduled` conflates two things on Cancel:** budget-parked
  never-ran steps and dispatched-then-dropped in-flight steps (whose
  rows orphan-complete, ADR-0022). 5.12 resume will need to split
  them via the row-id presence in the step records.
- **Trust-the-worker:** `cost_usd` comes from the step's own output
  — same trust model as every result. Notably `mcp.proxy` passes
  foreign tool results through top-level, so an external MCP server
  can INFLATE cost (spurious stop) but never deflate below $0
  (clamp). 5.9's result-row costing removes the vector.
