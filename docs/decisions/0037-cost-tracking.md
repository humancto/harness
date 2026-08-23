# ADR-0037 — Real-time cost tracking (5.9)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 5.9, PRD §17.8. Gives 5.8's budget enforcement
real dollars and 5.10's dashboard a data source. Division of labor:
enforcement in `harness-orchestrator` (ADR-0036); PRICING and the
LEDGER here in `harness-cost`.

## What ships

1. **Pricing** (`harness_cost::pricing`): a built-in per-model table
   ($/1M input, $/1M output) matched by **longest model-id prefix**
   across built-ins ∪ overrides — plain first-match would price
   `gpt-4o-mini` (the OpenAI cap's default model) at `gpt-4o`'s ~16x
   rate; the pair is pinned by test. `[cost.model_prices]` policy
   overrides (validated finite/non-negative/non-empty at load) are
   the operator contract; **built-ins are best-effort snapshots** and
   an unknown model is UNPRICED (`None`) — never guessed. The daemon
   installs the table once at boot (process-wide `OnceLock`).
2. **Cloud caps emit `cost_usd`**: `llm.cloud.{claude,openai,gemini}`
   price provider-reported usage into a top-level output field (null
   when unpriced/usage absent) — 5.8 enforcement now sees real
   dollars. The brain's `estimated_cost_usd` is untouched (it is the
   LLM's plan-cost estimate, not a token price — repricing it would
   be a category error).
3. **Cost lands on the RESULT ROW** (`task_results.cost_usd`, V0007;
   the dead `tasks.cost_usd/cost_tokens_in/cost_tokens_out` columns
   from V0001 are recorded here as legacy-unwritten and left in
   place). Written via `write_result_cost` at **all** the done-row
   sites that matter, behind the hint gate:
   - the executing node's executor (its own manifest in scope);
   - the ISSUER-side ingest of a remote worker's result — the row
     the coordinator's ledger actually reads (both stores get a
     row); judged against the **issuer's own local manifest**, never
     the worker-signed capability announcement;
   - the federated parent writes NULL (its sub-rows carry dollars).
   The gate: cost persists only for `CostHint::CloudPaid`
   capabilities, finite and ≥ 0. A `LocalFast` output claiming
   `cost_usd` (the `mcp.proxy` passthrough vector from ADR-0036) is
   warned about and NEVER reaches the ledger. 5.8's in-loop
   enforcement still reads raw output `cost_usd` — deliberately
   conservative (an inflated claim can only stop a plan early);
   ledger truth ≠ worker claim.
4. **Ledger** (`harness_cost::ledger`): read-side aggregation over
   the LOCAL store — no new database, no background task. Window =
   **30 days** (time-bounded on the existing `completed_at_ms`
   index) with a 5000-row cap as backstop; the response carries
   `window_days` + `truncated` so the UI never presents a truncated
   number as an all-time total. Per-plan context (name, `status`,
   the 5.8 `budget` object's spent/cap/soft/triggered) comes from
   parsing bounded `plan.execute` aggregates — there is no plans
   table. Per-"user" ≡ per issuing node (the mesh has no users).
   `estimated_usd` per plan is deliberately absent (no honest
   source; revisit in 5.10).
5. **`GET /api/v1/costs`** — session-auth read endpoint returning
   the ledger fold. Polling only; live push is a 5.10 decision.

## Recorded limitations

- **Local view:** result rows are local derived data (gossip carries
  `ReplicatedTaskState` with a 256-byte preview — no wire change).
  `/costs` is meaningful on the coordinating node and near-empty on
  a bystander; per-issuer totals collapse onto the coordinator for
  plan steps (sub-tasks are issued by it).
- **Failed calls still cost $0.** ADR-0036 promised "5.9 fixes it";
  RETRACTED in part: error paths discard response bodies, so no
  usage exists to price. The column is the future home; a retry loop
  of billed-then-failed cloud calls still never trips a budget.
- **Retry overwrite:** `ON CONFLICT DO UPDATE` on the result row
  means a billed-then-retried call counts once (known undercount).
- **Cloud PLANNING spend is untracked:** `brain.plan` is a LocalFast
  capability and the brain's `CloudBackend` does not parse `usage` —
  a cloud-escalated planning call's real dollars are invisible to
  both enforcement and the ledger. (The `estimated_cost_usd` field
  is the LLM's plan-cost self-estimate — repricing it would be a
  category error.) Fix path: parse usage in the cloud planner and
  attach it to the brain.plan result.
- **Price drift:** built-ins go stale; overrides + unpriced-unknown
  bound the damage. Issuer-side re-pricing from tokens (instead of
  accepting the CloudPaid claim) is the recorded upgrade path.
