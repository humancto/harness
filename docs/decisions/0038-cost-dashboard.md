# ADR-0038: Cost dashboard UI + task cancel (the §17.8 stop button)

- **Status:** accepted
- **Date:** 2026-08-23
- **Roadmap:** 5.10
- **PRD:** v2 §17.8 — "UI Costs page: live spend graph, projected
  total, big red stop button."

## Context

5.9 shipped the read side (`GET /api/v1/costs`: windowed totals,
per-plan rows with 5.8 budget context, per-issuer, per-day). §17.8
asks for a page over that fold plus a stop button. Nothing in the
API could stop a task, and nothing in the plan-exec loop watched its
own task row — a runaway plan could only be stopped by killing the
daemon.

## Decision 1 — cancel is a record-level stop, not an interrupt

`POST /api/v1/tasks/:id/cancel` (session auth) flips any
non-terminal task row to `Cancelled` and stops all FORWARD spend.
It does not interrupt an already-executing capability future.

Semantics, precisely:

- **Store:** new atomic `Store::cancel_task(id)` — one transaction
  reads the state, checks the transition table, flips the row.
  `Claimed → Cancelled` was added to the table (it was not legal
  before 5.10); every other non-terminal state already had it. No
  read-then-CAS race: outcome is `Cancelled`, `Unknown` (404), or
  `AlreadyTerminal(state)` (409, body names the state).
- **Leases:** cancel releases the task's live (`pending`/`claimed`)
  leases. Consequences, each deliberate: a late worker result now
  drops at the existing terminal-lease guard instead of writing
  Done-over-Cancelled and gossiping `Done`; lease expiry cannot
  later write `Expired` over `Cancelled`; no breaker/EWMA penalty
  lands on a worker because an operator cancelled.
- **Replica:** the coordinator mirrors `ReplicatedState::Cancelled`
  to the gossip layer like every other local transition. No wire
  change — `Cancelled` was already in `ReplicatedTaskState`.
- **Executor:** terminal writes now gate on winning the
  `Running → Done/Failed` CAS. A lost CAS (row cancelled mid-flight)
  skips the result write and the replica broadcast, logs at info,
  and still signals `terminal_tx` so local waiters wake. Honesty
  note: the executor has **no local timeout of its own** — "running
  steps finish on their own timeouts" means capability-internal
  deadlines. A capability without one runs to completion after a
  cancel; it just writes nothing when it gets there.
- **Plan loop:** `PlanExec::own_cancelled(task_id)` (default false;
  the store-backed impl does one indexed read) is checked once per
  step completion in the fan-out loop. Cancelled → stop exactly like
  the budget-Cancel path: break, stranded steps `Skipped`, aggregate
  `status: "cancelled"`. A cancelled runaway plan stops minting new
  steps at the next completion boundary — bounded by the in-flight
  window, not by luck.

### Scope

The endpoint cancels any top-level row; the UI surfaces only
`plan.execute` rows (the spend-bearing case §17.8 names). Cancelling
a STEP row of a live plan is legal and acts as a fail-fast lever:
`await_terminal` maps `Cancelled` to a failed step, so the plan's
normal failure policy takes over. On a worker, cancelling an
ingested row drops the reply obligation — the coordinator's lease
expires normally. Both are documented behaviors, not UI affordances.

## Decision 2 — `plan_id` stamped at mint

`mint_task` parses `input.plan.id` for `plan.execute` submissions
and stamps `task.plan_id` (previously always `None` for top-level
rows). All three submit paths flow through `mint_task`, so the
Costs page's active-plans join (tasks listing × ledger `per_plan`)
is real rather than aspirational. The ledger itself is unaffected:
the `plan.execute` row's own `cost_usd` stays NULL.

## Decision 3 — projected total is an elapsed-days run rate

"Projected 30-day run rate" = window total ÷ **elapsed** days × 30,
where elapsed = days since max(window start, earliest spend day),
clamped ≥ 1, and the figure is withheld (`insufficient data`) until
≥ 3 elapsed days. Spend-days was rejected as the denominator: a
month with 3 spend days would project 10× reality. Today counts as
a full elapsed day, so the projection reads slightly low intra-day
rather than high. Nothing fancier is claimed — 5.9 deliberately cut
per-plan estimates, and a run rate is the only honest aggregate the
ledger supports.

## Decision 4 — polling, not WebSocket

The page polls `/costs` + `/tasks` every 5 s, gated on
`document.visibilityState`, with a generation guard and per-cycle
`AbortController` (the 5.4 poll-hygiene lesson). The 5.9 ADR left
live push open; a read-only fold at a bounded interval does not
justify a WS channel. Revisit only if someone needs sub-second
spend.

## Consequences

- The stop button stops *spending forward*; it is not time travel.
  The confirm dialog says exactly that.
- Cancelled plans report `status: "cancelled"` with the stranded
  steps listed as skipped — same aggregate contract as budget stops
  (ADR-0036).
- True cooperative in-flight cancellation is deferred to the
  checkpointing work (5.11/5.12), where a natural yield point
  exists.
