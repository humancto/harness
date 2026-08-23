# ADR-0040: Plan resume — and why "brain handover" is the wrong frame

- **Status:** accepted
- **Date:** 2026-08-23
- **Roadmap:** 5.12
- **PRD:** v2 §23 Phase 5 demo — *"Crash brain mid-plan; new brain
  resumes from checkpoint."*

## Context

5.11 (ADR-0039) built the checkpoint store and the settle-from-
checkpoint path: a re-submitted plan replays every step whose
checkpoint still stands. Two things were missing, and one of them was
worse than missing.

## Decision 1 — checkpointing is on by default, or 5.11 is inert

Nothing in the product ever set `Plan.checkpoint`. Every planner
backend emits `None`; the only `CheckpointConfig` constructed outside
`harness-core` was a test helper. So 5.11 ran for hand-authored plans
only, and every resume path would have replayed zero steps.

`PlanExecCapability` now fills a silent plan's checkpoint config with
`{enabled, Sqlite, Blake3}`, controlled by `[execution]
checkpoint_plans` (default `true`). A plan that carries its own config
still wins.

This is a semantics choice, not a default-value tidy-up: with it on, a
resumed plan **replays** a recorded output instead of re-running the
step. That is the point — but an operator who would rather re-execute
everything can set the knob to `false`.

## Decision 2 — resume mints a new row; it cannot re-dispatch the old one

`POST /api/v1/tasks/{id}/resume` creates a **new** `plan.execute` task
carrying the **same plan id** (which is what makes 5.11's checkpoints
hit), optionally with a raised cap.

Re-dispatching the original row was the obvious design and it is
impossible:

- a `plan.execute` that ran locally takes **no lease** — `dispatch_to`
  returns early for self — so there is nothing for lease expiry to
  reset;
- the boot orphan sweep marks locally-issued `Claimed|Running(self)`
  rows **`Failed`**, and `Failed` has no outgoing transition.

So the same-node restart case — the one ADR-0039 called the common
one — leaves a terminal row that only a new row can follow.

Safety comes from an idempotence check instead: resume refuses with
`409 already_running` while any non-terminal `plan.execute` row exists
for that plan id. Never two coordinators on one plan.

## Decision 3 — the honest scope: "brain handover" is a misnomer here

The roadmap item says *brain handover*. Under this architecture that
frame does not hold:

- `plan.execute` is `Cardinality::Anyone` — the brain coordinates no
  plans and has no special role in running them;
- gossip carries **state**, not envelopes, so a newly-elected brain
  physically cannot act on another node's plan row: it does not have
  the plan.

An election-driven "resume stranded plans" sweep was designed, then
cut. It would have keyed on peer liveness (`PEER_TIMEOUT` 6 s) while
the mechanism that already works keys on lease expiry (30 s, with a
CAS that a live worker defeats by extending). A coordinator with a
routine wifi blip still holds its lease, its plan semaphore, and its
in-flight steps — resetting its row would have produced **two
concurrent coordinators for one plan**, both minting steps, both
writing checkpoints, both causing side effects.

What actually recovers, and how:

| Failure | Recovery |
|---|---|
| Coordinator died, plan dispatched remotely | Existing lease expiry re-dispatches the row (~30 s), safely |
| Daemon restarted, plan ran locally | Boot sweep fails the row; operator (or UI) calls resume |
| Budget paused / cancelled / partial | Resume, optionally with a raised cap |
| Coordinator gone permanently | Plan re-runs from scratch — its checkpoints were on that disk |

The last row is the honest limit. Checkpoints are **not** gossiped:
that would put full step outputs on the LAN where only 256-byte
previews go today — a new exposure surface, a wire change, and
unbounded volume — to cover a case that only bites when a node never
comes back.

## Decision 4 — `unscheduled` vs `in_flight`, keyed on minted rows

A stopped plan's leftover steps are now reported in the aggregate as
`resume: { unscheduled, in_flight }`:

- **`unscheduled`** — no task row was ever minted. The step never
  left the ready set; resuming it is plainly safe.
- **`in_flight`** — a row exists, so the step really was dispatched
  and its outcome was never recorded. Resuming may run it a second
  time, so the endpoint refuses until the caller passes
  `allow_in_flight`.

The split keys on `row_ids`, not scheduler state: a step is marked
`InFlight` when it leaves the ready set, *before* the fan-out window
pulls it, and the 5.8 Pause path deliberately leaves buffered steps
unpulled. Scheduler state would have flagged nearly every budget
resume as unsafe. (`budget.unscheduled` from ADR-0036 is unchanged for
compatibility; it remains the union.)

## Consequences

- **Costs read strangely across a resume.** The ledger sums actual
  dollars over every row carrying the plan id but takes the reported
  cap from the newest aggregate, so a resumed plan shows actual
  spend exceeding its reported cap. Both numbers are true; they
  answer different questions.
- **A raised cap is still clamped** by `plan_budget_ceiling_usd` when
  the plan runs. The endpoint reports what it minted, not what will
  survive the ceiling.
- **Resume within the retention window.** The 7-day age sweep deletes
  checkpoints by `created_at` regardless of plan status, so a plan
  parked longer than that loses its prefix and re-runs. The
  completeness sweep is not a hazard here: a `paused_budget`
  aggregate never counts as complete.
- **Steps the dead coordinator had in flight re-run.** Their rows and
  leases lived only in its store; nobody else can settle them, and
  their outputs were never checkpointed.
- **Deferred, not cut:** placing a re-dispatched plan on the node that
  holds its checkpoints, as a genuine *soft preference* in
  `eligible_scored` (a scorer tie-break — never `pin_to_node`, which
  is a hard filter that fails the task terminally when the target is
  unreachable, and which would aim straight at the node most likely to
  still be running the plan, where the one-plan-per-node semaphore
  turns it into an immediate failure).
