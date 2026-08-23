# ADR-0028 — Lease extension, retry backoff, circuit breaker, orphan sweep (4.6)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 4.6, PRD §14.5 ("Lease expiry → re-dispatch. Retry
policy. Idempotency via task_id. Circuit breaker (5 consecutive fails →
60s bench)"), §13.6 (`harness.task.lease` channel). Closes ADR-0027's
carries: the FrameSink promotion (ADR-0024 debt (e)), the
leaseless-`Running` orphan, send-failure backoff (3.3 carried risk 9),
and the RR-cursor churn.

## What ships

1. **FrameSink promotion.** `ExecutionContext.frame_sink:
   Option<FrameSink>` (manual `Debug`); the executor stamps the
   daemon's partial-stream sink into every context;
   `MeshExec::progress_sink` and `PlanExec::progress_sink` are deleted.
   Capabilities emit `Progress` frames with zero bespoke plumbing.
   `shell.exec`'s constructor sink stays (Phase 6, per ADR-0027).

2. **Retry + send-failure backoff.** The `RetryPolicy` backoff fields
   (on the wire since 2.1, never read) now drive re-dispatch pacing:
   `initial × multiplier^(attempt-1)`, capped at `backoff_max_ms`,
   overflow-safe, hostile fields floored. Fed on non-terminal lease
   expiry and BOTH send-failure paths (the sync enqueue error and the
   async evict-and-retry failure surfacing via `on_assign_send_failed`
   — the actual half-dead-peer case). Backing-off tasks batch in the
   existing WAITING class from insert time; `constraints.deadline` is
   enforced in the skip path (the only reachable check while a task is
   never scored) via the `Submitted → Failed` supervisor hop. State is
   in-memory BY DESIGN: a restart forgets backoffs, costing one
   immediate retry burst bounded by `DISPATCH_BATCH` — never a tight
   loop (it re-arms on the next failure). `expire_pass` prunes entries
   whose task left `Submitted` sideways. `max_attempts: 1` sub-tasks
   never enter the map (their expiry is terminal — ADR-0022 unchanged).

3. **Lease extension** (`LeaseExtend` on the new `harness.task.lease`
   channel — **additive**: an old node resets the unknown-name stream,
   the connection survives, and both mixed-version directions degrade
   to exactly the pre-4.6 timeout bound).
   - *Worker*: the executor spawns a per-task extender for
     REMOTE-issued rows only (10 s cadence; immediate first tick
     consumed, so short tasks never send one), aborted the instant the
     capability future settles. Each tick resolves the LIVE lease via
     the runtime's reply-obligation map — a re-delivered assign's
     fresh `lease_id` is picked up automatically. Sends are
     fire-and-forget (the `TASK_PARTIAL` doctrine).
   - *Issuer*: `on_lease_extend` CAS-sets `expires_at = now + 30 s`
     **unconditionally** — the first extension SHRINKS a long lease to
     the rolling horizon, so a dead worker on a 10-minute task is
     detected in ~30 s instead of ~10 minutes — **hard-capped at the
     lease's original budget** (`issued_at + max(lease_ms, timeout +
     slack)`): a wedged or malicious extender can never hold a lease
     past the task's own declared budget, preserving the fleet
     liveness bound and ADR-0022's posthumous rule verbatim. Store
     guards: `worker_id` + `state IN ('pending','claimed')` (the R3
     lost-claim doctrine); terminal leases no-op, killing
     cross-attempt replays (attempt N's lease is `expired` before
     attempt N+1 exists).
   - *Expiry races*: `try_expire_lease` and the expiry-pass reset
     (`expire_and_reset_task_if_unextended`) additionally require
     `expires_at < now`, so an extension landing between
     `find_expired`'s snapshot and the CAS makes the expiry lose — a
     provably-alive worker's task is never yanked back for duplicate
     execution. The unguarded reset remains for deliberate
     send-failure revocation (the worker never received the task).
   - Leases still MINT at the uncapped TTL: old workers that never
     extend keep today's behavior exactly (zero mixed-version
     regression); a worker that crashes before its first extension
     degrades to the timeout+slack bound — accepted.

4. **Circuit breaker** (PRD §14.5). `Breaker` beside `SuccessTracker`:
   5 consecutive **node-health** failures (lease expiry, send failures
   — never task-level result statuses, which may be the caller's bad
   input) bench a node for 60 s; ANY accepted result — even a Failed
   one — proves liveness and clears the streak; a served bench resets
   the streak. Self is never benched (a single-node install must not
   gate its own queue). Consumption layers a bench-aware filter over
   liveness∩secrets for `Anyone`/`Owner` only — pinned tasks (operator
   intent) and Federated fan-outs (availability-first; a benched node
   just shows up non-Ok in provenance) bypass structurally. When the
   bench filtered anyone and eligibility then fails
   (`NoEligibleNodes`/`Owner`, the sole-benched-owner case included),
   the task joins the ResourceGated WAITING arm — an ≤60 s transient
   waits, it never burns the terminal eligibility window.

5. **Boot orphan sweep.** Once at daemon build, before any loop
   spawns, every `claimed|running(assigned=self)` row is provably
   crash debris: locally-issued rows → `Failed` "orphaned by daemon
   restart" (no lease, no coordinator left — crashed federated parents
   included, closing ADR-0027's headline carry); REMOTE-issued rows →
   reset to `Dispatched(self)` for re-execution (at-least-once, the
   re-dispatch doctrine) so the issuer's recovery (lease expiry →
   re-assign → terminal-resend) ships the REAL result. A synthetic
   stored Failed would poison the issuer's retry budget via the
   terminal-resend arm — rejected in plan review.

6. **Idempotency by task_id** was already structural (INSERT OR IGNORE
   ingest, `dispatched → claimed` CAS, lease-CAS result acceptance,
   assign-time terminal-resend) and is now additionally exercised by
   the 4.6 tests (m08 exactly-once across a shrunken lease; the m02
   invariant unchanged).

## Bounds and constants

`EXTEND_HORIZON_MS = 30_000` (test 2 000); `EXTEND_INTERVAL_MS =
10_000` (test 300) — horizon = 3× interval: ONE lost extension is
survivable with margin; a second consecutive loss is boundary-exact
(expiry and the next tick coincide), so treat two losses as plausible
expiry; `BENCH_THRESHOLD =
5`, `BENCH_MS = 60_000` (PRD literals); backoff default 250 ms × 2^n
capped 30 s (RetryPolicy defaults).

## Deferred (recorded, not dropped)

- **Federated-parent leases / coordination-lease extension**
  (ADR-0027 named them for 4.6): parents stay leaseless — a
  mid-coordination hang is bounded by the driver's own deadline, and
  crash debris is now swept at boot. Rewiring coordination under
  leases belongs with the Phase 5 wrapper work; re-recorded there.
- Retry-aware `Wait`: still requires wrapper-side re-submission, not
  lease machinery — Phase 5 wrapper rewrite.
- Federated fan-out scoring (ADR-0026 trail): re-deferred to 4.7.
- `shell.exec` constructor-sink → `ctx.frame_sink`: Phase 6 cleanup.

## Risks accepted

- In-memory backoff/bench: restart = one immediate retry burst
  (bounded, re-arms). Durable backoff would cost a schema migration
  for marginal benefit.
- The extender ticks even if the capability future is wedged — by
  design (extension exists for long tasks); the issuer-side budget cap
  is the enforcement, not worker cooperation.
- Bench applies per-node, not per-capability: a node failing one
  capability's sends is likely unhealthy for all (transport-level
  signals only feed it).
