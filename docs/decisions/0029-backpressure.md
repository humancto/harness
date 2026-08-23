# ADR-0029 — Backpressure: paused producer, admission, pause-aware routing, bounded everything (4.7)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 4.7, PRD §14.10 ("Bounded channels everywhere.
Backpressure is tested, not assumed"), §25.1 (paused nodes), §17
(N×X scaling). The signed heartbeat has carried a `paused` field since
1.2 with **no producer**; the SQLite task queue accepted unbounded
submissions; the pin/Owner/Federated routing arms were pause-blind; and
several shipped bounds had no failure-condition tests.

## 1. The `paused` producer

`PauseState` (daemon-owned, `Arc`-shared) = operator flag OR an
auto-latch with hysteresis:

- **Auto**: latch at `work_depth ≥ max_queue_depth`, release only at
  `work_depth ≤ resume` (= `max·3/4`, derived) — no flapping across the
  2 s heartbeat cadence. State is in-memory; a restart re-derives it
  from the next snapshot (self-healing).
- **Operator**: `POST /api/v1/admin/pause|resume` (bearer-auth, 503
  when unwired), surfaced as `paused` on `GET /status`. The API sees
  the daemon through the narrow `PauseControl` trait.
- **Work depth, not raw inflight**: active coordinations (federated
  slots ≤8 + executor coordination permits ≤16) are subtracted via RAII
  guards — worst case 24 phantom rows against the default max of 64
  would otherwise auto-pause a coordinator-heavy brain on bookkeeping.
  Heartbeat `queue_depth` therefore means WORK depth as of 4.7.
- **Knob**: `DaemonRuntimeConfig.max_queue_depth` (default 64) — the
  operator-facing bound PRD §14.10 implies; m09 sets 4 on one node.

One instance feeds the heartbeat `snapshot_fn`, the API, and the
dispatch runtime's `StoreLoadView` — which consults it for SELF (no
`PeerTable` self entry), so a paused node stops dispatching to itself
too. Already-`Dispatched` rows still drain through the executor:
pausing gates *dispatch*, never execution — coherent single-node
semantics, no deadlock, and never a weakening of election/policy.

## 2. Submit admission (the API edge)

`POST /tasks` refuses with `429 Too Many Requests` + `Retry-After: 2`
while ≥ `MAX_SUBMITTED_BACKLOG` (1024) rows sit at `Submitted`
(`Store::count_tasks_by_state`, indexed COUNT). Decisions recorded:

- **Check-then-insert is non-transactional**: concurrent submits can
  overshoot by the number of in-flight requests. The cap protects
  against unbounded growth; an exact ceiling would serialize every
  submit through a write transaction for no operational gain.
- **Count errors fail open**: admission is protective; a broken counter
  must not take submission down (the insert surfaces a genuinely broken
  store as a 500).
- **Internal sub-task mints bypass the gate** (wrappers, coordinator,
  plan steps): they are already O(window)-bounded by the 4.1
  controllers, and gating them would deadlock wrappers mid-await.
- 1024 is deliberately far above any drain window; a healthy fleet
  behind a busy issuer is the accepted trade (risk noted in plan).

## 3. Pause-aware routing (`eligible_scored`, all arms)

The gate lives INSIDE `eligible_scored`, which owns the `LoadView`
(`NodeSnapshot.paused` from heartbeats; self from `PauseState`) — the
non-Anyone arms are no longer delegated to the LoadView-less
`eligible()`:

- **Anyone** (pins included): already gated since 4.4 — `fit_score`
  returns 0 for paused nodes → `ResourceGated`.
- **Pin (Federated/Owner arms)**: a live-but-paused pin ⇒
  `ResourceGated` (waits in the gated class). A DEAD pin stays
  `PinnedNodeNotLive` — dead ≠ paused; the pause gate consults only
  live nodes, so m08's fast-terminal path is untouched.
- **Owner**: paused owners drop from the intersected owner set; all
  paused ⇒ `ResourceGated`. Owners WAIT, never silently reroute outside
  the owner set.
- **Federated (unpinned)**: paused candidates are excluded into
  `DispatchPlan::Federated::excluded` and appear in provenance as
  `Skipped` (item_count 0) — exclusion is never silent (MAJOR-5). They
  are NOT counted by the failure policy: an excluded-at-start node
  simply isn't a target. All paused ⇒ `ResourceGated`. This closes the
  ADR-0026/0028 "federated scoring" debt deliberately: federated stays
  score-BLIND (§14.6 fans to all eligible) but is no longer
  pressure-DEAF.
- **Asymmetry recorded as deliberate**: Federated capabilities
  exclude+mark (availability-first inventory); the Anyone-cardinality
  `mesh.*` scope wrappers WAIT, bounded by the new sub-task deadlines
  (data-critical — a missing scope is data loss, not a partial view).
- Unpaused routing is byte-identical (regression-locked, s11).

**Sub-task deadlines (the posthumous-wait bound).**
`build_pinned_subtask` and `PlanExec::submit_step` stamp
`constraints.deadline = issued_at + timeout_ms`: the pre-dispatch
waiting phase 4.7 introduces is deadline-bounded. A sub-task parked
behind a paused worker terminalizes via the existing deadline checks
when its await budget passes and can never execute posthumously —
ADR-0022's rule extended to the pre-lease phase.

This also makes the honest reading of ADR-0023's "the fan-out
controller respects worker queue depth" true: the gate lives at
dispatch, where per-target pressure is actually known, not inside the
pure controller. The 4.1 window refill needs no separate gate — a
gated sub-task's runner simply doesn't settle, so the bounded window
itself stops pulling (refill-gate equivalence).

## 4. Existing bounds, finally tested

No behavior changes; the failure conditions themselves are pinned:
peer_net `QueueFull` (both the sync `send_to` surface and the async
`on_assign_send_failed` arm — no assign is silently dropped); the
reply pump, the TableEvent bridge, and the WS `/events` subscriber all
survive `Lagged` per their documented recovery stories (skip +
assign-time terminal-resend; log-and-continue; 1011 close for resync);
`into_channel`'s bounded channel provably PARKS its producer. Bare
literals named: `MESH_EVENT_CAPACITY = 1024`,
`TERMINAL_EVENT_CAPACITY = 256`.

## 5. Bounds hygiene

- `llm_batcher`: `MAX_SLOT_SENDERS = 64` — the overflowing caller
  atomically removes the slot and flushes early (the remove is the
  double-flush guard; the timer finds the slot gone);
  `MAX_LIVE_FINGERPRINTS = 256` — new fingerprints bypass batching at
  cap (documented degradation, never an error).
- `partial_stream.pending`: the map itself is bounded
  (`MAX_TRACKED_TASKS = 256`, insertion-order eviction of the oldest
  task's queue, warned with the lost count). Hygiene, not a hazard —
  live streaming tasks are bounded by work permits.
- Sweep extension (risk #13): `elig_failures` entries whose task left
  `Submitted`, and reply obligations for **Cancelled** tasks, are
  pruned. Correction to the recovery story: replica gossip (ADR-0019)
  — not lease-expiry resend — is what informs an issuer of a remote
  cancellation outcome; the terminal pump never fires for Cancelled,
  so the sweep is the obligation's only exit.
- API lossiness flag: `GET /tasks/:id` gains additive
  `partials_dropped` = local ring evictions + the worker's
  wire-reported queue drops (accumulated by `on_partial` from the
  batch `dropped` field). The UI can now say "output shown is
  incomplete" instead of silently truncating.
- `harness run` fan-out windowed at `CLI_FANOUT_WINDOW = 16`.

## Proof (m09 + operator-leg composition)

m09: worker B (`max_queue_depth 4`) saturated by pinned sleeps →
advertises `paused` at A → further pinned work WAITS (no lease),
Anyone work routes to A (leaseless self-execution proves it), federated
`mesh.info` completes with B `Skipped`/A `Ok` in provenance → B drains
below resume → un-pauses → the waiting task dispatches and completes
exactly once. The operator path composes from unit-tested parts:
endpoints (admin_pause.rs) → `PauseControl.set_operator` →
`effective()` (unit) → the same snapshot/dispatch plumbing m09 proves
end-to-end for the auto half; t29 pins operator-pause gating
dispatch-to-self against a real runtime.

## Rejected

- `PauseAwareLiveSet` wrapper (plan draft): liveness and load are
  different axes; a paused node is LIVE (heartbeating, finishing work).
  Folding pause into the live set would have corrupted the dead-pin
  fast-terminal path and the bench remap. The LoadView is the pressure
  axis; the gate belongs there (review BLOCKER-1).
- Gating internal sub-task inserts through admission (deadlocks
  wrappers; already window-bounded).
- Counting `Submitted` backlog into auto-pause: pausing on the ISSUER
  dimension would deadlock a busy issuer's own submissions; auto-pause
  reads assigned inflight only.

**Carried:** operator pause persistence across restarts (in-memory by
design; PRD §25.2 sleep/wake will revisit); federated-parent lease
extension stays Phase 5 (ADR-0028 carry); `shell.exec` ctor sink Phase
6 (ADR-0027/0028 carry).
