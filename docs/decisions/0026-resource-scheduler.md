# ADR-0026 — Resource-aware scheduler: fit_score adaptation (4.4)

**Status:** Accepted (2026-08-22)
**Context:** Roadmap 4.4, PRD §14.3 (fit_score) / §14.9 (multidimensional
load) / §14.10 (backpressure). Also closes carried risks 10
(dispatch batch head-of-line) and 12 (`SubmitRequest.execution`
unclamped), and the 4.3 carry (manifest-union liveness).

## The adapted formula — no wire changes

The v2 `Heartbeat` already carries every load field §14.3 needs
(queue_depth, cpu_busy_pct, cpu_pinned_count, ram/gpu used+total,
in_flight, on_battery, paused), but the daemon publishes zeros for the
sampled fields (cpu/ram/gpu sampling is a Phase 6 hardening item — no
`sysinfo` dependency added). The scheduler therefore composes what is
real today:

- **Issuer-side in-flight counts** from the store (`assigned_node` rows
  in Dispatched|Claimed|Running, one GROUP BY per 100 ms poll) plus
  same-poll reservations so one batch spreads across nodes;
- **heartbeat load when nonzero** — this PR also makes `queue_depth`
  truthful (tasks assigned-to-self, non-terminal), giving *other*
  issuers a real cross-issuer signal; the two in-flight signals compose
  by `max()` (never sum — no double-count now, and none when 1.5
  populates `in_flight` or Phase 6 lands sampling);
- **manifest capacity** (cpu_cores, ram_total_mb, GPU/VRAM);
- **a per-node success EWMA** (`SuccessTracker`, α = 0.2, optimistic
  prior 1.0, floor 0.05), in-memory on the issuer, fed at: remote
  results (after the lease CAS accepts them — duplicates never
  double-count), BOTH lease-expiry branches (`worker_id` held a lease
  and didn't finish), assign-send failures, and **local executor
  terminals** (shared `Arc` — otherwise the self node's rate would pin
  at the prior while remote failures accrue).

```
fit_score = hard_gates × soft(cpu) × soft(mem) × soft(gpu)
          × success_rate / cost_weight        (battery ⇒ 2.0)
```

**Hard gates (score 0) are genuine impossibilities only**: paused
(§14.10), gpu_required without a GPU, requested memory/VRAM over
demonstrated free capacity, pinned-CPU-full (§14.9). **Pressure is
soft**: each `1 − pressure_after` factor is floored at 0.05, so a
saturated node ranks last but stays schedulable — saturation is
transient queueing, never task failure. Unknown capacity (zeros) is
neutral at pressure and permissive at gates. `network_class`/
`disk_io_class` contribute nothing yet (no telemetry exists); §14.9's
"CPU-full node still takes network-bound work" holds because a Light
task adds only 0.1 core of demand and isn't pinned-gated.

## Selection and the ResourceGated wait

`Dispatcher::eligible_scored` filters candidates exactly as before, then
takes argmax with a 1% relative **tie band resolved round-robin** (the
persisted cursor machinery unchanged). Consequences, both deliberate:

- **Equal-capacity fleets behave exactly as today** (uniform scores →
  the tie band is the whole candidate list → the identical RR sequence,
  regression-locked by test).
- **Heterogeneous fleets get capacity-proportional placement from day
  one** (an idle 12-core node outranks an idle 2-core node) — a
  behavior change vs blind RR, locked by its own test.

When every candidate is hard-gated the poll returns
`DispatchError::ResourceGated`, and the dispatch runtime treats it as
**queued, not undispatchable**: the eligibility-window clock never
starts, the task waits bounded only by its own `constraints.deadline`,
and gates re-evaluate against fresh snapshots every poll. `Owner` and
`Federated` routing are unchanged (ownership dominates; 4.5 owns
federated scoring).

## Carried risks closed here

- **Risk 10** — the dispatch batch is now **fresh-first**: tasks not in
  the eligibility-failure map go first (submission order preserved per
  partition), so ≤16 known-undispatchable tasks can't starve fresh
  work. Tradeoff: under a sustained full batch of fresh tasks, a
  known-failing task's terminal write defers past the window until the
  first non-full poll — harmless while it waits. (Batch rotation was
  rejected: a bad tranche would still consume whole polls cyclically.)
- **Risk 12** — `ExecutionPolicy::clamped()` at the API submit boundary
  (before signing; wire ingest stays verbatim — mutating a signed
  envelope would break verification): timeout 1 s..610 s (preserving
  the CLI's engineered +5 s slack over the 600 s capability ceiling),
  lease 1 s..900 s (≥ max timeout + the dispatcher's 15 s slack),
  redundancy normalized to 1 (accepted-but-ignored until 6.2).
  `SubmitRequest` also gains an optional `resource_hints` field —
  callers can finally declare §14.9 hints; the scheduler unions them
  with the capability manifest's declaration (max-demand).
- **4.3 carry** — `known_capabilities` applies the same liveness
  predicate as `targets()`: a departed peer's manifest no longer
  validates a plan step that would then die at dispatch.

## Known limits (accepted, with owners)

Double-count safety once Phase 6 sampling lands rests on the `max()`
composition. `assigned_inflight` is issuer-local; multi-issuer meshes
lean on the now-truthful `queue_depth`. Success rate conflates
capability failure with node failure (uniform inputs distort nothing;
node-specific bad inputs bounded by the floor + decay) — 4.6's circuit
breaker refines on this substrate. Federated/Owner scoring → 4.5.
Real sampling (`sysinfo`) → Phase 6.
