# ADR-0009 — Local executor loop walks the full PRD §14.1 lifecycle ladder

**Status:** Accepted (2026-05-03)
**Context:** Phase 3.3a of HARNESS_PRD_v2.md / ROADMAP.md.
**Supersedes:** —
**Superseded by:** —

## Decision

The local executor loop in `harness-daemon` walks every state transition the PRD §14.1 lifecycle prescribes:

```
Submitted → Dispatched → Claimed → Running → Done|Failed
```

Each hop is a `Store::try_transition_task` CAS. The single-daemon ("local-only") form does all four hops itself. Cross-node fanout (3.3-fanout) does not change the ladder — the cross-node dispatcher hops `Submitted → Dispatched` on a peer, the worker on another node hops `Dispatched → Claimed → Running → Done|Failed`. Same code, same invariants, just the actor varies.

## Why not relax `can_transition_to` to allow `Submitted → Running` directly?

Tempting for the single-daemon case (one CAS instead of three). Rejected because:

- The lifecycle is a load-bearing PRD invariant. Weakening it for single-daemon convenience silently breaks the chokepoint when fanout lands.
- The dispatcher hop (`Submitted → Dispatched`) is the "claim ownership" boundary. Without it, two nodes could race and both run the same task in cross-node mode.
- The cost is three SQLite UPDATEs at ~100µs each — invisible at any reasonable throughput.

## Why CAS-per-hop instead of one transaction with four UPDATEs?

For 3.3a's ~100 tasks/s ceiling, four serial UPDATEs per task is fine (~32 statements per 100ms tick on the worst case, against SQLite WAL doing 10k-50k writes/s). When Phase 4 raises throughput targets, collapse the ladder into `BEGIN; UPDATE × 4; COMMIT;` — the CAS semantics still hold because each UPDATE filters on `state=?`, and a single fsync amortizes the cost. Not blocking 3.3a.

## Durability vs visibility

Two distinct boundaries:

- **`Store::try_transition_task`** is the **durability boundary** — single-source-of-truth for state. SQLite per-DB writes serialize. A successful CAS means the row is on disk in the new state.
- **`Store::replica_apply_local`** is the **visibility / gossip boundary** — what cross-node peers see in the LWW replica map.

The order matters: durably commit the state transition first, then apply to the replica. If the replica write fails, gossip is briefly stale, but the canonical state is correct. The reverse order would risk peers seeing a "Done" that was never durably persisted.

## `output_preview` semantics

The LWW replica's `output_preview: Option<Vec<u8>>` is the **first 256 bytes of `serde_json::to_vec(&output)`**. It is _not_ parseable JSON — it's truncated mid-string for any non-trivial output.

UI / dashboards must hit `GET /api/v1/tasks/<id>` for the full output. The replica preview is a gossip-friendly summary, not a primary data path. This is documented in `done_replica` in `executor.rs` and pinned by test `t12_replica_preview_set_on_done`.

For Failed, `output_preview` is the first 256 bytes of the error message (UTF-8 bytes). Same prefix-256 discipline.

## LWW byte ordering

`ReplicatedState` enum byte values: `Failed = 7`, `Done = 8`. Per `supersedes(self, incoming) → rhs > lhs`, a Done arriving after a local Failed wins. This matters when a peer races a different result for the same task. The gossip layer must not reissue terminals — once a node writes a terminal, it's done. The current code does not retry terminal writes; documented here so a future caller doesn't reintroduce the foot-gun.

## Issuer-name plumbing for 3.3-fanout

`ExecutionContext::issued_by_name` is an `Arc<str>`. In 3.3a single-daemon, issuer == self, so we set it to `local_node_name`. When 3.3-fanout adds remote issuers, this needs to plumb from the manifest map.

The executor logs a `tracing::warn!` if `task.issued_by != local_node` (instead of a `debug_assert!` so multi-issuer test harnesses don't trip). The warning is the loud signal that 3.3-fanout must do the plumbing before remote-issuer tasks can run.

## Concurrency floor

`max_concurrent = available_parallelism().clamp(2, 8)`:

- ≥ 2: a 1-core CI runner can still make progress on a slow capability.
- ≤ 8: a 16-core workstation isn't artificially throttled, but isn't running 16 shells simultaneously either (we have no resource accounting yet — Phase 6).

## Recovery on daemon restart

If the daemon crashes while a task is at Running, the row stays at Running forever (no `try_transition_task(_, Running, _)` happens). Phase 6 hardening adds startup recovery: on boot, transition any in-flight Running tasks to Failed with reason "daemon restarted".

For 3.3a, document in STATE.md as carryover. Not blocking the demo gate.

## Test surface

`crates/harness-daemon/src/executor.rs` `mod tests` covers:

- t07: happy path (`echo` → Done with correct output)
- t08: unknown capability → Failed with descriptive error
- t09: panic boundary → Failed with "panicked" in error
- t10: terminal idempotence (capability runs exactly once)
- t11: shutdown drains the loop in <500ms
- t12: replica preview is bytes-prefix-256 (not parseable JSON)

`crates/harness-store/tests/transition_cas.rs` covers:

- t05: legal-hop CAS race semantics
- t06: illegal-hop CAS returns `InvalidTransition` (not Ok(false))
- t06b: full ladder walks Submitted → Dispatched → Claimed → Running
- t06c: CAS on missing task returns Ok(false) (not Err)

`crates/harness-store/tests/results.rs` covers:

- t01-t04: write/load/replace/missing-row CRUD for `task_results`

## References

- HARNESS_PRD_v2.md §13.6 (channels), §14.1 (lifecycle), §14.2 (cardinality routing).
- ROADMAP.md item 3.3 + sub-items 3.3a / 3.3-fanout / 3.3-ui.
- ADR-0008 (the 3.2 / 3.2a / 3.2-stream split — same pattern as 3.3).
- `.planning/phase-3.3-cli-run.plan.md` (rust-expert round-2 review).
