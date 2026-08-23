# ADR-0039: Checkpoint store for plan.execute steps

- **Status:** accepted
- **Date:** 2026-08-23
- **Roadmap:** 5.11
- **PRD:** v2 §14.11 (`CheckpointConfig`, "on crash/restart: load
  checkpoint, hash each input, skip if the hash exists, dispatch
  otherwise"), §13.5 (`Plan.checkpoint`).

## Context

`Plan.checkpoint` has ridden the signed wire envelope since Phase 2 and
nothing has ever read it: every plan re-executed every step from
scratch after any interruption. 5.11 makes the field real.

## Decision 1 — key on the plan NODE, validate with the input hash

V0008's `checkpoints` table is keyed `UNIQUE(plan_id, node_id)`, where
`node_id` is the plan node's id from the signed `Plan`. `input_hash`
— blake3 over the canonical JSON of the **resolved** input (after
`OutputRef` substitution) — is stored alongside as a *validity* check:
a hit requires both a node match and a hash match, so a re-planned
node whose input changed re-runs.

The obvious design — key on `(plan_id, input_hash)`, straight from the
PRD's "hash each input, skip if the hash exists" — is wrong, and the
plan review caught it before implementation. Input alone is not step
identity:

- **Cross-capability collision.** `fs.read {"path":"/x"}` and
  `fs.delete {"path":"/x"}` hash identically. Whichever ran first
  writes the row; the other is settled `ok` from that row, never
  dispatched, and its side effect never happens.
- **Within-run collapse, no crash needed.** Two legitimately distinct
  nodes with the same input — `notify.send {"msg":"deploy finished"}`
  at the start and end of a plan — collapse into one execution, since
  lookup and record share a table inside a single run.

Node ids are stable across resubmission (they live in the signed
plan), so keying on them costs nothing and bounds the table at
`MAX_PLAN_STEPS` (64) rows per plan.

**Canonicality.** `serde_json`'s object map is a `BTreeMap` in this
workspace (`preserve_order` is off — pinned by test), so encoding is
key-sorted and stable regardless of how a `Value` was built. `HashFn`
is matched exhaustively inside `harness-core`, where
`#[non_exhaustive]` still makes a future variant a compile error. A
hash that fails to match — for any reason, including a number that did
not survive a wire round-trip bit-identically — produces a MISS, which
re-runs the step. The failure mode is degraded, never wrong.

## Decision 2 — settle in `feed_ready`, before the step is minted

The checkpoint is consulted where inputs are resolved. On a hit the
step is settled with `scheduler.complete(node, Done(cached))` — the
same call the resolution-failure arm already makes — so no task row is
created, nothing is dispatched, dependents resolve against the
replayed output, and `newly_ready` cascades normally. Three
consequences fall out of that placement:

- **Budget bypass is automatic.** Spend accounting lives in the
  `FanoutEvent::Item` arm, which a settled step never reaches. This is
  the intended semantics: replayed work was paid for in the earlier
  run. The reporting consequence is real and deliberate — a resumed
  plan reports `ok: N` with `spent_usd: 0.00` — so the aggregate also
  carries `replayed: K`, and each replayed step is marked
  `from_checkpoint: true`. Total spend across runs can exceed the cap;
  each run's fresh spend cannot. Charging replays would make resume
  impossible for exactly the jobs checkpoints exist for.
- **The stop button is checked in the settle path too.** A
  fully-checkpointed resume settles the whole graph synchronously and
  never reaches the completion arm, where `own_cancelled` normally
  lives — without the extra check a cancelled plan would replay
  straight to `done`.
- **Only successful steps are recorded.** Re-running a failure on
  resume is the point.

## Decision 3 — GC when every step is done, not when status says "done"

`checkpoint_finish` (which drops the plan's rows) fires only when
`summary.done == summary.total`. The aggregate's `status` field is
*not* the condition: it reads `"done"` whenever no budget stop fired,
including a fail-fast abort. GCing there would delete precisely the
successful prefix an operator resubmits for. Budget stops, cancels,
aborts, deadline hits and crashes all keep their checkpoints.

A plan that crashes and is never resubmitted would keep its rows
forever, so the daemon sweeps checkpoints older than 7 days at boot,
beside the existing orphan sweep.

## Decision 4 — outputs are stored, not referenced

A checkpoint row holds the output JSON (capped at 256 KiB; a larger
output is simply not checkpointed and its step re-runs). The
alternative — store `task_id` and read the output back from
`task_results` on resume — avoids the duplicate blob, but couples
checkpoint validity to result-row lifetime and adds a second
authority for "what did this step return". With ≤64 rows per plan the
duplication is bounded, so the simpler, self-contained row wins.
`WITHOUT ROWID` was rejected for this table: it stores rows inside the
index B-tree, which is the wrong shape for a 256 KiB text column.

## Consequences and limits

- **Local only, so 5.12 is not free.** Checkpoints are local derived
  data and are never gossiped. `plan.execute` is `Cardinality::Anyone`,
  so a plan resubmitted after the coordinating node dies can land on a
  node with an empty table and re-run everything. 5.11 makes
  **same-node restart** cheap and correct — the real, common case.
  5.12 ("checkpoint resume on brain handover") must choose: pin the
  resumed plan to a node holding checkpoints, gossip checkpoints (a
  wire *and* privacy decision, not a detail), or scope itself to
  same-node restart. Recorded here so it is a decision, not a surprise.
- **This checkpoints DAG steps, not 100k fan-out items.** §14.11's
  headline example is a fan-out job; `plan.execute` caps a plan at 64
  steps and rejects larger ones. Item-level fan-out checkpointing
  (`harness fanout --checkpoint`) remains open in the v2 backlog.
- **Replay bypasses policy evaluation.** A settled step executes
  nowhere, so no policy runs on it. The invariant is: *a checkpoint
  replays a decision a prior policy evaluation already permitted;
  tightening policy does not retroactively invalidate checkpoints.*
  `from_checkpoint: true` keeps that auditable in the aggregate. A
  policy epoch in the key would change this and is not in scope.
- **Tampering with the local DB grants nothing new.** A forged
  checkpoint row injects JSON into a dependent's input — but that
  input is still schema-validated before dispatch and the dependent
  still executes under its own node's live policy. Anyone who can
  write `~/.harness/harness.db` can already edit `task_results`. Local
  trust model, no new privilege.
- **Webhook restart durability is NOT in this change.** It was carried
  here from ADR-0033 §6, but it needs a local
  `webhook_conversations` table (reply address, the `brain.plan` →
  `plan.execute` link, and a `reply_sent` flag for idempotency) — and
  specifically NOT a `reply_to` task tag: tags ride the signed
  envelope across the LAN, so a tag would replicate the user's phone
  number into every peer's database that executes the task. That is
  its own design, its own review, and its own PR.
