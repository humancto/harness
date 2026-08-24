# ADR-0041: Audit log — per-node hash chains, and what they prove

- **Status:** accepted
- **Date:** 2026-08-24
- **Roadmap:** 5.13a (5.13b History UI, 5.13c replication)
- **PRD:** v2 §10.6 — *"Every privileged action (dispatch, shell exec,
  secret access, peer approval, policy change, cloud escalation) →
  append-only audit log replicated to every node. Tamper-evident via
  hash chain. Viewable in History tab."*
- **Supersedes:** ADR-0006's standing promise to introduce `automerge`
  for the audit log (see Decision 5).

## Decision 1 — one chain per node, not one chain per mesh

A single mesh-wide chain would require agreement on who appends next.
That is consensus, and "no broker, no consensus" is load-bearing in
this design. So each node appends only to its own chain, keyed
`(node_id, seq)`, and 5.13c replicates the others'.

The cost is honest and worth stating: **there is no global order.**
Entries carry `at_ms` and `node_id`; the History view merges by time,
and clock skew across nodes makes that view approximate. Order within
a node is exact.

## Decision 2 — what "tamper-evident" actually buys

`entry_hash` covers the entry's fields *and* its position (`node_id`,
`seq`) and its predecessor, so editing any stored row breaks
verification from that row forward. That detects edits made **outside
the daemon**: `sqlite3` at the shell, bit rot, a restore from a bad
backup.

It does **not** make a node honest about itself. A node holds its own
database and its own signing key; it can rebuild its chain end to end
and sign the result. What it cannot do is un-tell a peer that already
pinned `(seq, entry_hash)` at an earlier time. That is why
`Store::signed_audit_head` ships now even though nothing gossips it
yet — and why 5.13c, not this PR, is the item that turns the chain
into evidence. Anyone reading the History page before then is looking
at a local integrity check.

The hash is over a JSON **object**, never `entry ‖ prev_hash`: this
repo already rejected concatenation in `step_hash`, and `subject`,
`detail` and `actor` are free-form enough to make `a ‖ b` a live
collision surface. `detail` is hashed exactly as stored — never
re-serialized, which would invite float and escape drift across
versions.

## Decision 3 — the record carries identifiers, never payloads

`actor` is a **closed enum** (`local_admin` | `webhook:<channel>` |
`peer:<node>` | `system`), not free text. There is no user identity in
this system — sessions are anonymous bearer tokens behind one admin
password — so a "session" actor would put token material into a
persisted, soon-to-be-replicated table. And a webhook actor naming the
sender would replicate the user's phone number across the LAN, which
is exactly the defect 5.11 refused when it kept `reply_to` out of task
tags. Only the channel is recorded.

`detail` follows the same rule:

- **dispatch** → capability, target node — never the task input,
  because a webhook-minted plan's input *is* the user's message text.
- **shell.exec** → the command and an `argv_hash`, never argv, which
  routinely carries credentials (`curl -H "Authorization: …"`).
  Denials also record the policy reason.
- **secret access** → the TAG, never the value. Recorded by wrapping
  `SecretsStore`, so every present and future consumer is covered.
  (The routing-side `SecretAwareLiveSet` is not the access site — its
  own docs say it is not a security boundary.)
- **cloud escalation** → the backend id and which triggers opened the
  gate, never the goal.

## Decision 4 — retention prunes *through* a marker

Retention appends an `audit.truncated` entry carrying
`{through_seq, through_hash}` **first**, then deletes the rows it
names, in the same transaction. Verification seeds from the marker
instead of a missing row.

The naive alternative — prune, then note it — leaves entry `N+1`
pointing at a row that no longer exists, so **every node that ever hit
the retention bound shows a permanent "BROKEN" banner**, which trains
operators to ignore the one signal the feature exists to produce. A
locally-forged marker is of course indistinguishable from a real one
until heads are pinned by peers (Decision 2).

## Decision 5 — no `automerge`, and here is why

ADR-0006 chose a custom LWW map for task state and deferred the
`automerge` question to "the audit log at 5.13". The answer is that a
CRDT sequence solves a problem this does not have: its defining
property is convergent *reordering and interleaving* of concurrent
inserts, which is flatly incompatible with a hash chain over
positions. Per-node append-only logs are already conflict-free —
exactly one writer per chain — so there is no merge algorithm to
choose. ADR-0006's promise is retired, not deferred again.

## Consequences

- **Verification is cached, not per-request.** Walking a chain is
  O(N) inside the single store mutex; doing it on every History page
  load would let any authenticated caller stall the 100 ms dispatch
  poll. The endpoint verifies only the page it returns.
- **Because policy is evaluated on the executing node**, denials,
  secret reads and cloud escalations land on the *worker's* chain.
  Until 5.13c replicates, an operator's node sees only its own
  dispatch/cancel/resume rows — the API says which node's chain its
  `verified` flag covers.
- **Denials are attacker-triggerable**, and `rate_limit` is declared
  in manifests but enforced nowhere in the workspace, so a peer that
  can assign tasks can append at submit rate. Retention is by row
  count, so flooding can evict older entries. Coalescing denials into
  a suppression window is recorded as follow-up work in 5.13b rather
  than left unsaid.
- **`GET /api/v1/audit` pages by time, not `seq`**: `seq` is per-node
  and meaningless once two chains interleave.
