# ADR-0041: Audit log — per-node hash chains, and what they prove

- **Status:** accepted
- **Date:** 2026-08-24
- **Roadmap:** 5.13a, 5.13b (5.13c replication)
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
names. Verification anchors on that marker instead of a missing row.

The two steps are separate transactions, not one: `audit_append`
commits its own. The crash window between them is benign — a marker
with nothing deleted still verifies — and separate transactions also
avoid nesting `with_conn`, whose mutex is not reentrant. A prune
racing an append simply prunes slightly less than asked.

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

- **Verification is bounded per request, and says what it covered.**
  Walking a whole chain is O(N) inside the single store mutex, so the
  endpoint verifies at most one page's worth of local rows plus their
  anchor, and the response reports `from_seq`/`through_seq` rather
  than a bare "verified". The bound is a ROW count, not a seq span: a
  filtered page's rows are contiguous in time but scattered across
  the chain, so a span bound would restore the full walk. A page with
  no local rows reports `checked: false` — it proves nothing, and
  saying otherwise would be a lie. There is no verification cache;
  bounding the work was the simpler correct answer.
- **Because policy is evaluated on the executing node**, denials,
  secret reads and cloud escalations land on the *worker's* chain.
  Until 5.13c replicates, an operator's node sees only its own
  dispatch/cancel/resume rows — the API says which node's chain its
  `verified` flag covers.
- **Denials are attacker-triggerable, so 5.13b rate-limits them.**
  `rate_limit` is declared in manifests but enforced nowhere in the
  workspace, so a peer that can assign tasks appends one
  `shell.denied` row per attempt at submit rate. Retention (100k
  entries per node, pruned on the hourly housekeeping tick, through a
  marker so the survivor still verifies) bounds the disk cost — but on
  its own it means flooding pushes older entries out of the window.
  `StoreAuditSink` therefore rate-limits floodable actions (today:
  `shell.denied`) in a 60s window keyed **`(action, actor)`**.

  The key is deliberately subject-free. Keying on `subject` — the
  first thing one reaches for, and what the first draft of 5.13b
  did — is worse than doing nothing: a denial's subject IS the
  submitted command, so `/bin/x1`, `/bin/x2`, … mints a fresh key per
  attempt and the flood passes untouched, while the only records
  dropped are a real operator's repeated denials of one command. The
  trade is exactly backwards, and a reviewer caught it before merge.

  Within a window the first `BURST_ALLOWANCE` (10) records append IN
  FULL, so distinct denials keep their own argv, reason and task id at
  any rate a person produces. Past the allowance a record is dropped
  and counted, along with a bounded sample of the distinct subjects
  dropped (8 subjects, truncated; distinct counted exactly to 64, then
  reported as a floor). When the window CLOSES, a summary entry is
  appended carrying those counts, the sample and the window's own
  start — as its own entry, not folded into an existing row, because
  an existing row is already hashed into the chain and rewriting it is
  precisely what verification exists to catch. Closing runs on the
  housekeeping tick as well as inline: a burst that simply STOPS must
  still be recorded, or a completed flood shows its first ten rows and
  no trace of the rest.

  The summary's own detail is bounded by ENCODED size, not by the
  sample count: `audit_append` drops a detail WHOLE once its stored
  JSON passes `MAX_AUDIT_DETAIL_BYTES`, and the sampled subjects come
  from commands the adversary chose — a control character escapes to
  six bytes, so eight samples of them would blow the cap and take the
  suppressed count with them, erasing exactly the record the summary
  exists to keep. Control characters are folded out before sampling
  and samples are admitted one at a time against a byte budget, so
  the counts are written first and can never be crowded out.

  Costs and residuals, stated plainly. Past the allowance, per-attempt
  argv and reason are NOT retained — only the count and the sample.
  The flood threat is **mitigated, not closed**: 10 rows plus one
  summary per actor per minute is still ~15.8k rows/day/actor against
  the 100k bound, so a handful of flooding peers evicts history in
  days rather than minutes. The periodic close runs on the HOURLY
  housekeeping tick, so on an otherwise idle node a stopped burst can
  go unrecorded for up to an hour; the summary row's `at_ms` is its
  flush time, and the burst's real time is in `window_start_ms`
  (which the History page renders). Daemon shutdown force-closes
  every open window, elapsed or not — an operator restarting mid-flood
  is the expected reaction to a flood. That flush runs in `shutdown()`
  itself, NOT on the housekeeping task's shutdown arm: `shutdown()`
  aborts every spawned task synchronously before its first await, so
  such an arm is racy on a multi-thread runtime and unreachable on the
  current-thread one the daemon uses. An exit that does not run `shutdown()`
  still loses the open window's count — SIGKILL and power loss, but
  also **SIGTERM**, which is the normal production stop
  (`systemctl stop`, `docker stop`): `run_until_signal` awaits
  `tokio::signal::ctrl_c()`, which is SIGINT only, and no SIGTERM
  handler exists in the daemon. Handling SIGTERM is a daemon-lifecycle
  change affecting every subsystem's shutdown, so it is carried as its
  own item rather than widened into this one: that residual is
  irreducible for an in-memory map and is stated rather than implied
  away. The window map is in
  memory (a restart resets it, erring toward recording) and bounded at
  1024 open windows; hitting the bound CLOSES windows, emitting their
  summaries, rather than discarding counts — an eviction that dropped
  pending counts would let a flooder erase the record of its own
  attempts.
- **`peer.approved` is wired but dormant.** The auditor subscribes to
  the trust store's broadcast, which only `TrustStore::add` emits —
  and pairing's QUIC leg is still stubbed, so nothing calls it in
  this build. Peers loaded from `peers.toml` at open emit nothing.
  The subscription is the right shape for when pairing lands; until
  then this action cannot fire.
- **`argv_hash` protects argv from disclosure, not confirmation.**
  Once 5.13c replicates these rows, anyone holding the log can test
  candidate arguments offline. High-entropy secrets are safe;
  low-entropy ones (an internal hostname, a short password) are
  guessable. Keying the digest with the node's identity secret is the
  fix, and belongs with the replication PR that makes the exposure
  real.
- **`GET /api/v1/audit` pages by time, not `seq`**: `seq` is per-node
  and meaningless once two chains interleave.

## Amendment — 5.13c-1: peer head pins

5.13a's honest limit was that a node holds its own DB and its own key,
so it can rebuild its chain end to end and it will verify. A local hash
chain catches an edit made by something that is not the daemon; it does
not catch the node itself lying.

What a node cannot do is **un-tell a peer that already pinned
`(seq, entry_hash)`** — and it cannot rewrite the entries BETWEEN two
pins it cannot un-tell, because reproducing the later pinned hash over
altered entries is a blake3 collision problem. That second clause is
the mechanism. It needs entries, which 5.13c-2 supplies; 5.13c-1 builds
the pins it will check against.

**Decision 8 — pins are append-only, keyed `(node_id, seq)`.** The
obvious design is one row per node with "higher seq replaces". That
deletes the very pin the corroboration check needs: a node that
truncates and regrows past the pin then reads as ordinary growth, and
the ingest of the lie erases the only evidence of it. Supersession is a
verification obligation, never a delete. The PK is also the fork
detector — keying on the hash as well would store two conflicting
histories as two ordinary pins and detect nothing.

**Decision 9 — a lower-seq head is not evidence by itself.**
`AuditSyncEnvelope` is not `Sequenced` (pins are idempotent, and a
replayed head can only re-pin a position we hold), so any peer can
rebroadcast a genuine old head forever. Treating "below a pin we hold"
as a regression would turn that into a one-packet, permanent
defamation of an honest node — and a node returning from a partition
would trip it by accident. Only a contradiction AT a held position
counts.

**Decision 10 — head advertisement is on `harness.audit`, not the
heartbeat.** The obvious move is a `audit_head` field on `Heartbeat`,
on the `replica_head` precedent from ADR-0019. That precedent is
unsound in the new→old direction: `Signable::canonical_bytes` re-encodes
the DECODED struct, so a pre-5.13c node drops the unknown key,
re-encodes without it, and **signature verification fails** — killing
that connection's heartbeat channel permanently and aging the peer out
of the mesh. That is a wire BREAK, not an addition. A new channel has
no such failure mode: an unknown channel name is reset and the
connection survives (the `TASK_LEASE` precedent). ADR-0019's
`replica_head` got away with the same move only because nothing was
deployed; the general fix is carried as ROADMAP 6.0-adjacent work.

**Decision 11 — the inner key comes from our own trust store.** A head
is verified against ITS OWN node's key, which is what makes it
relayable: node C can hold A's head learned from B and it still proves
A said it. The key must therefore come from `TrustStore` (or a received
manifest), never from the relaying envelope — a relayer that supplied
the key would mint a keypair and forge the entire history it claims to
be reporting. A head for a node we cannot independently key is dropped,
not stored: an unverifiable accusation is worse than none.

**Decision 12 — thinning is status-aware and keys on `first_seen_ms`.**
The pin table needs a bound, and a naive "newest K plus a thinned tail"
sweep reaches Decision 8's failure by the back door: it eventually
evicts a `contradicted` pin, which is the evidence. So only `unchecked`
and `corroborated` pins are evictable, the oldest pin per node is
always kept, and thinning buckets on when WE first saw a pin rather
than on `seq` — a node that floods 100k entries must not be able to
push honest historical pins into the tail. A store test asserts the
Rust `evictable()` predicate and the thinning SQL agree, because they
are two statements of the same rule.

**Decision 13 — a low head is classified, not discarded.** The first
version of `pin_peer_head` returned `StaleIgnored` *without inserting*.
That let one validly-signed head at `u64::MAX` permanently immunize its
sender: every genuine head afterwards compared below the poisoned
maximum, was dropped, and no peer ever pinned that node again — with no
log line and no removal path. Not treating a low head as EVIDENCE is
correct and is Decision 9; not STORING it is a different thing, and it
was the one that mattered. A below-newest head is now pinned and
classified `StalePinned`, which costs one row and buys a position a
later contradiction can fork against. Relatedly, an oversized `seq` is
REJECTED rather than clamped to `i64::MAX`: clamping collides distinct
signed heads onto one row (a fork manufactured by a lossy cast) and
makes the relayed rebuild carry a seq the signer never signed, so it
fails verification at every receiver.

**Decision 14 — relays rank on our clock, not the head's.** `at_ms` is
stamped by the head's own signer, so ordering the relay window by it
lets a node stamp `u64::MAX` and hold a slot forever, pushing honest
heads out on any mesh larger than the window. Ranking uses
`observed_at_ms`, which is ours. The same rule applies to entry
ordering in 5.13c-2.

### What 5.13c-1 does NOT yet give you

Pins alone detect two things: two signed heads at one position, and a
first-party head contradicting a pin we hold. They do **not** detect
the dominant rewrite shape — truncate, re-append, grow past the pin —
because linking pin `(100, h100)` to pin `(200, h200)` requires walking
entries 101..200. Until 5.13c-2 pulls entries, every pin sits at
`unchecked`, and the UI must render that as what it is rather than as
corroboration.

Three further gaps, named so they are not mistaken for coverage:

- **Detection of an in-place rewrite is racy against the push
  cadence.** The newest pin sits at the subject's current head, so a
  rewrite that leaves `seq` unchanged forks on the next push — but a
  node that appends one entry first lands its new head at an unpinned
  position, which reads as ordinary growth. Pin density is the
  mitigation, and it is a 30s timer, not a guarantee.
- **A node that never advertises is invisible.** Nothing consumes
  `observed_at_ms` to age a peer out or flag silence.
- **A detected fork has no reader.** `head_conflicts`,
  `peer_head_pins` and `set_pin_status` have no non-test callers: a
  fork surfaces as one `tracing::error!` and a database row. The
  History UI shipped in 5.13b shows nothing. Surfacing it is 5.13c-3
  work, and until then the loudest signal this feature produces is a
  log line.
