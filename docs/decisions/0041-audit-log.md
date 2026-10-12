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
  such an arm never runs. (An earlier version of this sentence said the
  daemon uses a current-thread runtime; it does not — `run_daemon`
  builds `new_multi_thread`, and only `run_async_cli` is
  current-thread. Round 4 of the 5.13c-2 review caught the error. The
  conclusion is unchanged: the arm is unreachable because the abort
  precedes the await, on either flavour.) An exit that does not run
  `shutdown()`
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

**Decision 14 — relays rank AND select on our clock, not the head's.**
`at_ms` is stamped by the head's own signer, so ordering the relay
window by it lets a node stamp `u64::MAX` and hold a slot forever,
pushing honest heads out on any mesh larger than the window. Ranking
uses `observed_at_ms`, which is ours.

The first version of this decision fixed only the cross-node ranking
and left the within-node selection on `ORDER BY seq DESC` — and `seq`
is exactly as attacker-chosen as `at_ms` was. `RejectedSeq` stops only
values above `i64::MAX`, so a head signed at exactly `i64::MAX` would
be selected as that node's newest pin forever, and every honest peer
would relay that garbage on its behalf permanently. Selection now
orders on `observed_at_ms` first. The general rule, which 5.13c-2
inherits for entry ordering: **no ordering decision may key on a field
the subject of that ordering controls.**

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
- **Ingest is unrated.** A trusted peer can insert pin rows faster
  than the hourly sweep reclaims them; thinning bounds the steady
  state but not the interval. This is the only unbounded-work path a
  trusted peer controls today. It was planned for 5.13c-2 beside the
  entry-serving rate limit; 5.13c-2 shipped the serving limit and NOT
  this one, because two attempts at it there produced a blocker each
  (one of them suppressing fork detection). It is ROADMAP **5.13c-1b**
  — see Decision 27 for the evidence.
- **A detected fork has no reader.** `head_conflicts`,
  `peer_head_pins` and `set_pin_status` have no non-test callers: a
  fork surfaces as one `tracing::error!` and a database row. The
  History UI shipped in 5.13b shows nothing. Surfacing it is 5.13c-3
  work, and until then the loudest signal this feature produces is a
  log line.

## Amendment — 5.13c-2: entries, and what a pin is worth

5.13c-1 built pins. A pin catches a fork at one position: two signed
heads at the same seq. It does not catch the rewrite shape that
matters — truncate, re-append, grow past the pin — because the
rewritten node's new head lands at a position nobody pinned and reads
as ordinary growth.

Linking pin `(100, h100)` to pin `(200, h200)` means walking entries
101..200 and checking that each hashes to the next one's `prev_hash`.
Reproducing `h200` over altered entries is then a blake3 collision
problem. **That walk is the guarantee**; the pins are what make it
meaningful. This amendment adds the walk.

**Decision 15 — batches commit as they link; a run marker tracks the
rest.** Two constraints in the plan contradicted each other: "ingest is
all-or-nothing" and "cap the rows per pull". With a cap, every batch
but the last ends short of the pin, so all-or-nothing makes the last
batch unreachable. Resolution: a batch commits when it links to the
last row we committed for that run, and `audit_ingest_runs` records
that the walk is incomplete. Only a run that reaches its target pin
with a matching hash corroborates it — so a half-finished pull is
never mistaken for evidence, which was the real thing all-or-nothing
was protecting.

**Decision 16 — `audit_verify_range` is a separate function.**
`audit_verify_chain`'s `anchor_holds` accepts three anchors: seq 1
with a zero `prev_hash`, a locally stored predecessor, or a truncation
marker in that node's chain. A legitimate partial pull of 500..600 has
none of them and reports `Broken { at_seq: 500 }` — honest replication
flagged as tampering. The fourth anchor is a pin WE took at
`first.seq - 1` whose hash matches the first row's `prev_hash`. It must
be a pin already held, never a head carried in the same message that
delivered the rows: otherwise the sender supplies both the claim and
its own corroboration.

**Decision 17 — the receive clock governs ordering, and skew is
refused.** A peer's `at_ms` sits inside its own hash preimage, so it is
chosen by the peer, and `audit_recent` orders by it — one ingested row
stamped `u64::MAX` would occupy page 1 of every node's History
forever. Ingested rows carry `received_at_ms` (ours), the merged feed
orders on `COALESCE(received_at_ms, at_ms)`, and a row implausibly far
ahead of our clock is refused outright. This is 5.13c-1's rule applied
again: **no ordering decision may key on a field the subject of that
ordering controls.**

**Decision 18 — serving is rate-limited, and gated on
`tier != Guest`.** Reading rows holds the single process-wide
connection mutex and a `RangeReq` is replayable, so an unlimited
server lets a trusted peer stall dispatch by asking repeatedly; the
limit is 20 batches per peer per minute. It does NOT close the "ingest
is unrated" gap 5.13c-1 recorded, though an earlier version of this
decision claimed it did: `may_serve` gates `serve_range` on
`harness.audit.entries`, while that gap is pin-row insertion by
`ingest` on `harness.audit` — a different channel and a different
path, which is why closing it needed a separate budget (Decision 27).

The tier gate says
`!= Guest` rather than `== Trusted` because nothing in this build
constructs `TrustTier::Trusted` outside tests and pairing's QUIC leg is
stubbed — gating on Trusted would ship the entire pull path inert.
**`peers.toml` membership is the trust boundary in this build**, and
that is a statement about what exists, not an aspiration.

Be precise about what the tier gate reaches, though, because the
sentence above overstates it: `TrustStore::tier` returns
`TrustTier::Default` for an *unknown* node, and nothing in this build
constructs `Guest` either. So the gate fires only for a peer an
operator hand-marked `tier = "guest"` in `peers.toml`. The admission
that actually keeps strangers out is at the transport —
`incoming.accept(|pk| trust.lookup_by_pubkey(pk).is_some())` in
`lifecycle.rs` — and the rate limit is what bounds an admitted peer.
The tier gate is an operator's explicit downgrade switch, not the
thing standing between the mesh and an unknown caller. Round 4 of the
review found both of these guards surviving their mutation test — the
whole `harness-daemon` suite stayed green with either one deleted — so
they now have tests of their own. A security property asserted here
with no test is how the first three rounds of this PR went wrong.

**Decision 19 — a served batch is bounded by bytes as well as rows.**
`detail` is capped at 4 KiB per row, so a 5000-row cap alone permits a
20 MB frame. The budget is 768 KiB against a 1 MiB channel cap;
truncation is reported so the requester resumes where the server
stopped.

**Decision 20 — the walk must be anchored, and the answer must start
where we asked.** Review found the headline property was not actually
delivered: `consume_range` never checked that a batch begins at the
requested `from_seq`, and `audit_ingest_range` required contiguity only
when a run row already existed. A malicious subject could answer
"give me 1..200" with **only** entry 200 — it self-hashes, it is the
pinned hash, `reached_pin` is true, and the pin is marked corroborated
without a single preceding entry being walked. Three guards now stand
between a reply and a corroborated pin: the response must start at the
requested seq; a fresh run above seq 1 must anchor on a stored
predecessor or a pin we already hold whose hash matches; and a
continuing batch must hash-link to the last row committed, not merely
follow it numerically.

**Decision 21 — corroboration is re-derived, and the verifier has the
last word.** `audit_run_corroborates` consults `audit_verify_range`
rather than trusting the run marker. The two used to disagree with
nothing reconciling them: a run stitched from two different histories
reported corroboration while the verifier called the same rows broken.
Nothing is cached — tampering after a completed walk revokes
corroboration on the next read.

**Decision 22 — a same-seq disagreement is refused, not swallowed.**
Ingest used `INSERT OR IGNORE`, which silently kept our existing row,
reported rows it had not written, and threw away the fact that a peer
offered a *different* entry at a position we already held — which is
precisely the shape 5.13c-1 detects at the head. It is now an explicit
`Conflict` refusal, and `committed` counts rows actually written.

Deliberately NOT called evidence in the code: the refusal logs a
warning and writes **nothing** to `audit_head_conflicts`, and no pin
becomes `Contradicted`. Entry-level disagreement is strictly stronger
proof than the head-level fork that *is* recorded, so this is an
asymmetry worth naming rather than glossing. It stays a refusal for
one reason: the benign cause — a peer that rebuilt its database and
restarted at seq 1 — is indistinguishable here from a malicious one,
and auto-recording a conflict would fire on the honest case. Promoting
it to recorded evidence needs the operator surface and the repair path
that 5.13c-3 brings.

**Decision 23 — `contradicted` is terminal, and the only exit is an
operator acknowledgement.** The second review round found that an
ordinary entry walk could repaint fork evidence green, so
`set_pin_status` now refuses to move a pin out of `contradicted`. That
refusal is deliberately absolute rather than "unless the walk is
convincing": the walk sees only the side of the fork we happen to
hold, and both sides of a real fork walk cleanly — a fork is two
internally consistent histories, which is exactly why the head
collision and not the walk is the detector. A verdict a walk could
retire would be a verdict an attacker retires by serving us its own
branch.

The consequence is that `contradicted` is a state the code cannot
leave on its own, and 5.13c-1's benign cause (a peer that rebuilt its
database) therefore sticks. That is accepted, not overlooked: a stuck
amber state is recoverable by a human, a silently cleared one is not.
`force_pin_status` exists for precisely one caller — 5.13c-3's
conflict acknowledgement, where a human has seen both heads — and its
doc comment names that caller and states that fork detection itself
must not use it (fork detection writes the `contradicted` status
inline, inside its own `with_conn` closure, because `with_conn` is
non-reentrant and calling the setter from there would deadlock). Until
5.13c-3 lands there is no repair path at all, and `i22` pins the
behaviour so the exemption cannot quietly widen. `unverifiable`, by
contrast, is **not** terminal (`i23`): it records an absence of
evidence, and evidence arriving later should be allowed to settle it.

**Decision 24 — entry pulls get their own channel, because putting
them on the head envelope was the exact break Decision 10 forbids.**
The first implementation added `range_req`/`range_resp` as two
`Option` fields on `AuditSyncEnvelope`, with
`#[serde(default, skip_serializing_if = "Option::is_none")]` and a
comment claiming that made it compatible. The `default` covers only
the old→new direction. The direction that matters is new→old, and
Decision 10 spells out the mechanism three screens up:
`Signable::canonical_bytes` re-encodes the DECODED struct, so a
receiver that does not know a field drops it, re-encodes without it,
and signature verification fails.

Measured, not reasoned about — round 4 of the review built a probe
declaring the 5.13c-1 struct shape and verifying real envelopes
against it:

```
push envelope      -> old node verify: Ok(())
range_req envelope -> old node DECODED, verify: Err(BadSignature)
```

`skip_serializing_if` saves a plain head push, so the defect hides
until the first pull. Then: the old node's `channel_recv_loop` takes
the error branch and **tears down its `harness.audit` stream**, our
next head push to it fails, `note_send_failed` drives the backoff, and
head pushes to that peer stop. Pin density is the security parameter
of this whole design, so a change that silently reduces it on contact
with an older peer is a wire BREAK dressed as an addition — Decision
10's words, applied to the layer below where it was written. There is
no protocol-version negotiation anywhere in this build, so nothing
would have prevented it.

`range_req`/`range_resp` therefore live on `harness.audit.entries` in
their own signed `AuditEntriesEnvelope`, and `AuditSyncEnvelope` is
byte-identical to its 5.13c-1 shape again (`harness.audit` keeps the
64 KiB default frame too; the 1 MiB cap moved to the new channel with
the batches that needed it). An old node returns `None` from
`channels::known`, resets the one unknown stream, and both the
connection and its head pushes survive. That is the `TASK_LEASE`
precedent, and it is the same reasoning that kept heads off the
heartbeat in 5.13c-1 — the lesson simply had to be learned twice, one
layer down.

A consequence worth stating: this is now the shape any future field
on either envelope must take. A new field on a signed wire type is
never additive across versions in this codebase; a new channel is.

**Decision 25 — choosing ONE verify target cannot be made
adversary-proof, so the ordering is defensive and the operator gets an
override.** This took four attempts, each keying on something the
subject controls, and the sequence is worth keeping because the same
mistake kept reappearing in a new costume:

| attempt | rule | how the subject took it |
|---|---|---|
| 1 | `MAX(seq)` | a head signed at `i64::MAX` owns the slot forever |
| 2 | `observed_at_ms DESC, seq DESC` | all heads in one envelope share `now_ms`, so the subject manufactures a tie and `seq DESC` resolves it |
| 3 | + `seq ASC`, + `status <> 'unchecked'`, + dedupe keeping the lowest seq | the envelope is not replay-protected, so re-pushing a genuine OLD head re-stamps `observed_at_ms` on that row; `Contradicted` is also `<> 'unchecked'`, so self-forking the newest position hands the slot back to an older pin; and the dedupe DROPPED the honest head when the subject attached a lower one |
| 4 | `first_seen_ms DESC, seq ASC`, pin every distinct head | — |

Attempt 3 was worse than attempt 2, which is the part worth
remembering: in trying to stop the subject *out-ranking* its honest
head, it stopped the honest head from being *stored at all*, collapsing
that node's pin density to a position of the subject's choosing and
poisoning every honest peer's relay slot for it. Discarding evidence to
fix an ordering problem is never the trade.

The rule now: `first_seen_ms` is written once on INSERT and never
touched by the `Refreshed` branch, so a replayed head cannot re-win.
The tie breaks on `seq ASC` — still the subject's field, but in the
only direction that cannot be inflated, since winning a tie requires
claiming a *lower*, therefore reachable, position. Not `rowid`: inside
one envelope `rowid` is the sender's `Vec` order, so `rowid DESC` just
hands the slot to whichever head the sender listed last. The relay
selector shares the `first_seen_ms` half and deliberately differs on
the tie — see Decision 27. `p09` pins the verify selector; `m25` the
relay one.

**And it is still not enough.** The first version of this paragraph
said the residual was a *subject* pushing a fresh *inflated* head
*every tick*. Round 7 measured the actual attack and it was the
opposite on all three counts: **any trusted peer** (not just the
subject), **one envelope** (no repetition), carrying a **lower**
genuine position — the direction this decision had just finished
calling safe. Worse, with the `status` clause gone, resolving the
captured pin did not advance the slot either, so the capture was
permanent under all three verdicts.

What closed the demonstrated captures was not a cleverer ordering: it
was removing the selection's monopoly. `POST
/api/v1/audit/verify/:node?seq=N` lets an operator name the position,
and 5.13c-3a's eager sweep walks several unchecked pins per tick rather
than one. The relay side still has exactly one slot, and Decision 27
explains why fixing that belongs to its own item. What
remains is narrow and worth stating exactly: a peer can still make the
DEFAULT target a position of its choosing for a window, and a pin whose
walk fails stays `unchecked` and keeps that default. Naming the pin
bypasses it; nothing silently reports success.

**Decision 26 — the local and ingest `subject` caps differ, on
purpose.** `subject` had no cap at all before 5.13c-2, so rows far above
any new cap already exist on nodes that ran a long command under an
older build, and `subject` is inside `entry_hash` — such a row can never
be truncated into acceptability. Refusing one is fatal rather than
cosmetic: `validate_batch` stops at the first offending row and no
follow-up is issued, so a single legacy row makes the whole chain past
it permanently unverifiable. So new local appends truncate at
`MAX_AUDIT_SUBJECT_BYTES` (4 KiB, matching `detail`) while ingest admits
up to `MAX_INGESTED_SUBJECT_BYTES`, derived below. An intermediate commit set
them equal and called them "agreed by construction"; they agreed only
for rows written after that commit, and the migration case is the one
that matters.

The ceiling is the CHANNEL FRAME cap, and getting there took two
wrong answers. 64 KiB stranded legacy rows from 64 KiB to ~1 MiB.
`MAX_SERVE_BYTES` (768 KiB) read as airtight — "a row larger than that
cannot cross the wire anyway" — but the `harness.audit.entries` frame
cap is 1 MiB, and `audit_entries_for_range` admits its first row
unconditionally, so a 900 KiB `subject` was served, fitted the frame,
arrived, and was refused `SubjectTooLarge` — fatally, stranding the
chain past it. The same failure mode, moved up a band, under text
asserting it was closed (round 9 finding 4). At the frame cap no band
is left: a row that cannot be framed cannot arrive, and anything that
arrives is accepted. `m26` pins the two constants together.

**Decision 27 — the pin flood-control layer is NOT in this item, and
here is the evidence for why it needs its own.** Rounds 7 and 8 found
two real problems in the pin layer and I tried to fix both inside this
PR. Both fixes were wrong, in ways worth recording:

- A per-peer budget on new pin positions, checked before
  `pin_peer_head`. Because the check ran per head and skipped the call
  entirely, once a window's budget was spent EVERY head from that peer
  was dropped — including the ones whose outcome would have been
  `Fork`. A node could hide a rewrite of its own chain behind 24
  freshly-signed junk heads in a single envelope: no conflict row, no
  `AUDIT CHAIN FORK` line, nothing. A rate limit that disables the
  detection this subsystem exists for. It also tripped on honest
  traffic: a peer pushes `1 + RELAY_HEADS` heads every
  `HEAD_PUSH_INTERVAL`, which is 34 per window against a budget of 24,
  while the constant's own doc claimed it "never trips".
- Relaying a bounded SET instead of one pin. The set was selected by
  `first_seen_ms DESC` — which the REPORTING peer controls, since it
  chooses which previously-unseen genuine positions to introduce. Three
  unseen old heads took all three slots and the honest high position was
  EVICTED from the relay envelope: strictly worse than the single slot
  it was fixing.

Neither problem is a regression from this item — `main` already pins
every head with no budget and relays one pin per peer, so both are
5.13c-1 residuals that review surfaced here. Both are security-sensitive
and both produced a blocker on first attempt. They are ROADMAP
**5.13c-1b**, with the probe shapes recorded there.

What this item keeps, because they are its own:

- `PinStatus::evictable` admits only `Unchecked`. `Corroborated` is a
  status THIS item introduces, so protecting it from the sweep is this
  item's job — and round 8 showed one 64-head envelope plus the
  ordinary hourly sweep deleting it, unrecoverably, since
  `audit_run_corroborates` reads the pin. The keep-recent exemption
  counts only `unchecked` rows above a candidate, or permanent rows
  would push every unchecked pin out of the window at once (`p06e`).
- The relay slot keeps `first_seen_ms DESC, seq DESC`: `first_seen_ms`
  because the `Refreshed` branch re-stamps `observed_at_ms` (what
  `main` ordered on) and a re-push must not re-take the slot, and `seq
  DESC` rather than the verify path's `seq ASC` because for relay a
  lower position is simply worse. One slot is still capturable by a
  peer introducing an unseen position — and `seq DESC` opens the other
  direction while closing that one, since it hands the slot to an
  INFLATED self-claim. Both are 5.13c-1b, which records both;
  no single-slot rule satisfies them together.
- The serve path now applies its byte budget INSIDE the cursor loop.
  It used to collect up to `cap + 1` rows and then trim, so serving 7
  rows out of a wide span read and allocated the whole span under the
  process-wide mutex — the row cap bounded the count and nothing
  bounded the bytes read.

### What 5.13c-2 does NOT give you

- **The pull is manual or opportunistic, not eager.** A pin is
  corroborated when something asks for it — the endpoint, or a
  follow-up after a truncated batch. Peers prune at 100k entries, so a
  pin never walked before its owner pruned past it becomes permanently
  unverifiable. Background corroboration is 5.13c-3, and it is a
  requirement rather than a nicety for exactly that reason.
- **`unverifiable` is defined but never set.** Nothing yet detects
  "the owner pruned through this position", so the status exists for
  5.13c-3 to use.
- **Ingested peer rows have no retention and no repair path.**
  `prune_audit_log` runs only for the local node, and
  `audit_forget_peer_range` is 5.13c-3. A peer that rebuilt its
  database from seq 1 will collide with rows we already hold and be
  refused (Decision 22) with no way to clear them until then — the
  refusal is correct, the missing remedy is the gap.
- **`audit_ingest_runs` is not thinned.** One row per
  `(node, target_seq)`; it grows with pin density and the pin sweep
  does not touch it.
- **Corroboration is not cheap, and Decision 21 makes it uncached.**
  Re-deriving it walks the whole run. Measured on this machine, over a
  chain built through the real sink and verified end to end:

  | rows | debug | release |
  |---|---|---|
  | 20 000 | 422 ms | 64 ms |
  | 100 000 | 2.12 s | 230 ms |

  An earlier draft of this ADR said "roughly 107 ms (debug) for a
  20k-row run … about half a second at the 100k retention bound"; the
  diff review called the figure out and the measurement above
  confirms it was understated about fourfold. The number that matters
  for a shipped build is **230 ms at the 100k retention bound**.

  That 230 ms is spent holding the single process-wide connection
  mutex, inside the `harness.audit.entries` recv arm — which is an
  ordinary async task on a tokio worker. (The ENTRIES arm, not the head
  arm: after Decision 24's split `harness.audit` carries only head
  envelopes.) So one settle stalls both every other
  store caller on the node and one runtime worker, on a path a peer
  triggers by answering a request.

  Wrapping the settle in `spawn_blocking` was considered and
  rejected as this item's fix. It would free the worker thread and do
  nothing at all about the mutex, which is the larger half; it would
  single out the settle while leaving `audit_ingest_range`'s
  up-to-5000-row write inline on the same task under the same mutex,
  which is incoherent; and it would make the pin status land after
  `ingest()` returns, turning every assertion about it into a race.
  The coherent fix is to move the whole `harness.audit.entries` recv
  arm off the runtime's workers, which is a change to `peer_net.rs`'s
  channel loop and is out of this item's scope — ROADMAP **6.0a**.
  Until then the stall is real and documented rather than hidden.

  5.13c-3's per-pin UI will multiply this cost, and a cache there must
  not become a way for a stale verdict to outlive the rows it was
  derived from.
- **A subject can keep its own honest head out of the default verify
  target.** Decision 25 removes every *permanent* capture we found, but
  not the general case: minting a fresh signed position each tick takes
  whatever slot a "newest" rule picks. Use `?seq=` to name the pin, and
  5.13c-3a's sweep removes the single-target dependency.
- **A verify against a pre-5.13c-2 peer returns `202 started: true`
  and then never settles.** Decision 24 buys that the connection and
  its head pushes survive, but there is no version or capability
  signal anywhere in this build, so the request opens
  `harness.audit.entries`, that peer resets the unknown stream, and the
  operator's only feedback is a pin that stays `unchecked` forever —
  indistinguishable from a slow walk. A capability signal is the real
  fix and belongs with 5.13c-3's operator surface, which is where the
  distinction becomes visible.
- **A 1 MiB `subject` ceiling means a feed page can render half a
  gigabyte.** `audit_recent` materializes up to `MAX_LIMIT` (500) rows
  across all nodes inside `with_conn`, and the handler clones every
  `subject` into a DTO and serializes it — so `MAX_LIMIT ×
  MAX_INGESTED_SUBJECT_BYTES` is ~512 MB of strings per authenticated
  `GET /api/v1/audit?limit=500`, with the process-wide store mutex held
  for the read, and ingested rows are never pruned. This is the hazard
  the ingest `detail` cap exists to prevent, reopened for `subject` at
  256x the local cap — deliberately, because the alternative strands
  honest chains (Decision 26), but the cost belongs written down.
  Reaching it takes a trusted peer writing such rows into its own
  chain and ~17 operator-triggered verifies, since the 1 MiB frame
  admits one such row per batch. `verify_page` and the keyset cursor
  are unaffected: they touch only local rows, which `truncate_subject`
  clamps at 4 KiB. The remedy is render-side truncation in the DTO —
  safe, because `entry_hash` is reported separately and the stored row
  is untouched — and it belongs with 5.13c-3's `audit_forget_peer_range`.
- **Still no reader.** `audit_verify_range`, the run table and the
  corroborated status have no UI. A fork or a failed corroboration
  surfaces as a log line, a database row, and a 202 from the verify
  endpoint. 5.13c-3 owns the operator surface.

## Amendment — 5.13c-1b-i: what the ingest allowance cost to attempt

Decision 27 moved the pin flood-control layer out of 5.13c-2 and
promised 1b would record its resolution. This amendment records
something less convenient: the allowance was implemented twice, failed
adversarial review both times for the same reason, and has been taken
back out. What 1b-i ships is the substrate plus two bugs the attempt
uncovered. The allowance itself is 5.13c-1b-ii.

Writing this down rather than quietly re-cutting the scope is the
point: four earlier rounds in this subsystem each "fixed" an ordering
defect with a new ordering defect, and the only thing that broke the
cycle was naming the rule being broken.

**Decision 28 — an allowance whose spend order the SENDER can steer is
worse than no allowance, so no allowance ships until the order is
sender-independent.**

The rule this subsystem keeps breaking: *no ordering decision may key
on a field the subject of that ordering controls.* Two attempts:

- **Attempt 1 — per head, in the order received.** `ingest` iterates
  the sender's `Vec`, so the budget was spent in the sender's order: a
  subject lists junk positions first and its genuine head last, and
  the honest position is refused in every window, in every peer,
  forever. Rejected in review.
- **Attempt 2 — two tiers, `(best_per_subject != seq, Reverse(seq),
  index)`.** Intended to give each subject its newest position first.
  `best_per_subject` was built ONLY from the sender's envelope and
  never consulted `audit_peer_heads`, so tier-0 membership and the
  intra-tier order both keyed on `seq` — a field the subject mints.
  Two probes disproved the two properties this ADR had claimed:
  - "A subject cannot crowd out its own honest head": with
    `[(N1, seq 9_000, honest), (N1, seq 9_000_000, bogus)]` and one
    unit of allowance, the bogus head takes tier 0 and the honest one
    is refused.
  - "A relayer cannot crowd out a third party's": with
    `[(N1, seq 5, third party), (N9, seq 1_000_000, relayer's own)]`,
    the relayer's own head wins.

  Attempt 2 is strictly worse than attempt 1, which is why it is worth
  recording. Attempt 1 keyed on the sender's `Vec` index, which is
  rewritten by every relay. `seq` is covered by the subject's
  signature, so a head stamped `seq = i64::MAX` holds first place in
  every relayer's envelope at every receiver, permanently.

What 1b-ii must do instead: allocate **round-robin across distinct
subjects**, subjects ordered by something we own (our own
`first_seen_ms` for that subject), and let each subject's own slot be
filled by whichever of its heads it likes — a choice that can only
affect itself. With `S` distinct subjects and allowance `A` every
subject gets at least `floor(A / S)`, which is the property this ADR
claimed and did not have, and it is a property of the allocator rather
than of a sort key. Two further constraints 1b-ii inherits:

- Candidates must be deduped by `(node_id, seq)` BEFORE the allowance
  is applied, or a sender fills the selection window with free
  duplicates of one position: 24 heads at one position spent 1 unit
  and produced 23 forks. **That dedupe is for SELECTION only, and it
  must not become the ingest dedupe.** `ingest` keys on
  `(node_id, seq, entry_hash)`; narrowing it to `(node_id, seq)`
  destroys the second branch of a same-envelope fork before anything
  can classify it — one ordinary pin, no conflict row, no log line,
  and a subject double-signs with impunity. That narrowing left the
  whole workspace green when this amendment was first written, which
  is why `m28` now exists at the ingest layer: every head still
  reaches `pin_one` and is still classified, and only the allowance's
  own candidate list is collapsed.
- `Fork`, `Refreshed` and `RejectedSeq` are classifications, not new
  rows, and must never be refused. An allowance that suppressed
  `Fork` would let a node hide a rewrite of its own chain behind junk
  heads. Attempt 1 did exactly that.

A same-envelope fork at a position we do NOT already hold is the sharp
edge here, and it is why the allowance cannot simply be re-added over
this item's batching: with the budget spent, neither branch inserts,
both read back no stored row, and the fork is classified as a refusal
— no conflict row, no log line. In 1b-i, with no allowance, the first
branch always inserts and the second is a genuine `Fork` (`p11`).
1b-ii must keep that true, and the honest way is to write the conflict
row from the two in-batch branches directly, ordered canonically by
`entry_hash` so a replay with the branches swapped dedupes on
`UNIQUE(node_id, seq, held_hash, other_hash)` instead of writing a
mirrored second row. Force-selecting an insert instead would hand out
up to 12 free, eviction-immune `contradicted` pins per envelope on a
channel with no message rate limit.

**Decision 29 — the per-position conflict cap is withdrawn: as built it
let the accused bury the branch that accuses it.**

1b-i implemented `MAX_CONFLICTS_PER_POSITION = 8` per `(node_id, seq)`,
refusing further branches rather than deleting any, and counting the
refusals. Refusing rather than deleting was right. Keying the cap on
the position alone was not: the subject is the only party who can mint
validly signed branches at its own position, so it fills all 8 slots
with self-signed junk and the genuine contradicting head — relayed by
a different peer — is refused and persisted nowhere. Not in
`audit_head_conflicts`, because it was refused; not in
`audit_peer_heads`, because the position is held. The returned
`conflict_id` then points at a junk pair, so 5.13c-3's acknowledgement
would bind the wrong evidence.

That is Decision 12 ("a contradicted pin is the evidence") defeated
through a path an attacker triggers at will, and `main` does not have
it, so it is a regression and it comes out. The pin is still marked
`contradicted` and the 8 junk rows still prove the subject
double-signed, so detection survives — but the specific branch pair,
and the head of the chain the subject is actually running, do not.

1b-iii may bound the table again, either way:

- partition the cap by provenance, `(node_id, seq, reported_by)`, so
  heads the subject pushes itself can never occupy the slot a
  third-party report needs; or
- keep one global cap but persist the refused branch's `other_hash`
  and `other_sig` in the overflow row, so no signature is ever
  discarded.

Whichever lands, the metric must be honest about what it counts. The
withdrawn version documented `suppressed` as the number of DISTINCT
refused branches while incrementing it once per occurrence, so a
replayed branch inflated it without bound. Deduplicating by hash needs
unbounded per-position hash storage, which is the very thing the cap
exists to prevent, so the honest resolution is to name it an
occurrence count.

**Decision 30 — a seq the store cannot hold is rejected, not allowed to
reach the INSERT.**

`audit_peer_heads` and `audit_head_conflicts` have both carried
`CHECK (seq > 0)` since V0010, and `ingest` filters heads on dedupe,
own-chain, keyability and signature — never on `seq`. `pin_one`
guarded only the upper end of the range (`i64::try_from`). So a head
at seq 0 reached the INSERT, raised a constraint violation, and
returned `Err`.

Per head that cost one head. Batching the envelope into ONE
transaction — which is the main thing 1b-i does — turns it into the
loss of the whole envelope: the transaction is abandoned and every
honest head beside it is rolled back, including heads for subjects the
sender is the only path to. Any paired peer can mint a self-signed
seq-0 head for its own node_id and repeat it every window.

Rejected at both ends of the range for the same reason: the store
cannot represent it, and clamping would forge a seq its signer never
signed (Decision 11's `RejectedSeq`, now covering 0 as well as
`> i64::MAX`). `p10` pins it, and deleting the guard reproduces
`CHECK constraint failed: seq > 0` through the whole batch.

**Decision 31 — `MAX_HEADS_PER_ENVELOPE` 64 -> 24, and this one does
not wait for the allowance.**

An honest sender emits `1 + RELAY_HEADS` = 17 heads, so 64 left 47
heads of pure attacker headroom in a receive-side `take()` that no
signature covers. Each of those heads costs the receiver an ed25519
verify, and `ingest` performs that verify BEFORE anything downstream
could refuse the head — so the cap is the only bound on per-envelope
crypto, and no per-peer budget could ever be one. It is also no longer
equal to `PIN_KEEP_RECENT` (64), so one envelope can no longer fill a
node's whole recent-pin window; `m24` keeps its original pressure by
sending as many full envelopes as that now takes.

`m24` needed a second correction beyond the envelope count, and it is
worth recording because the first version of this paragraph claimed a
guard the test did not have. The sweep exempts `MIN(rowid)` per node
AND `MIN(rowid)` per time bucket, and both exemptions are status-blind.
`m24` ingested its corroborated pin first, making it `rowid` 1, so it
was protected twice over by rules that never look at `status` —
deleting the sweep's `status = 'unchecked'` filter outright, which is
the round-7 defect in full, left `m24` passing. It now seeds an older
`unchecked` pin ahead of the corroborated one so that both exemptions
are taken by that row, and the corroborated pin survives only because
of its status. Verified in both directions.

`harness-daemon` const-asserts `MAX_HEADS_PER_ENVELOPE > RELAY_HEADS`,
since a cap below what an honest sender emits would make the same
`take()` silently truncate a legitimate relay set. `harness-core`
cannot assert it: `RELAY_HEADS` lives in the daemon. `m27` pins the
truncation, and the truncation is now logged — the one case where the
narrowing discards something must not look like a quiet peer.

**Decision 32 — the batching's cost is tail latency, not throughput,
and it is not a free win.**

Worth recording next to "one lock acquisition and one transaction":
fairness granularity on the process-wide store mutex moves from
per-head to per-envelope. Measured on a file-backed WAL store with 24
heads, total work roughly halves — 2.17 ms of short holds becomes
1.09 ms as one hold — but the worst-case wait for an unrelated store
caller (task dispatch, leases, result writes) rises from about 0.16 ms
to about 1.09 ms. It is bounded by the cap, and the cap fell 64 -> 24
in the same item, so it is better than `main`'s worst case in
aggregate. Rollback is `DropBehavior::Rollback` on the early `?`, which
is why closing the seq-0 path (Decision 30) mattered: before it, one
unstorable head discarded the whole envelope's writes.

### Carried out of 1b-i

- The allowance (Decision 28) and the conflict bound (Decision 29).
  Until they land, `audit_head_conflicts` and the pin table grow with
  what peers send, bounded only by the hourly sweep for `unchecked`
  pins and not at all for `contradicted` ones.
- A fork logs an `ERROR` per occurrence with no per-peer rate limit.
  Bounded fork logging was built for 1b-i and withdrawn with the rest
  of the rate-limiting machinery; it belongs with 1b-ii.
- The oversized-envelope line logs at `debug` for the same reason. The
  const assert keeps the cap above what an honest sender emits, so no
  honest peer can reach that branch without a code change: every
  overflow arriving today is misbehaviour, at a rate the sender picks,
  on a channel with no message rate limit. A per-envelope `WARN` there
  would have been a log-flood vector handed to the sender. 1b-ii
  promotes it to `warn` under the per-peer log window. The const
  assert is per-binary, though, and this crate's cap deliberately
  leaves headroom for `RELAY_HEADS` to grow without a lockstep change:
  so in a MIXED-VERSION mesh an upgraded sender whose
  `1 + RELAY_HEADS` exceeds an older node's cap has the excess
  truncated silently at `debug`, which is the one honest case the
  original `warn` was justified by. Promoting it belongs with the log
  window rather than before it.
- The `debug_assert_eq!` on the `zip` pairing in `ingest` is a
  placeholder, not a guard: it is compiled out in release and checks
  only lengths, not order, so a skipped outcome plus a filler would
  pass it while mis-attributing every later fork. The real coverage is
  `p10` and `m28`, which index outcomes positionally. 1b-ii adds the
  `continue` this protects against and should either promote it to a
  plain `assert_eq!` (the invariant is internal, so a panic is not an
  attacker-reachable DoS) or have `pin_peer_heads` return
  `(head, outcome)` pairs so the pairing cannot drift at all.
- Earlier drafts of this amendment stated the residual row growth as
  `allowance x sweep interval`. That was wrong on its own terms: each
  admitted position also carried up to 8 conflict rows and an overflow
  row, none of which any sweep touches. The corrected figure belongs
  with whichever item bounds those tables.
- `harness.audit` still has no message rate limit, so per-message work
  is bounded only by the envelope cap. Charging cannot substitute,
  because the cost is upstream of any charge.

## Amendment — 5.13c-1b-ii PR A: rating a peer-provoked log line

PR A carries the mechanical half of 1b-ii: paired `(head, outcome)`
returns, a per-peer window on the log lines a PEER can provoke, the
oversized-envelope line promoted back to `warn` under that window, and
`HEAD_PUSH_INTERVAL_PROD` split out so no budget is ever derived from
the `cfg(test)` 200ms interval. No allowance, no ordering decision.

**Decision 33 — a rated line must leave a record of its own size, and
the limiter must fail OPEN on a bad clock.**

Three things went wrong in the first cut of this, all found by review
and all now mutation-covered:

1. **The guard had no test, only a counter.** `m29` asserted
   `fork_logs_in_window`, not the lines. A refactor that keeps the
   charge and emits unconditionally therefore restored the whole flood
   lever with all 141 daemon tests green — the repo's own named
   failure mode ("a guard with no test is a guard that disappears"),
   reproduced in the file that quotes it. The tests now count real
   `tracing` events. Two mechanics matter: the subscriber must be the
   GLOBAL default, because `rebuild_interest_cache` re-registers
   against the global dispatcher and a thread-local one leaves the
   callsite cached as disabled (this presented as `m29` counting 0 in
   parallel while passing alone); and the count is a thread-local, so
   parallel tests emitting the same line cannot inflate it.
2. **Dropping a line silently is not rating it.** `if may_log { log }`
   leaves an operator unable to tell 8 forks from 8000, and the
   residual attack is concrete: a peer burns the budget on self-forks
   at its own position, and the one fork it later relays about a third
   party is dropped with nothing recording that it was. Decision 18's
   `shell.denied` limiter had already settled this — the counts are
   written first and can never be crowded out. So `may_log` returns a
   `LogDecision`, not a `bool`: the line that spends the budget is
   marked `suppressing`, and the drop count rides the next line that
   gets through. It is cleared by being REPORTED, never by time
   passing, so it survives any number of window rolls.
3. **`saturating_sub` prevents the underflow, not the freeze.** If a
   peer's first envelopes arrive while the clock reads ahead — a
   batteryless RTC before NTP settles, which is exactly when the mesh
   first connects — the window start is stamped in the future, every
   later subtraction saturates to 0, and the counters never reset.
   That silences `AUDIT CHAIN FORK` for that peer for the life of the
   process. The asymmetry the shared pattern hides is the point:
   `may_serve` has the same shape and fails OPEN (serves more), while
   this one fails CLOSED, on the loudest alarm the subsystem has. An
   explicit `now_ms < start` check rolls the window instead.

The oversize line's own rating needed both halves covered separately:
deleting the gate AND deleting the counter's reset left the whole
workspace green, and the reset is the sharper half — without it the
`warn` fires once per peer per PROCESS, so an upgraded sender whose
`1 + RELAY_HEADS` exceeds an older node's cap (Decision 31's one
honest case) looks exactly like the quiet peer that case exists to
distinguish. A counter-only assertion cannot see it, because a stuck
counter and a correctly rolled one read the same; `m27` counts the
line across the roll.

Carried to PR B: key the fork budget on `(reported_by, subject)`
rather than `reported_by` alone. That removes the cross-subject
residual in (2) entirely, and it is NOT the subject-keying mistake
Decision 18 records for `shell.denied` — there the subject was an
attacker-minted command string with a fresh key per attempt, whereas
here it is a `NodeId` that must already be in the trust store, so the
key space is bounded by trusted peers and cannot be minted.
