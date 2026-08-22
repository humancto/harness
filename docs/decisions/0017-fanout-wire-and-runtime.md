# ADR-0017 — 3.3-fanout wire protocol + dispatch runtime

**Status:** Accepted (2026-08-22)
**Context:** Phase 3.3-fanout of HARNESS_PRD_v2.md / ROADMAP.md. Covers PR-A1 (wire +
index plumbing) and PR-A2 (dispatch runtime + CLI); the gossip half (`harness.gossip.state`,
heartbeat `replica_head`, `WS /api/v1/runs/<id>`) is PR-B and will extend this ADR.
**Supersedes:** — (extends ADR-0009's lifecycle-seam contract)

## Named channel streams, one router per connection

Every logical channel gets its own bidi QUIC stream, opened with a tiny header
`[0xC5][u16 BE name-len][name]` followed by the existing `4-byte BE len || CBOR` framing.
**Every stream is named — heartbeats included.** There is no unnamed "stream 0" in the
daemon anymore; the legacy single-stream `Connection::send/recv` API survives only for
transport unit tests.

Why not a tagged-frame enum on one stream: a single stream head-of-line-blocks heartbeats
behind an 8 MiB result frame; per-channel streams isolate loss/teardown to one channel
(a decode/signature/oversize error resets that stream only) and give each channel its own
flow control.

The race this design must kill (review R1): two concurrent `accept_bi` consumers on one
connection nondeterministically steal each other's streams. Rule: **exactly one
accept-router task per connection** (`PeerNet::router_task` → `Connection::accept_channel`)
is the sole `accept_bi` caller; it validates the header and spawns one recv task per
channel. Unknown names, malformed headers, header timeouts (5 s), and duplicate streams for
an already-open channel name are reset without touching the connection (the duplicate rule
also bounds per-peer buffered-stream memory). When a recv loop ends, the registration is
released so the peer can legitimately re-open the channel after an error.

## Aggregate channel names

PRD §13.6 names per-task logical channels (`harness.task.result.<task_id>`). On the wire we
multiplex them onto one aggregate stream per peer pair per direction
(`harness.task.assign|claim|result`), with `task_id` inside the payload. This keeps the
replay design (`&'static str` channel constants a peer can never forge), bounds stream
count, and matches quinn's 16-bidi-stream budget. The per-task names remain the logical
model; Phase 4's per-task streaming (`harness.task.partial.<id>`) can revisit.

## Per-channel frame caps

heartbeat/claim 64 KiB · announce/gossip 256 KiB · assign 1 MiB (a `Task` carries arbitrary
JSON input) · result 8 MiB (`shell.exec` caps stdout+stderr at 1 MiB each; JSON escaping can
~4×). Oversize is refused at send (`FrameTooLarge`, stream still usable) and at recv (stream
torn down, connection unaffected).

## Sequencing + signatures

Each `ChannelStream` owns a send counter starting at 0 and a `last_seen: Option<u64>` replay
slot; a reconnect creates fresh streams on both ends so the counters reset in lockstep
(`seq = 0` accepted exactly once per stream). `TaskAssign`/`TaskClaim`/`TaskResultMsg` are
stamped + signed by the sender task at wire-write time (the seq is a per-stream property).
Signature layering on results: the outer envelope sig authenticates the channel peer, the
inner `FinalResult::sig` the executing node — both verified independently, so a Phase-4
relay cannot forge execution provenance. v1 additionally requires
`assigned_by == task.issued_by == connection peer` on assigns (review R10).

## ConnMap tiebreak (review R5)

Connections are keyed by `NodeId`. On duplicates both endpoints deterministically keep the
connection whose **dialer** is `min(local_id, peer_id)` (dialer identity is a property of
the connection, so both sides agree); among same-dialer duplicates the incumbent wins. The
loser is closed. Self-connections are refused.

## Outbound discipline (review R7)

Recv tasks never write to the wire. Every wire write goes through a bounded (64) per-peer
queue drained by one sender task per connection; send errors evict the cached channel and
retry once; a failed/unqueueable `TaskAssign` reports to
`TaskChannelHandlers::on_assign_send_failed` so the dispatcher resets the lease. Heartbeats
ride the same queues (a full queue for a stalled peer drops that peer's heartbeat tick, not
the broadcast).

## Announce

Each side enqueues its signed `NodeManifest` once per adopted connection. On receipt (sig
verified against the connection pubkey; `node_id`/`pubkey` cross-checked against the
connection identity) the `CapabilityIndex`, `ScopeIndex`, and `Store::upsert_manifest` are
updated; the self manifest is indexed at boot. Runtime manifest *changes* are not
re-announced (the registry is static after boot today); a capability-hash-triggered
re-announce is future work alongside heartbeat `capabilities_hash` checking.

## Wire compatibility

This intentionally breaks wire compat with pre-3.3 daemons (heartbeats moved onto named
streams). Nothing has shipped outside this repo; the CBOR encodings of all existing
envelopes are unchanged (the `heartbeat_wire_v0` / `node_manifest_wire_v0` insta fixtures
still pin them). Recorded here per the CLAUDE.md stopping-condition rule.

## Decisions reserved for PR-A2 (from the plan, review-approved)

- Lease TTL = `max(lease_ms, execution.timeout_ms + 15 s)`; worker-driven `extend` is
  Phase 4.6 (review R2). `RetryPolicy` backoff fields stay unused until 4.6 (R16).
- Issuer accepts results while the lease is `pending` **or** `claimed` (a lost claim must
  not drop a valid result), walking the task row from its current pre-terminal state; the
  issuer-side `Claimed→Running→Done` hops at result time are synthetic (R3).
- Worker ingest is idempotent: a re-delivered assign for a terminal row immediately re-sends
  the stored result under the new lease id; reply connections resolve through the ConnMap at
  send time (R4).
- Undispatchable tasks fail terminally after `constraints.deadline` or a 30 s eligibility
  window via a documented supervisor hop `Submitted → Failed` (R6/R9); max-attempts lease
  exhaustion terminates as `Dispatched → Expired`.
- `expire_and_reset_task` guards its task reset on the lease's `assigned_node` (R12);
  leases are created only after winning the dispatch CAS.
