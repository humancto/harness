# ADR-0019 — 3.3-gossip: replica sync over QUIC + replica_head anti-entropy + WS run stream

**Status:** Accepted (2026-08-22)
**Context:** Phase 3.3-gossip (the "PR-B" half ADR-0017 reserved). Closes STATE.md
Phase-2 carryovers 1 (`harness.gossip.state`), 2 (heartbeat `replica_head`), 5
(`WS /api/v1/runs/<task_id>`), and 3.3-fanout carried risk 6 (dispatch-envelope
wire fixtures).
**Extends:** ADR-0017 (channel design), ADR-0006 (LWW replica map).

## Gossip transport: signed, idempotent, chunked

`GossipService` (harness-daemon) pushes `ReplicaSyncEnvelope`s (harness-core,
unwired since 2.5) over the pre-reserved `harness.gossip.state` channel through
the `PeerNet` outbound queues (`OutboundMsg::Gossip`). Envelopes are signed at
assembly time with the local identity; the recv side verifies the signature
against the connection pubkey and drops envelopes whose `source` is not the
connection peer (a trusted peer must not be able to replay another node's
validly-signed envelope as its own traffic). The channel is deliberately **not**
`Sequenced`: LWW merge (`ReplicatedTaskState::supersedes`) makes re-delivery and
reordering harmless, and a replayed old envelope can never regress state — the
same reasoning as `harness.announce`.

### Chunking bound

The gossip frame cap is 256 KiB (ADR-0017). Snapshots are chunked into
envelopes of at most **350 entries**. The bound is measured, not estimated
(locked by `max_entry_wire_bytes_is_bounded` +
`full_chunk_envelope_fits_frame_cap` + a compile-time `const` assertion in
`gossip.rs`): a worst-case entry — 16-byte task id, 16-byte source, longest
state string, `u64::MAX` timestamp, full 256-byte preview (the store truncates
previews at 256) — encodes to **628 bytes**, not the ~320 an estimate suggests,
because serde encodes `Vec<u8>` as a CBOR *integer array* (2 wire bytes per
byte ≥ 0x18), doubling the preview. 350 × 640 B + ~1 KiB envelope overhead ≈
219 KiB, comfortably under the cap. Each chunk is independently signed, so a
lost chunk loses only its own entries (recovered by anti-entropy). Switching
the preview to `serde_bytes` would halve the worst case but is a wire-format
change to a now-pinned envelope — deferred.

## Two convergence paths

1. **Periodic delta push (every 5 s; 500 ms in test builds).** Per-peer
   watermark = max `at_ms` successfully enqueued to that peer; each tick sends
   only entries with `at_ms > watermark`. A fresh (or reconnected) peer starts
   at watermark 0, so its first push is naturally the full snapshot. The
   watermark only advances when every chunk was enqueued — a partial send
   retries in full next tick (idempotent).
2. **Head-triggered full sync.** `Heartbeat` gained
   `#[serde(default)] replica_head: [u8; 32]` = `Store::replica_head()`
   (blake3 over the canonical sorted snapshot — deterministic across converged
   nodes). On heartbeat receipt the daemon compares the peer's head to its own;
   on mismatch it sends the full chunked snapshot, **rate-limited to one full
   sync per peer per 10 s** (2 s in test builds). The rate-limit stamp is taken
   even if the send fails: heads still differ, so a later heartbeat re-triggers
   — conservative beats a tight send-fail loop.

The delta path alone is not convergent: an entry applied with an `at_ms` older
than the watermark (third-node relay, clock skew, out-of-band apply) is
invisible to it, as are increments lost to a full outbound queue. The head
comparison catches every such divergence because the head hashes the *entire*
snapshot. `g02_stale_timestamp_entry_recovers_via_head_triggered_full_sync`
locks exactly this scenario; `g01` locks the delta path.

An all-zero `replica_head` means "not advertised" (pre-3.3-gossip peer, or no
store) and never triggers a sync — an empty replica map hashes to a non-zero
head, so zero is unambiguous.

## Heartbeat wire-format ADDITION + fixture regeneration

`replica_head` is a serde-`default` addition: old CBOR (no field) decodes into
new nodes (locked by `heartbeat_without_replica_head_still_decodes`); new CBOR
decodes on old nodes only if their serde ignores unknown fields — ciborium's
struct decode does, so mixed-version meshes interoperate during rollout. The
`heartbeat_wire_v0` insta fixture pins the canonical encoding and therefore
**had to be regenerated** — expected and intentional for an addition; this ADR
is the record. The heartbeat size-budget test moved 512 → 576 bytes (the field
costs ~47 encoded bytes on a ~480-byte heartbeat).

Cost note: the head is recomputed per heartbeat tick (2 s) and per received
heartbeat — each recompute serializes the full snapshot. Fine at Phase-3 task
counts; a cached head invalidated on `replica_apply_*` is the obvious
optimization when Phase 6 profiling asks for it.

New wire fixtures (3.3-fanout carried risk 6): `task_assign_wire_v0`,
`task_claim_wire_v0`, `task_result_msg_wire_v0`, `replica_sync_envelope_wire_v0`
now pin the dispatch + gossip CBOR encodings alongside the heartbeat/manifest
fixtures, before mixed-version nodes exist.

## `WS /api/v1/runs/:task_id` — polling honesty

The per-task result stream sends one JSON `{state, output?, error?}` frame per
observed state change and closes (code 1000) after the terminal frame.
**Implementation is a 250 ms server-side store poll, documented as such.** The
API layer has no in-process event bus from the store (transitions are written
by the executor/dispatch runtime straight into SQLite; the worker's
`FinalResult` arrives over QUIC on another node entirely). The honest v1 is one
poller per open socket — bounded by connection count, pushes only on change,
same freshness class as the CLI's existing 250 ms poll of `GET /tasks/:id`. A
store notification bus can replace the poll without changing the wire shape.
Auth/origin discipline mirrors `WS /api/v1/events` (loopback-only `Origin`
when present; header-less non-browser clients allowed). The DTO is mirrored as
`RunStreamEvent` in `ui/src/lib/types.ts`.

## Drive-by fix: axum 0.7 path params

`/api/v1/tasks/{id}` used axum-0.8 brace syntax; axum 0.7 / matchit 0.7 treats
that as a **literal** segment, so `GET /api/v1/tasks/<uuid>` always fell
through to the 404 fallback (latent — no HTTP-level test covered it; the CLI's
poll loop was the victim). Fixed to `/tasks/:id` alongside the new `/runs/:id`
route; `get_task_by_id_over_http_resolves` is the regression test.

## Decisions deliberately deferred

- Gossip fan-out is full-mesh (every live peer) — fine at LAN scale; epidemic
  peer sampling is a Phase 6 concern.
- Watermark equality ties (`at_ms == watermark`, different source) can be
  missed by the delta path; the head sync recovers them. Not worth a
  lexicographic watermark today.
- Reconnect resets the per-peer gossip state → full re-push (correct, if
  wasteful).
- Announce-failure → connection close (3.3-fanout carried risk 4) was NOT
  bundled here; it stays open.
