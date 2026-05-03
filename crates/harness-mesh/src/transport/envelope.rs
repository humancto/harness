//! Wire framing, the [`Sequenced`] trait, and the per-connection replay
//! table.
//!
//! Three load-bearing pieces live here:
//!
//! 1. **[`RecvFramer`]** — a cancel-safe byte-state machine that turns a
//!    stream of incoming chunks into discrete `[u8; 4] BE length` + payload
//!    frames. The framer holds *all* the state; the I/O loop in
//!    `connection::recv` just feeds it `Bytes` returned by `quinn::RecvStream::read_chunk`.
//!    Because the state is not in the future, dropping a `recv` mid-poll
//!    preserves the partial frame for the next call.
//! 2. **[`Sequenced`]** — extends [`harness_core::Signable`] with a `seq()`
//!    accessor. Only message types whose semantics demand replay protection
//!    opt in; [`harness_core::Heartbeat`] does, [`harness_core::NodeManifest`]
//!    does not (gossip-on-change is idempotent).
//! 3. **[`ReplayTable`]** — per-channel `last_seen_seq` storage. Strict
//!    `<=` rejection. Backed by `DashMap<&'static str, Option<u64>>` so
//!    `seq=0` on a freshly-opened channel is correctly accepted exactly
//!    once (a naive `last==0` sentinel would let it replay forever) and
//!    concurrent `recv_sequenced` calls on different channels don't
//!    block each other.
//!
//! Channel names are `&'static str` constants in [`channels`]; we never
//! accept a runtime-sourced channel name from a peer, so the `'static`
//! bound is correct and prevents wire-controlled channel-name forgery.

use bytes::Bytes;
use dashmap::DashMap;
use harness_core::{Heartbeat, Signable};

/// Maximum wire-frame size, including the 4-byte header? **No — payload
/// only.** A frame is `4 BE bytes` length + N bytes payload, where
/// `N <= MAX_FRAME_BYTES`.
///
/// 64 KiB is comfortable headroom for every Phase 1 message type:
/// - Heartbeat ~480 B (PRD §13.1).
/// - `NodeManifest` scales with capability count; even with a hundred
///   capabilities + `serde_json` schemas it stays well under 64 KiB.
/// - Phase 2.x's `Task` envelopes carrying `serde_json::Value` could push
///   past this — bumped then with a corresponding test that locks the
///   exact ceiling.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;

/// Wire-protocol logical channels (PRD §13.6). `&'static str` so a peer
/// can never inject a forged channel name from the wire.
pub mod channels {
    /// Heartbeats from a node. Replay-protected via `Heartbeat::seq`.
    pub const HEARTBEAT: &str = "harness.heartbeat";

    /// New-node manifest announcements. Not replay-protected (manifest is
    /// gossiped on change, idempotent).
    pub const ANNOUNCE: &str = "harness.announce";
}

/// Wire-protocol-layer errors that the framer / replay table surface.
/// `connection::TransportError` (commit 4) absorbs these via `From` arms.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(dead_code)]
pub(crate) enum WireError {
    #[error("frame too large: {len} > {max}")]
    FrameTooLarge { len: usize, max: usize },
    #[error("replay: seq {got} <= last_seen {last_seen} on channel {channel}")]
    Replay {
        channel: &'static str,
        got: u64,
        last_seen: u64,
    },
}

/// Trait implemented by wire types that carry a monotonically-increasing
/// sequence number for replay protection.
///
/// Heartbeats opt in via the `seq` field; future Phase 2 types (`Task`,
/// `Result`) will follow when they grow seq fields. `NodeManifest` does
/// **not** implement this — its semantics are idempotent gossip.
pub trait Sequenced: Signable {
    fn seq(&self) -> u64;
}

impl Sequenced for Heartbeat {
    fn seq(&self) -> u64 {
        self.seq
    }
}

// -----------------------------------------------------------------------------
// RecvFramer
// -----------------------------------------------------------------------------

/// Per-stream byte-state machine. Cancel-safe by construction: all state
/// lives on the framer, none in the futures that drive it.
///
/// `leftover` holds residual bytes from the previous `read_chunk` when the
/// chunk straddled a frame boundary. Without it, two back-to-back frames
/// coalesced into one quinn STREAM frame would silently lose the second.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct RecvFramer {
    state: FramerState,
    leftover: Bytes,
}

#[derive(Debug)]
enum FramerState {
    NeedHeader {
        buf: [u8; 4],
        filled: usize,
    },
    NeedPayload {
        len: usize,
        buf: Vec<u8>,
        filled: usize,
    },
}

#[allow(dead_code)]
impl RecvFramer {
    pub(crate) fn new() -> Self {
        Self {
            state: FramerState::NeedHeader {
                buf: [0u8; 4],
                filled: 0,
            },
            leftover: Bytes::new(),
        }
    }

    /// Drain any bytes that were consumed by a prior chunk but not yet
    /// fed into the framer (e.g., the tail of a chunk that contained
    /// frame N's last byte plus the start of frame N+1).
    pub(crate) fn take_leftover(&mut self) -> Bytes {
        std::mem::take(&mut self.leftover)
    }

    /// Stash residual bytes for the next call. Caller is the I/O loop in
    /// `Connection::recv_one_frame`.
    pub(crate) fn put_leftover(&mut self, b: Bytes) {
        debug_assert!(self.leftover.is_empty(), "double put_leftover");
        self.leftover = b;
    }

    /// Pure byte-pump. Feed `input` (possibly a partial chunk from
    /// `quinn::RecvStream::read_chunk`); returns `Ok(Some(frame))` when a
    /// full frame is assembled. The framer consumes from `input` only what
    /// it needs; remainder stays in the caller's `Bytes` for the next call.
    ///
    /// `Ok(None)` means "need more bytes; call me again with the next chunk."
    pub(crate) fn try_decode(&mut self, input: &mut Bytes) -> Result<Option<Vec<u8>>, WireError> {
        loop {
            match &mut self.state {
                FramerState::NeedHeader { buf, filled } => {
                    let want = 4 - *filled;
                    if input.is_empty() {
                        return Ok(None);
                    }
                    let take = want.min(input.len());
                    buf[*filled..*filled + take].copy_from_slice(&input[..take]);
                    *filled += take;
                    *input = input.slice(take..);
                    if *filled == 4 {
                        let len = u32::from_be_bytes(*buf) as usize;
                        if len > MAX_FRAME_BYTES {
                            return Err(WireError::FrameTooLarge {
                                len,
                                max: MAX_FRAME_BYTES,
                            });
                        }
                        self.state = FramerState::NeedPayload {
                            len,
                            buf: Vec::with_capacity(len),
                            filled: 0,
                        };
                        // Continue loop — payload bytes may already be in `input`.
                    }
                }
                FramerState::NeedPayload { len, buf, filled } => {
                    if *len == 0 {
                        // Zero-length frame is valid (empty payload).
                        let out = std::mem::take(buf);
                        self.state = FramerState::NeedHeader {
                            buf: [0u8; 4],
                            filled: 0,
                        };
                        return Ok(Some(out));
                    }
                    let want = *len - *filled;
                    if input.is_empty() {
                        return Ok(None);
                    }
                    let take = want.min(input.len());
                    buf.extend_from_slice(&input[..take]);
                    *filled += take;
                    *input = input.slice(take..);
                    if *filled == *len {
                        let out = std::mem::take(buf);
                        self.state = FramerState::NeedHeader {
                            buf: [0u8; 4],
                            filled: 0,
                        };
                        return Ok(Some(out));
                    }
                }
            }
        }
    }

    /// True if the framer is mid-frame (has consumed bytes that don't yet
    /// constitute a complete frame). Useful for diagnostics + EOF handling
    /// in `connection::recv`.
    pub(crate) fn is_partial(&self) -> bool {
        match &self.state {
            FramerState::NeedHeader { filled, .. } => *filled > 0,
            FramerState::NeedPayload { .. } => true,
        }
    }
}

impl Default for RecvFramer {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode `payload` as a single wire frame: 4-byte BE length + payload.
/// Errors only if `payload.len() > MAX_FRAME_BYTES`.
#[allow(dead_code)]
pub(crate) fn encode_frame(payload: &[u8]) -> Result<Vec<u8>, WireError> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(WireError::FrameTooLarge {
            len: payload.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    // Length fits in u32 because we just checked it's <= MAX_FRAME_BYTES = 64 KiB.
    #[allow(clippy::cast_possible_truncation)]
    let len = payload.len() as u32;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

// -----------------------------------------------------------------------------
// ReplayTable
// -----------------------------------------------------------------------------

/// Per-channel last-seen-seq tracker. Used by `Connection::recv_sequenced`.
///
/// Storage is `Option<u64>` (not bare `u64` with sentinel) so the very
/// first `seq=0` is correctly accepted exactly once and a second
/// `seq=0` is rejected. A naive `last == 0` sentinel would let `seq=0`
/// replay indefinitely on a freshly-opened channel — a real protocol
/// hole even if today's `Heartbeat` happens to start its counter at 1.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct ReplayTable {
    last_seen: DashMap<&'static str, Option<u64>>,
}

#[allow(dead_code)]
impl ReplayTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Strict `<=` rejection: re-receiving the same seq is a replay. On
    /// success, updates the table to `Some(seq)` and returns Ok.
    pub(crate) fn check(&self, channel: &'static str, seq: u64) -> Result<(), WireError> {
        // DashMap's entry API serializes the read-modify-write, no race.
        let mut entry = self.last_seen.entry(channel).or_insert(None);
        if let Some(last) = *entry {
            if seq <= last {
                return Err(WireError::Replay {
                    channel,
                    got: seq,
                    last_seen: last,
                });
            }
        }
        *entry = Some(seq);
        Ok(())
    }

    /// Peek without mutating. Diagnostics only.
    pub(crate) fn last_seen(&self, channel: &'static str) -> Option<u64> {
        self.last_seen.get(channel).and_then(|r| *r)
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Identity, NodeId, SemVer, Signature, TaskId};

    fn sample_heartbeat(seq: u64) -> Heartbeat {
        Heartbeat {
            node_id: NodeId::from_bytes([0x01; 16]),
            seq,
            timestamp: 1_700_000_000_000,
            queue_depth: 0,
            cpu_busy_pct: 0,
            cpu_pinned_count: 0,
            ram_used_mb: 0,
            ram_total_mb: 0,
            gpu_used_mb: 0,
            gpu_total_mb: 0,
            capabilities_hash: [0u8; 16],
            in_flight: vec![TaskId(uuid::Uuid::nil())],
            leader_belief: NodeId::from_bytes([0x01; 16]),
            brain_score: 0,
            on_battery: false,
            paused: false,
            version: SemVer::new(0, 1, 0),
            sig: Signature::from_bytes([0u8; 64]),
        }
    }

    // --- framer ---

    #[test]
    fn frame_round_trip_small() {
        let payload = b"hello world".to_vec();
        let frame = encode_frame(&payload).expect("encode");
        let mut input = Bytes::from(frame);
        let mut f = RecvFramer::new();
        let decoded = f.try_decode(&mut input).expect("decode").expect("frame");
        assert_eq!(decoded, payload);
        assert!(input.is_empty());
        assert!(!f.is_partial());
    }

    #[test]
    fn frame_round_trip_max_size() {
        let payload = vec![0xAB; MAX_FRAME_BYTES];
        let frame = encode_frame(&payload).expect("encode");
        let mut input = Bytes::from(frame);
        let mut f = RecvFramer::new();
        let decoded = f.try_decode(&mut input).expect("decode").expect("frame");
        assert_eq!(decoded.len(), MAX_FRAME_BYTES);
    }

    #[test]
    fn encode_frame_too_large_rejected() {
        let payload = vec![0u8; MAX_FRAME_BYTES + 1];
        let err = encode_frame(&payload).expect_err("too large");
        match err {
            WireError::FrameTooLarge { len, max } => {
                assert_eq!(len, MAX_FRAME_BYTES + 1);
                assert_eq!(max, MAX_FRAME_BYTES);
            }
            other => panic!("expected FrameTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn decode_frame_too_large_rejected() {
        // Hand-build a frame header claiming 2 MiB.
        let len = MAX_FRAME_BYTES + 1024;
        let mut header = Vec::new();
        header.extend_from_slice(&u32::try_from(len).unwrap().to_be_bytes());
        let mut input = Bytes::from(header);
        let mut f = RecvFramer::new();
        let err = f.try_decode(&mut input).expect_err("too large");
        assert!(matches!(err, WireError::FrameTooLarge { .. }));
    }

    #[test]
    fn frame_resumes_across_chunks() {
        // Split a single frame into many tiny chunks and feed them one at
        // a time. The framer must produce the same payload as a single-shot
        // decode.
        let payload = vec![0xCD; 5000];
        let frame = encode_frame(&payload).expect("encode");
        let mut f = RecvFramer::new();
        let mut output = None;
        for chunk in frame.chunks(7) {
            let mut input = Bytes::copy_from_slice(chunk);
            if let Some(p) = f.try_decode(&mut input).expect("decode") {
                output = Some(p);
                assert!(input.is_empty(), "no leftover after final chunk");
            } else {
                assert!(input.is_empty(), "framer must consume the chunk");
            }
        }
        assert_eq!(output.expect("eventual frame"), payload);
        assert!(!f.is_partial());
    }

    #[test]
    fn frame_resumes_across_two_frames() {
        // Two back-to-back frames in the same buffer.
        let p1 = b"first".to_vec();
        let p2 = b"second-payload".to_vec();
        let mut concat = encode_frame(&p1).expect("encode 1");
        concat.extend_from_slice(&encode_frame(&p2).expect("encode 2"));
        let mut input = Bytes::from(concat);
        let mut f = RecvFramer::new();
        let d1 = f.try_decode(&mut input).expect("decode 1").expect("first");
        assert_eq!(d1, p1);
        let d2 = f.try_decode(&mut input).expect("decode 2").expect("second");
        assert_eq!(d2, p2);
        assert!(input.is_empty());
    }

    /// Regression test for the `read_chunk`-boundary residue bug.
    /// Earlier `recv_one_frame` only `debug_assert!`-ed that `bytes` was
    /// empty after extracting one frame; in release the residue was
    /// silently dropped, losing the head of the next frame. Now the
    /// framer stashes leftover via `put_leftover` / `take_leftover`,
    /// which the I/O loop in `connection.rs` honors.
    #[test]
    fn framer_leftover_round_trips_two_concatenated_frames() {
        let p1 = b"frame1".to_vec();
        let p2 = b"frame2-bigger".to_vec();
        let mut concat = encode_frame(&p1).expect("encode 1");
        concat.extend_from_slice(&encode_frame(&p2).expect("encode 2"));
        let combined = Bytes::from(concat);

        // Simulate the connection.rs loop: feed all bytes once, get
        // frame 1, observe leftover; feed nothing the second time but
        // drain leftover, get frame 2.
        let mut f = RecvFramer::new();
        let mut input = combined;
        let frame_a = f
            .try_decode(&mut input)
            .expect("decode 1")
            .expect("frame 1");
        assert_eq!(frame_a, p1);
        // What recv_one_frame would do: stash residue.
        if !input.is_empty() {
            f.put_leftover(input);
        }
        // Next frame: take leftover, decode, no read_chunk needed.
        let mut leftover = f.take_leftover();
        assert!(!leftover.is_empty(), "leftover must carry frame 2's bytes");
        let frame_b = f
            .try_decode(&mut leftover)
            .expect("decode 2")
            .expect("frame 2");
        assert_eq!(frame_b, p2);
    }

    #[test]
    fn frame_partial_is_observable() {
        let payload = b"partial".to_vec();
        let frame = encode_frame(&payload).expect("encode");
        let mut input = Bytes::copy_from_slice(&frame[..3]);
        let mut f = RecvFramer::new();
        assert!(f.try_decode(&mut input).expect("decode").is_none());
        assert!(f.is_partial(), "framer should report mid-frame state");
    }

    #[test]
    fn empty_payload_frame_round_trip() {
        let frame = encode_frame(&[]).expect("encode");
        let mut input = Bytes::from(frame);
        let mut f = RecvFramer::new();
        let out = f.try_decode(&mut input).expect("decode").expect("frame");
        assert!(out.is_empty());
        assert!(!f.is_partial());
    }

    // --- replay ---

    #[test]
    fn replay_strict_inequality() {
        let t = ReplayTable::new();
        t.check(channels::HEARTBEAT, 1).expect("first");
        let err = t
            .check(channels::HEARTBEAT, 1)
            .expect_err("same seq must replay");
        assert!(matches!(err, WireError::Replay { got: 1, .. }));
    }

    /// Regression test for the `seq=0` bypass — earlier code used a
    /// `last == 0` sentinel, so the first `seq=0` was accepted but the
    /// table stayed at `0`, letting subsequent `seq=0` slip through
    /// unboundedly. `Option<u64>` storage fixes this.
    #[test]
    fn replay_zero_seq_first_accepted_then_rejected() {
        let t = ReplayTable::new();
        t.check(channels::HEARTBEAT, 0)
            .expect("first 0 must accept");
        let err = t
            .check(channels::HEARTBEAT, 0)
            .expect_err("second 0 must replay");
        assert!(matches!(
            err,
            WireError::Replay {
                got: 0,
                last_seen: 0,
                ..
            }
        ));
    }

    #[test]
    fn replay_old_seq_rejected() {
        let t = ReplayTable::new();
        t.check(channels::HEARTBEAT, 5).expect("first");
        let err = t
            .check(channels::HEARTBEAT, 3)
            .expect_err("older seq must replay");
        assert!(matches!(
            err,
            WireError::Replay {
                got: 3,
                last_seen: 5,
                ..
            }
        ));
    }

    #[test]
    fn replay_independent_per_channel() {
        let t = ReplayTable::new();
        t.check(channels::HEARTBEAT, 5).expect("hb 5");
        // Same seq on a different channel must succeed.
        t.check(channels::ANNOUNCE, 5).expect("announce 5");
    }

    #[test]
    fn replay_table_advances_on_success() {
        let t = ReplayTable::new();
        t.check(channels::HEARTBEAT, 1).expect("1");
        t.check(channels::HEARTBEAT, 2).expect("2");
        t.check(channels::HEARTBEAT, 100).expect("100");
        assert_eq!(t.last_seen(channels::HEARTBEAT), Some(100));
    }

    // --- Sequenced ---

    #[test]
    fn sequenced_impl_for_heartbeat_returns_seq_field() {
        let hb = sample_heartbeat(42);
        assert_eq!(<Heartbeat as Sequenced>::seq(&hb), 42);
    }

    #[test]
    fn sequenced_signed_heartbeat_roundtrips() {
        let id = Identity::generate();
        let mut hb = sample_heartbeat(7);
        hb.sign(&id).expect("sign");
        // Signature doesn't affect Sequenced::seq().
        assert_eq!(hb.seq(), 7);
        // And the sig still verifies after we read seq.
        hb.verify_signature(id.public_key()).expect("verify");
    }
}
