//! [`ChannelStream`] — one named logical channel on its own bidi QUIC
//! stream (3.3-fanout, ADR-0017).
//!
//! Every stream — heartbeat included — opens with a tiny header:
//! `[0xC5][u16 BE name-len][name bytes]`, then the standard
//! `4-byte BE len || CBOR` framing from [`super::envelope`]. The name must
//! be one of the [`super::channels`] constants; anything else is rejected
//! at accept time and the stream is reset without touching the
//! connection.
//!
//! Direction discipline: the side that *writes* a channel opens it
//! (`Connection::channel`); the reader obtains its handle from the single
//! per-connection accept-router (`Connection::accept_channel`). Replies
//! travel on the replier's own outbound channels, never back down an
//! accepted stream — so in practice each `ChannelStream` is used
//! unidirectionally even though QUIC gives us both halves.
//!
//! Replay protection: each stream carries exactly one channel, so instead
//! of the connection-level [`super::envelope::ReplayTable`] a stream owns
//! a single monotonic `last_seen` slot (`Option<u64>` — a fresh stream
//! accepts `seq = 0` exactly once). Senders stamp outgoing envelopes from
//! [`ChannelStream::next_seq`], which starts at 0 per stream; a reconnect
//! creates new streams on both ends so the counters reset in lockstep.
//!
//! Cancel-safety mirrors [`super::Connection`]: `recv*` is cancel-safe
//! (framer state lives on the stream), `send` is only partially
//! cancel-safe — do not race it against timeouts.

use std::sync::atomic::{AtomicU64, Ordering};

use harness_core::{PublicKey, Signable};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::transport::envelope::{encode_frame_capped, RecvFramer, Sequenced, WireError};
use crate::transport::error::TransportError;

/// Magic first byte of a channel-stream header. Deliberately non-zero:
/// the legacy unnamed framing starts every stream with a 4-byte BE frame
/// length whose first byte is always `0x00` (frames are < 16 MiB), so a
/// mis-routed legacy stream fails fast instead of being misparsed.
pub(crate) const CHANNEL_MAGIC: u8 = 0xC5;

/// Upper bound on the header's name length. All real channel names are
/// < 32 bytes; 64 leaves headroom without letting a peer make us buffer
/// anything meaningful pre-validation.
pub(crate) const MAX_CHANNEL_NAME_BYTES: usize = 64;

/// One named channel on its own bidi stream. Obtained from
/// [`super::Connection::channel`] (outbound) or
/// [`super::Connection::accept_channel`] (inbound).
pub struct ChannelStream {
    name: &'static str,
    peer: PublicKey,
    cap: usize,
    send: Mutex<quinn::SendStream>,
    recv: Mutex<ChannelRecvState>,
    send_seq: AtomicU64,
    last_seen: parking_lot::Mutex<Option<u64>>,
}

struct ChannelRecvState {
    framer: RecvFramer,
    stream: quinn::RecvStream,
}

impl std::fmt::Debug for ChannelStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelStream")
            .field("name", &self.name)
            .field("peer", &self.peer.fingerprint_hex())
            .finish_non_exhaustive()
    }
}

impl ChannelStream {
    pub(crate) fn new(
        name: &'static str,
        peer: PublicKey,
        send: quinn::SendStream,
        recv: quinn::RecvStream,
    ) -> Self {
        let cap = super::channels::frame_cap(name);
        Self {
            name,
            peer,
            cap,
            send: Mutex::new(send),
            recv: Mutex::new(ChannelRecvState {
                framer: RecvFramer::with_cap(cap),
                stream: recv,
            }),
            send_seq: AtomicU64::new(0),
            last_seen: parking_lot::Mutex::new(None),
        }
    }

    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub fn peer_pubkey(&self) -> &PublicKey {
        &self.peer
    }

    /// Next outbound sequence number for this stream (starts at 0).
    /// Senders stamp `msg.seq = stream.next_seq()` **before** signing.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.send_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Encode + frame + send one signed message. Same contract as
    /// `Connection::send`: caller must have signed `msg`; partially
    /// cancel-safe (do not race against timeouts).
    pub async fn send<T: Signable>(&self, msg: &T) -> Result<(), TransportError> {
        debug_assert!(
            msg.sig_field().to_bytes() != [0u8; 64],
            "ChannelStream::send received an unsigned message — caller must sign first"
        );
        let mut payload = Vec::with_capacity(512);
        ciborium::ser::into_writer(msg, &mut payload)?;
        let framed = encode_frame_capped(&payload, self.cap)?;
        let mut guard = self.send.lock().await;
        guard.write_all(&framed).await?;
        Ok(())
    }

    /// Read one frame, decode as `T`, verify the signature against the
    /// connection peer's pubkey.
    pub async fn recv<T: Signable + DeserializeOwned>(&self) -> Result<T, TransportError> {
        let payload = self.recv_one_frame().await?;
        let decoded: T =
            ciborium::de::from_reader(payload.as_slice()).map_err(TransportError::Decode)?;
        decoded.verify_signature(&self.peer).map_err(|e| match e {
            harness_core::ProtocolError::Signature(ve) => TransportError::Signature(ve),
            harness_core::ProtocolError::CborEncode(e) => TransportError::Encode(e),
            harness_core::ProtocolError::CborDecode(e) => TransportError::Decode(e),
            _ => TransportError::Protocol("unknown ProtocolError variant".into()),
        })?;
        Ok(decoded)
    }

    /// Replay-checked recv: rejects any message whose `seq()` is `<=` the
    /// last accepted on this stream.
    pub async fn recv_sequenced<T: Signable + Sequenced + DeserializeOwned>(
        &self,
    ) -> Result<T, TransportError> {
        let msg: T = self.recv().await?;
        let seq = msg.seq();
        let mut last = self.last_seen.lock();
        if let Some(prev) = *last {
            if seq <= prev {
                return Err(WireError::Replay {
                    channel: self.name,
                    got: seq,
                    last_seen: prev,
                }
                .into());
            }
        }
        *last = Some(seq);
        Ok(msg)
    }

    /// Same frame-assembly loop as `Connection::recv_one_frame`, on this
    /// stream's own framer. Cancel-safe (see module docs).
    async fn recv_one_frame(&self) -> Result<Vec<u8>, TransportError> {
        let mut state = self.recv.lock().await;
        let ChannelRecvState { framer, stream } = &mut *state;

        if let Some(frame) = super::connection::try_decode_from_leftover(framer)? {
            return Ok(frame);
        }
        loop {
            let chunk = stream
                .read_chunk(64 * 1024, true)
                .await?
                .ok_or(TransportError::FrameTruncated)?;
            if let Some(frame) = super::connection::try_decode_combined(framer, chunk.bytes)? {
                return Ok(frame);
            }
        }
    }
}

/// Serialize the open-header for `name`.
pub(crate) fn encode_channel_header(name: &'static str) -> Vec<u8> {
    debug_assert!(name.len() <= MAX_CHANNEL_NAME_BYTES);
    // Cast is safe: names are compile-time constants under 64 bytes.
    #[allow(clippy::cast_possible_truncation)]
    let len = name.len() as u16;
    let mut out = Vec::with_capacity(3 + name.len());
    out.push(CHANNEL_MAGIC);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(name.as_bytes());
    out
}

/// Read + validate the open-header from an accepted stream. Returns the
/// interned `channels::*` constant for the name.
pub(crate) async fn read_channel_header(
    stream: &mut quinn::RecvStream,
) -> Result<&'static str, TransportError> {
    let mut prefix = [0u8; 3];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(read_exact_err)?;
    if prefix[0] != CHANNEL_MAGIC {
        return Err(TransportError::Protocol(format!(
            "bad channel-header magic 0x{:02x}",
            prefix[0]
        )));
    }
    let len = usize::from(u16::from_be_bytes([prefix[1], prefix[2]]));
    if len == 0 || len > MAX_CHANNEL_NAME_BYTES {
        return Err(TransportError::Protocol(format!(
            "channel name length {len} out of bounds"
        )));
    }
    let mut name_buf = vec![0u8; len];
    stream
        .read_exact(&mut name_buf)
        .await
        .map_err(read_exact_err)?;
    let name = std::str::from_utf8(&name_buf)
        .map_err(|_| TransportError::Protocol("channel name is not UTF-8".into()))?;
    super::channels::known(name)
        .ok_or_else(|| TransportError::Protocol(format!("unknown channel name {name:?}")))
}

fn read_exact_err(e: quinn::ReadExactError) -> TransportError {
    match e {
        quinn::ReadExactError::FinishedEarly(_) => TransportError::FrameTruncated,
        quinn::ReadExactError::ReadError(r) => TransportError::Read(r),
    }
}
