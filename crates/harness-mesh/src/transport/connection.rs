//! [`Connection`] — one persistent QUIC connection plus its single bidi
//! application stream.
//!
//! Cancel-safety contract (plan §10):
//! - `recv` / `recv_sequenced` are **cancel-safe** because the framer state
//!   lives on `Connection` (under `tokio::sync::Mutex<RecvFramer>`), not in
//!   the future. A dropped poll preserves the partial frame for the next
//!   call. We use `quinn::RecvStream::read_chunk`, which is the cancel-safe
//!   primitive per quinn's docs.
//! - `send` is **partially cancel-safe**. `quinn::SendStream::write_all`
//!   may have flushed bytes to the wire; cancellation mid-write can
//!   corrupt the framing on the stream. Documented; do not `select!` `send`
//!   against arbitrary timeouts.
//! - `close` is cancel-safe.

use std::net::SocketAddr;

use bytes::Bytes;
use harness_core::{PublicKey, Signable};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::transport::envelope::{encode_frame, RecvFramer, ReplayTable, Sequenced};
use crate::transport::error::TransportError;

/// A QUIC handshake has succeeded; the peer pubkey is known. The application
/// has not yet decided whether to keep the connection — it does so by
/// calling [`IncomingConnection::accept`].
pub struct IncomingConnection {
    pubkey: PublicKey,
    inner: quinn::Connection,
}

impl std::fmt::Debug for IncomingConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingConnection")
            .field("peer", &self.pubkey.fingerprint_hex())
            .field("remote", &self.remote_addr())
            .finish_non_exhaustive()
    }
}

impl IncomingConnection {
    pub(crate) fn from_quinn(connection: quinn::Connection, pubkey: PublicKey) -> Self {
        Self {
            pubkey,
            inner: connection,
        }
    }

    #[must_use]
    pub fn peer_pubkey(&self) -> &PublicKey {
        &self.pubkey
    }

    #[must_use]
    pub fn remote_addr(&self) -> SocketAddr {
        self.inner.remote_address()
    }

    /// Approve. Caller's `allow_pubkey` closure is the trust check (typically
    /// a `TrustStore::contains`); on `false` the connection is closed and
    /// `Err(TransportError::CertMismatch)` is returned.
    ///
    /// **Returns immediately on approval** — the bidi stream is accepted
    /// lazily on first `recv` (and opened lazily on first `send`). Eager
    /// stream open/accept here would deadlock with the dialer pattern,
    /// since `accept_bi` blocks until the peer writes its first byte but
    /// the peer cannot write until this method returns.
    pub fn accept(
        self,
        allow_pubkey: impl FnOnce(&PublicKey) -> bool,
    ) -> Result<Connection, TransportError> {
        if !allow_pubkey(&self.pubkey) {
            self.inner.close(1u32.into(), b"untrusted-peer");
            return Err(TransportError::CertMismatch);
        }
        Ok(Connection::accepter(self.inner, self.pubkey))
    }
}

impl TransportError {
    /// Helper for the accept-bi path where we get a bare `ConnectionError`
    /// without an address.
    pub(crate) fn from_quinn_connection_error(source: quinn::ConnectionError) -> Self {
        Self::Connect {
            addr: SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), 0),
            source,
        }
    }
}

/// Which side of the connection we are. Determines whether `send`'s
/// first call opens a bidi stream (`Dialer`) or accepts one (`Accepter`).
#[derive(Debug, Clone, Copy)]
enum Side {
    Dialer,
    Accepter,
}

/// One peer-to-peer harness connection. Cheap to clone is intentionally
/// **not** offered — this type owns the per-connection mutexes.
///
/// Stream open/accept is lazy. On the dialer, the first `send` calls
/// `quinn::Connection::open_bi`; on the accepter, the first `recv` (or
/// `send`) calls `quinn::Connection::accept_bi`. This avoids the
/// deadlock where eagerly accepting in `IncomingConnection::accept`
/// blocks on the dialer's first write, but the dialer cannot write
/// until the accept call returns.
///
/// **Send and recv use independent mutexes** so a `recv` blocked on
/// `read_chunk` does not gate a concurrent `send` (item 1.5's heartbeat
/// loop has both directions on one `Connection` and would deadlock
/// otherwise).
pub struct Connection {
    pubkey: PublicKey,
    addr: SocketAddr,
    inner: quinn::Connection,
    side: Side,
    /// One-shot init for the bidi-stream pair. Holds quinn's send stream
    /// after init.
    send_stream: Mutex<Option<quinn::SendStream>>,
    /// Framer + recv stream pair. Both protected by the same mutex
    /// because the framer's state and the underlying read are
    /// inseparable in the recv state machine.
    recv_state: Mutex<RecvState>,
    /// Synchronizes lazy stream initialization across send/recv tasks.
    init: tokio::sync::OnceCell<()>,
    replay: ReplayTable,
}

struct RecvState {
    framer: RecvFramer,
    stream: Option<quinn::RecvStream>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("peer", &self.pubkey.fingerprint_hex())
            .field("remote", &self.addr)
            .finish_non_exhaustive()
    }
}

impl Connection {
    fn new(inner: quinn::Connection, pubkey: PublicKey, side: Side) -> Self {
        let addr = inner.remote_address();
        Self {
            pubkey,
            addr,
            inner,
            side,
            send_stream: Mutex::new(None),
            recv_state: Mutex::new(RecvState {
                framer: RecvFramer::new(),
                stream: None,
            }),
            init: tokio::sync::OnceCell::new(),
            replay: ReplayTable::new(),
        }
    }

    pub(crate) fn dialer(connection: quinn::Connection, pubkey: PublicKey) -> Self {
        Self::new(connection, pubkey, Side::Dialer)
    }

    pub(crate) fn accepter(connection: quinn::Connection, pubkey: PublicKey) -> Self {
        Self::new(connection, pubkey, Side::Accepter)
    }

    /// Lazily open or accept the bidi stream pair exactly once, then
    /// install the half-streams into `send_stream` and `recv_state`.
    /// Subsequent calls are O(1) — `OnceCell::get_or_try_init` short-circuits.
    ///
    /// Held mutexes during init are minimal: the `OnceCell`'s internal
    /// semaphore serializes initialization across concurrent send+recv
    /// callers, but the per-half mutexes are NEVER both held at the same
    /// time after init returns.
    async fn ensure_streams(&self) -> Result<(), TransportError> {
        self.init
            .get_or_try_init(|| async {
                let (send, recv) = match self.side {
                    Side::Dialer => self
                        .inner
                        .open_bi()
                        .await
                        .map_err(TransportError::from_quinn_connection_error)?,
                    Side::Accepter => self
                        .inner
                        .accept_bi()
                        .await
                        .map_err(TransportError::from_quinn_connection_error)?,
                };
                *self.send_stream.lock().await = Some(send);
                self.recv_state.lock().await.stream = Some(recv);
                Ok::<(), TransportError>(())
            })
            .await?;
        Ok(())
    }

    #[must_use]
    pub fn peer_pubkey(&self) -> &PublicKey {
        &self.pubkey
    }

    #[must_use]
    pub fn remote_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Encode `msg` as canonical CBOR + length-prefixed wire frame and
    /// send. Caller must have already signed `msg` via
    /// `harness_core::Signable::sign`. Debug builds assert that the sig
    /// field is not all-zero so a forgotten `sign()` fires loudly in tests.
    pub async fn send<T: Signable>(&self, msg: &T) -> Result<(), TransportError> {
        debug_assert!(
            msg.sig_field().to_bytes() != [0u8; 64],
            "Connection::send received an unsigned message — caller must call .sign(&identity) first"
        );
        let mut payload = Vec::with_capacity(INITIAL_SEND_CAPACITY);
        ciborium::ser::into_writer(msg, &mut payload)?;
        let framed = encode_frame(&payload)?;
        self.ensure_streams().await?;
        let mut guard = self.send_stream.lock().await;
        let send = guard.as_mut().ok_or_else(|| {
            TransportError::Protocol("send stream not initialized after ensure".into())
        })?;
        send.write_all(&framed).await?;
        Ok(())
    }

    /// Read one frame, decode as `T`, **verify** `T::verify_signature`
    /// against `self.peer_pubkey()`, return.
    pub async fn recv<T: Signable + DeserializeOwned>(&self) -> Result<T, TransportError> {
        let payload = self.recv_one_frame().await?;
        let decoded: T =
            ciborium::de::from_reader(payload.as_slice()).map_err(TransportError::Decode)?;
        decoded.verify_signature(&self.pubkey).map_err(|e| {
            // Map ProtocolError -> TransportError. ProtocolError carries
            // either Cbor* (already decoded above without erroring, so
            // shouldn't recur) or Signature(VerifyError).
            match e {
                harness_core::ProtocolError::Signature(ve) => TransportError::Signature(ve),
                harness_core::ProtocolError::CborEncode(e) => TransportError::Encode(e),
                harness_core::ProtocolError::CborDecode(e) => TransportError::Decode(e),
                _ => TransportError::Protocol("unknown ProtocolError variant".into()),
            }
        })?;
        Ok(decoded)
    }

    /// Replay-checked recv. Rejects any frame whose `seq()` is `<=` the
    /// last accepted on `channel`. Channels are static strings; see
    /// [`crate::transport::channels`].
    pub async fn recv_sequenced<T: Signable + Sequenced + DeserializeOwned>(
        &self,
        channel: &'static str,
    ) -> Result<T, TransportError> {
        let msg: T = self.recv().await?;
        self.replay.check(channel, msg.seq())?;
        Ok(msg)
    }

    /// Cleanly close the connection.
    pub fn close(self) -> Result<(), TransportError> {
        self.inner.close(0u32.into(), b"close");
        Ok(())
    }

    /// Drive the framer until a complete frame is assembled. Cancel-safe:
    /// the framer + any partially-consumed `Bytes` chunk live on `self`
    /// (under `recv_state: Mutex<RecvState>`). If a `read_chunk` returns
    /// more bytes than the current frame needs, the residue is stashed on
    /// the framer's `leftover` slot and consumed on the next call.
    ///
    /// Cancel-safety invariant: leftover bytes are NEVER off the framer
    /// across an `.await`. Every place we extract leftover, run the
    /// (sync) state machine, and re-stash any remainder is bracketed by
    /// no await points. If the future is dropped at the
    /// `stream.read_chunk(...)` await, the framer still has the
    /// pre-await leftover and the next call resumes from the same byte
    /// position.
    async fn recv_one_frame(&self) -> Result<Vec<u8>, TransportError> {
        self.ensure_streams().await?;
        // Hold the recv-state lock for the entire frame assembly. Two
        // concurrent recv() calls on the same Connection will queue,
        // which is correct — frames are ordered.
        let mut state = self.recv_state.lock().await;
        let RecvState { framer, stream } = &mut *state;
        let stream = stream.as_mut().ok_or_else(|| {
            TransportError::Protocol("recv stream not initialized after ensure".into())
        })?;

        // Step A (sync, no await): try to assemble a frame from any
        // leftover alone. take_leftover + try_decode + put_leftover is
        // an atomic sync block — cancellation can't happen between them.
        if let Some(frame) = try_decode_from_leftover(framer)? {
            return Ok(frame);
        }

        loop {
            // Step B (await): read a chunk. At this point the framer
            // either has zero leftover (if Step A drained it dry) or
            // has the unconsumed tail of a prior chunk (if Step A
            // couldn't assemble a frame and re-stashed the partial).
            // Either way the framer is in a consistent state and a
            // cancellation here loses nothing.
            let chunk = stream
                .read_chunk(64 * 1024, true)
                .await?
                .ok_or(TransportError::FrameTruncated)?;

            // Step C (sync, no await): combine leftover + new chunk,
            // run the state machine, stash any tail back to leftover.
            if let Some(frame) = try_decode_combined(framer, chunk.bytes)? {
                return Ok(frame);
            }
            // Frame still incomplete — leftover already updated by
            // try_decode_combined. Loop and read more.
        }
    }
}

/// Sync helper: drain `framer.leftover` into a working buffer, run the
/// state machine, stash remainder. Returns `Some(frame)` if a complete
/// frame fell out, `None` if leftover wasn't enough.
fn try_decode_from_leftover(framer: &mut RecvFramer) -> Result<Option<Vec<u8>>, TransportError> {
    let mut buf = framer.take_leftover();
    if buf.is_empty() {
        return Ok(None);
    }
    let frame = framer.try_decode(&mut buf)?;
    if !buf.is_empty() {
        framer.put_leftover(buf);
    }
    Ok(frame)
}

/// Sync helper: combine `framer.leftover` (if any) with a freshly-read
/// `chunk`, run the state machine, stash remainder. Returns
/// `Some(frame)` on completion, `None` if more chunks are needed.
fn try_decode_combined(
    framer: &mut RecvFramer,
    chunk: Bytes,
) -> Result<Option<Vec<u8>>, TransportError> {
    let leftover = framer.take_leftover();
    let mut buf = if leftover.is_empty() {
        chunk
    } else {
        let mut combined = bytes::BytesMut::with_capacity(leftover.len() + chunk.len());
        combined.extend_from_slice(&leftover);
        combined.extend_from_slice(&chunk);
        combined.freeze()
    };
    let frame = framer.try_decode(&mut buf)?;
    if !buf.is_empty() {
        framer.put_leftover(buf);
    }
    Ok(frame)
}

const INITIAL_SEND_CAPACITY: usize = 512;
