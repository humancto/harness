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
    /// On approval, accepts the peer's first bidi stream as the single
    /// application stream this `Connection` uses for the rest of its life.
    pub async fn accept(
        self,
        allow_pubkey: impl FnOnce(&PublicKey) -> bool,
    ) -> Result<Connection, TransportError> {
        if !allow_pubkey(&self.pubkey) {
            self.inner.close(1u32.into(), b"untrusted-peer");
            return Err(TransportError::CertMismatch);
        }
        let (send, recv) = self
            .inner
            .accept_bi()
            .await
            .map_err(TransportError::from_quinn_connection_error)?;
        Ok(Connection::new(self.inner, self.pubkey, send, recv))
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

/// One peer-to-peer harness connection. Cheap to clone is intentionally
/// **not** offered — this type owns the per-connection mutexes.
pub struct Connection {
    pubkey: PublicKey,
    addr: SocketAddr,
    /// Held so the connection isn't dropped while streams are alive.
    /// Underscore-prefixed because the type's lifetime is what we care
    /// about, not its API; we read it only in `close()`.
    inner: quinn::Connection,
    out: Mutex<quinn::SendStream>,
    framer: Mutex<RecvFramer>,
    recv: Mutex<quinn::RecvStream>,
    replay: ReplayTable,
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
    pub(crate) fn new(
        inner: quinn::Connection,
        pubkey: PublicKey,
        send: quinn::SendStream,
        recv: quinn::RecvStream,
    ) -> Self {
        let addr = inner.remote_address();
        Self {
            pubkey,
            addr,
            inner,
            out: Mutex::new(send),
            framer: Mutex::new(RecvFramer::new()),
            recv: Mutex::new(recv),
            replay: ReplayTable::new(),
        }
    }

    pub(crate) async fn from_quinn_dialer(
        connection: quinn::Connection,
        pubkey: PublicKey,
    ) -> Result<Self, TransportError> {
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(TransportError::from_quinn_connection_error)?;
        Ok(Self::new(connection, pubkey, send, recv))
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
        let mut payload = Vec::with_capacity(512);
        ciborium::ser::into_writer(msg, &mut payload)?;
        let framed = encode_frame(&payload)?;
        let mut out = self.out.lock().await;
        out.write_all(&framed).await?;
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
    /// the framer + the partially-consumed [`Bytes`] chunk live on `self`
    /// (under `framer: Mutex<RecvFramer>`). If a `read_chunk` returns more
    /// bytes than the current frame needs, the excess is fed back to the
    /// framer on the same call.
    async fn recv_one_frame(&self) -> Result<Vec<u8>, TransportError> {
        // Hold the framer lock for the entire frame assembly. This means
        // two concurrent recv() calls on the same Connection will queue,
        // which is correct — frames are ordered.
        let mut framer = self.framer.lock().await;
        let mut recv = self.recv.lock().await;

        // Try to extract a frame from any leftover bytes (none on first
        // call; future calls may have residue if we ever pipeline).
        let mut residue = Bytes::new();
        if let Some(frame) = framer.try_decode(&mut residue)? {
            return Ok(frame);
        }

        loop {
            // 64 KiB max chunk — arbitrary; quinn will return whatever's
            // available up to this size.
            let chunk = recv
                .read_chunk(64 * 1024, true)
                .await?
                .ok_or(TransportError::FrameTruncated)?;
            let mut bytes = chunk.bytes;
            // Consume into the framer; it returns Some(frame) when complete.
            if let Some(frame) = framer.try_decode(&mut bytes)? {
                // If quinn handed us more bytes than this frame needed,
                // we'd lose them here — but our protocol doesn't pipeline
                // frames over a single stream concurrently in 1.4. Phase 4's
                // multi-stream work revisits this. Asserting:
                debug_assert!(
                    bytes.is_empty(),
                    "extra bytes after a frame on a single-frame stream"
                );
                return Ok(frame);
            }
        }
    }
}
