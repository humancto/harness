//! Transport-layer errors. One enum, every fallible path through the QUIC
//! transport surfaces here.

use std::net::SocketAddr;

use crate::transport::cert::CertError;
use crate::transport::envelope::WireError;

/// Errors from the QUIC transport layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    #[error("bind: {0}")]
    Bind(#[source] std::io::Error),

    #[error("dial {addr}: {source}")]
    Dial {
        addr: SocketAddr,
        #[source]
        source: quinn::ConnectError,
    },

    #[error("connect {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        #[source]
        source: quinn::ConnectionError,
    },

    #[error("accept: {0}")]
    Accept(#[source] quinn::ConnectionError),

    #[error("peer cert did not match expected pubkey")]
    CertMismatch,

    #[error("peer cert was malformed: {0}")]
    BadPeerCert(String),

    #[error("frame too large: {len} > {max}")]
    FrameTooLarge { len: usize, max: usize },

    #[error("frame truncated (stream ended mid-frame)")]
    FrameTruncated,

    #[error("decode: {0}")]
    Decode(#[from] ciborium::de::Error<std::io::Error>),

    #[error("encode: {0}")]
    Encode(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("signature: {0}")]
    Signature(#[from] harness_core::VerifyError),

    #[error("replay: seq {got} <= last_seen {last_seen} on channel {channel}")]
    Replay {
        channel: &'static str,
        got: u64,
        last_seen: u64,
    },

    #[error("connection closed")]
    Closed,

    #[error("write: {0}")]
    Write(#[from] quinn::WriteError),

    #[error("read: {0}")]
    Read(#[from] quinn::ReadError),

    #[error("certificate generation: {0}")]
    Cert(#[source] CertError),

    #[error("tls config: {0}")]
    Tls(#[from] rustls::Error),

    #[error("dial timed out after {0:?}")]
    DialTimeout(std::time::Duration),

    #[error("protocol: {0}")]
    Protocol(String),
}

impl From<quinn::crypto::rustls::NoInitialCipherSuite> for TransportError {
    fn from(_: quinn::crypto::rustls::NoInitialCipherSuite) -> Self {
        Self::Protocol("rustls provider has no TLS 1.3 cipher suite".into())
    }
}

impl From<CertError> for TransportError {
    fn from(e: CertError) -> Self {
        Self::Cert(e)
    }
}

impl From<WireError> for TransportError {
    fn from(e: WireError) -> Self {
        match e {
            WireError::FrameTooLarge { len, max } => Self::FrameTooLarge { len, max },
            WireError::Replay {
                channel,
                got,
                last_seen,
            } => Self::Replay {
                channel,
                got,
                last_seen,
            },
        }
    }
}
