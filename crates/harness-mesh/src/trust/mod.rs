//! `~/.harness/peers.toml` — the trust store.
//!
//! Holds [`Peer`] entries known to the local node, each tagged with a
//! [`TrustTier`] (`Trusted` / `Default` / `Guest`) and provenance
//! (`PairingCode` / `Static` / `Imported`).
//!
//! On-disk format is **TOML, version 1**, hex-encoded for `node_id` and
//! `pubkey`. The full schema is documented in this module's source. Atomic
//! writes (born `0600` on Unix, fsync, rename) are shared with
//! [`crate::identity`] via the internal [`crate::fs_util`] module.
//!
//! See `phase-1.8-peers-toml.plan.md` for the design notes that drove the
//! shape of this module.
//!
//! ## What does NOT live here
//!
//! - **The local node's own identity** — that's `identity.key`'s job.
//!   `peers.toml` is *other peers only*, and `TrustStore::open` rejects any
//!   self-entry on load.
//! - **Pairing approval flow** — item 1.7 produces the trust entries this
//!   module persists; this module exposes `add` / `remove` for callers to
//!   wire up.
//! - **Gossip framing** — item 1.5 broadcasts trust changes across the
//!   mesh; this module exposes a `subscribe()` event stream that gossip
//!   subscribes to.

mod file;

use std::path::PathBuf;

use harness_core::{NodeId, PublicKey};
use serde::{Deserialize, Serialize};

/// A single trust entry. Persisted in `peers.toml`. Cloneable so it can be
/// handed to event subscribers and TOML serialization without aliasing the
/// store's cache.
///
/// Equality is by-value (every field). Ordering / hashing is the store's
/// responsibility, not derived here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub node_id: NodeId,
    pub pubkey: PublicKey,
    pub hostname: String,
    pub tier: TrustTier,
    /// Unix seconds when this peer was added.
    pub added_at: u64,
    pub added_via: AddedVia,
}

/// Trust tier — verbatim from PRD §10.2. `Default` is the implicit tier for
/// any peer not present in `peers.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum TrustTier {
    Trusted,
    Default,
    Guest,
}

/// How a [`Peer`] ended up in the store. Audit-relevant; carried in events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AddedVia {
    PairingCode,
    Static,
    Imported,
}

/// Errors from the trust store. Hard-error semantics — silent dropping of
/// trust entries is a security antipattern this module refuses to engage in.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TrustError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("could not parse peers.toml: {0}")]
    Parse(String),
    #[error("could not serialize peers.toml: {0}")]
    Serialize(String),
    #[error("peers.toml format_version {got} is not supported (this binary supports {supported})")]
    UnsupportedFormatVersion { got: u32, supported: u32 },
    #[error(
        "peers.toml entry inconsistent: node_id {node_id} does not match blake3(pubkey)[..16]"
    )]
    Inconsistent { node_id: String },
    #[error("peers.toml has unsafe permissions {actual:o}; expected 0600")]
    UnsafePermissions { actual: u32 },
    #[error("duplicate node_id {0} in peers.toml")]
    DuplicateNodeId(NodeId),
    #[error("refusing to add self ({0}) to peers.toml")]
    SelfPeer(NodeId),
}

impl TrustError {
    // Used by `TrustStore` in the next commit; tests in `file.rs` exercise
    // the variants directly.
    #[allow(dead_code)]
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

impl From<crate::fs_util::FsError> for TrustError {
    fn from(e: crate::fs_util::FsError) -> Self {
        match e {
            crate::fs_util::FsError::Io { path, source } => Self::Io { path, source },
        }
    }
}

impl From<crate::fs_util::Mode0600Error> for TrustError {
    fn from(e: crate::fs_util::Mode0600Error) -> Self {
        match e {
            crate::fs_util::Mode0600Error::Io { path, source } => Self::Io { path, source },
            crate::fs_util::Mode0600Error::UnsafePermissions { actual } => {
                Self::UnsafePermissions { actual }
            }
        }
    }
}

/// Convert a [`PublicKey`] to its 64-character lowercase hex form (the
/// on-disk encoding). Used by `file::PeerOnDisk` and (next commit)
/// `TrustStore`.
#[allow(dead_code)]
pub(crate) fn pubkey_to_hex(pk: &PublicKey) -> String {
    hex::encode(pk.as_bytes())
}

/// Parse a 64-character lowercase hex string back into a [`PublicKey`],
/// validating curve membership via `PublicKey::from_bytes`.
#[allow(dead_code)]
pub(crate) fn pubkey_from_hex(s: &str) -> Result<PublicKey, TrustError> {
    if s.len() != 64 {
        return Err(TrustError::Parse(format!(
            "pubkey must be exactly 64 hex characters, got {}",
            s.len()
        )));
    }
    let mut bytes = [0u8; 32];
    hex::decode_to_slice(s, &mut bytes)
        .map_err(|e| TrustError::Parse(format!("pubkey hex: {e}")))?;
    PublicKey::from_bytes(&bytes).map_err(|e| TrustError::Parse(format!("pubkey: {e}")))
}

/// Parse a 32-character lowercase hex string back into a [`NodeId`].
#[allow(dead_code)]
pub(crate) fn node_id_from_hex(s: &str) -> Result<NodeId, TrustError> {
    s.parse::<NodeId>()
        .map_err(|e| TrustError::Parse(format!("node_id hex: {e}")))
}
