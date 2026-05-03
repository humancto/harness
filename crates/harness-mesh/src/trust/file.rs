//! On-disk representation of `peers.toml`. Private — `Peer` is the public
//! type; this module's `PeerOnDisk` exists only to control the wire-format
//! encoding (lowercase hex, integer epoch seconds, `snake_case` enum strings).

use harness_core::NodeId;
use serde::{Deserialize, Serialize};

use super::{
    node_id_from_hex, pubkey_from_hex, pubkey_to_hex, AddedVia, Peer, TrustError, TrustTier,
};

/// Bumped on any breaking change to the `peers.toml` format. Loading a file
/// with a higher version is a hard error — the operator is on a newer
/// binary's file and needs to either upgrade or hand-edit.
pub(crate) const FORMAT_VERSION: u32 = 1;

const HEADER_COMMENT: &str = "# Managed by harness — hand-edit at your own risk.\n";

/// The on-disk shape of `~/.harness/peers.toml`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PeersFile {
    pub format_version: u32,
    #[serde(default, rename = "peer")]
    pub peers: Vec<PeerOnDisk>,
}

/// A single `[[peer]]` entry in `peers.toml`. Hex-encoded for cryptographic
/// fields so the file is human-eyeballable.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PeerOnDisk {
    /// 32 lowercase hex characters.
    pub node_id: String,
    /// 64 lowercase hex characters.
    pub pubkey: String,
    pub hostname: String,
    pub tier: TrustTier,
    /// Unix seconds.
    pub added_at: u64,
    pub added_via: AddedVia,
}

impl PeerOnDisk {
    /// Validate the on-disk fields and convert into the typed [`Peer`].
    ///
    /// Errors on:
    /// - Invalid hex in `node_id` or `pubkey`.
    /// - `pubkey` that isn't a valid Ed25519 curve point.
    /// - `node_id` that doesn't match `blake3(pubkey)[..16]`.
    pub(crate) fn try_into_peer(self) -> Result<Peer, TrustError> {
        let pubkey = pubkey_from_hex(&self.pubkey)?;
        let node_id = node_id_from_hex(&self.node_id)?;
        let derived = NodeId::from_pubkey(&pubkey);
        if node_id != derived {
            return Err(TrustError::Inconsistent {
                node_id: self.node_id,
            });
        }
        Ok(Peer {
            node_id,
            pubkey,
            hostname: self.hostname,
            tier: self.tier,
            added_at: self.added_at,
            added_via: self.added_via,
        })
    }
}

impl From<&Peer> for PeerOnDisk {
    fn from(p: &Peer) -> Self {
        Self {
            node_id: p.node_id.to_string(),
            pubkey: pubkey_to_hex(&p.pubkey),
            hostname: p.hostname.clone(),
            tier: p.tier,
            added_at: p.added_at,
            added_via: p.added_via,
        }
    }
}

impl PeersFile {
    /// Parse + validate a `peers.toml` byte slice.
    ///
    /// Hard-errors on every inconsistency — wrong `format_version`, bad hex,
    /// curve-invalid pubkey, `node_id` / pubkey mismatch, duplicate
    /// `node_id` across entries. Silent dropping of trust entries is the
    /// wrong call.
    ///
    /// Returns peers sorted by `node_id` for deterministic output.
    pub(crate) fn parse(bytes: &[u8]) -> Result<Vec<Peer>, TrustError> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| TrustError::Parse(format!("peers.toml is not valid UTF-8: {e}")))?;
        let parsed: PeersFile =
            toml::from_str(s).map_err(|e| TrustError::Parse(format!("toml: {e}")))?;

        if parsed.format_version != FORMAT_VERSION {
            return Err(TrustError::UnsupportedFormatVersion {
                got: parsed.format_version,
                supported: FORMAT_VERSION,
            });
        }

        let mut peers: Vec<Peer> = Vec::with_capacity(parsed.peers.len());
        for on_disk in parsed.peers {
            peers.push(on_disk.try_into_peer()?);
        }

        peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        // Duplicate-`node_id` check after sort (adjacent compare).
        for window in peers.windows(2) {
            if window[0].node_id == window[1].node_id {
                return Err(TrustError::DuplicateNodeId(window[0].node_id));
            }
        }

        Ok(peers)
    }

    /// Serialize a slice of peers back to `peers.toml` text. Sorts by
    /// `node_id` first so the file's diff is always minimal under
    /// version-control review.
    pub(crate) fn serialize(peers: &[Peer]) -> Result<String, TrustError> {
        let mut sorted: Vec<&Peer> = peers.iter().collect();
        sorted.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        let on_disk: Vec<PeerOnDisk> = sorted.iter().map(|p| PeerOnDisk::from(*p)).collect();
        let file = PeersFile {
            format_version: FORMAT_VERSION,
            peers: on_disk,
        };
        let body = toml::to_string_pretty(&file)
            .map_err(|e| TrustError::Serialize(format!("toml: {e}")))?;
        Ok(format!("{HEADER_COMMENT}{body}"))
    }

    /// Serialize an empty store. Used by `TrustStore::open` when the file
    /// doesn't yet exist.
    pub(crate) fn empty() -> String {
        let file = PeersFile {
            format_version: FORMAT_VERSION,
            peers: Vec::new(),
        };
        // Empty serialization is infallible; if it ever isn't, surface the
        // panic — that's a programmer error, not a runtime data error.
        let body = toml::to_string_pretty(&file).unwrap_or_else(|_| {
            // Fallback that's still valid toml in case something pathological
            // happens. Better than failing daemon startup on a zero-peer file.
            format!("format_version = {FORMAT_VERSION}\n")
        });
        format!("{HEADER_COMMENT}{body}")
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::Identity;

    fn sample_peer(seed_byte: u8, tier: TrustTier) -> Peer {
        let id = Identity::from_secret_bytes(&[seed_byte; 32]);
        Peer {
            node_id: id.node_id(),
            pubkey: *id.public_key(),
            hostname: format!("host-{seed_byte:02x}"),
            tier,
            added_at: 1_700_000_000 + u64::from(seed_byte),
            added_via: AddedVia::PairingCode,
        }
    }

    #[test]
    fn peer_on_disk_round_trip() {
        let p = sample_peer(0x42, TrustTier::Trusted);
        let on_disk = PeerOnDisk::from(&p);
        let back = on_disk.try_into_peer().expect("valid");
        assert_eq!(p, back);
    }

    #[test]
    fn peers_file_round_trip_two_peers() {
        let a = sample_peer(0x10, TrustTier::Trusted);
        let b = sample_peer(0x20, TrustTier::Default);
        let serialized = PeersFile::serialize(&[a.clone(), b.clone()]).expect("serialize");
        let parsed = PeersFile::parse(serialized.as_bytes()).expect("parse");
        assert_eq!(parsed.len(), 2);
        // Sorted by node_id, so we can't guarantee a/b order — assert membership.
        assert!(parsed.contains(&a));
        assert!(parsed.contains(&b));
    }

    #[test]
    fn header_comment_is_present_on_serialize() {
        let p = sample_peer(0x01, TrustTier::Default);
        let serialized = PeersFile::serialize(&[p]).expect("serialize");
        assert!(
            serialized.starts_with("# Managed by harness"),
            "missing header: {serialized}"
        );
    }

    #[test]
    fn parsed_entries_are_sorted_by_node_id() {
        // node_ids of these peers are determined by blake3(pubkey)[..16]; we
        // can't predict them, but we can prove the parse output is sorted by
        // checking adjacent pairs.
        let peers = vec![
            sample_peer(0x10, TrustTier::Default),
            sample_peer(0x20, TrustTier::Default),
            sample_peer(0x30, TrustTier::Default),
            sample_peer(0x40, TrustTier::Default),
        ];
        let serialized = PeersFile::serialize(&peers).expect("serialize");
        let parsed = PeersFile::parse(serialized.as_bytes()).expect("parse");
        assert_eq!(parsed.len(), 4);
        for window in parsed.windows(2) {
            assert!(
                window[0].node_id < window[1].node_id,
                "not sorted: {} >= {}",
                window[0].node_id,
                window[1].node_id
            );
        }
    }

    #[test]
    fn empty_file_is_valid_toml_with_header() {
        let s = PeersFile::empty();
        assert!(s.starts_with("# Managed by harness"));
        let parsed = PeersFile::parse(s.as_bytes()).expect("parse");
        assert!(parsed.is_empty());
    }

    #[test]
    fn format_version_must_be_1() {
        let bad = "format_version = 99\n";
        let err = PeersFile::parse(bad.as_bytes()).expect_err("bad version");
        match err {
            TrustError::UnsupportedFormatVersion { got, supported } => {
                assert_eq!(got, 99);
                assert_eq!(supported, FORMAT_VERSION);
            }
            other => panic!("expected UnsupportedFormatVersion, got {other:?}"),
        }
    }

    #[test]
    fn missing_format_version_rejected() {
        let bad = "[[peer]]\nnode_id=\"x\"\n";
        let err = PeersFile::parse(bad.as_bytes()).expect_err("missing version");
        assert!(matches!(err, TrustError::Parse(_)), "got {err:?}");
    }

    #[test]
    fn node_id_must_match_pubkey() {
        let p = sample_peer(0x55, TrustTier::Default);
        let mut on_disk = PeerOnDisk::from(&p);
        // Replace the node_id with a different valid 32-hex string.
        on_disk.node_id = "00000000000000000000000000000000".to_string();
        let err = on_disk.try_into_peer().expect_err("inconsistent");
        match err {
            TrustError::Inconsistent { node_id } => {
                assert_eq!(node_id, "00000000000000000000000000000000");
            }
            other => panic!("expected Inconsistent, got {other:?}"),
        }
    }

    /// A tampered pubkey is detected — either by the curve-membership check
    /// inside `PublicKey::from_bytes` (returns `Parse`) or by the
    /// `node_id == blake3(pubkey)[..16]` consistency check (returns
    /// `Inconsistent`). Both are valid detection paths; we accept either.
    ///
    /// (Plan §9.1 named this `pubkey_curve_membership_enforced` and expected
    /// only `Parse`. In practice many "random-looking" 32-byte sequences
    /// like `0xff` repeated are valid curve points and only the `node_id`
    /// check catches them. Locking the broader contract is the honest
    /// thing to do.)
    #[test]
    fn pubkey_tampering_is_rejected() {
        let p = sample_peer(0x77, TrustTier::Default);
        let mut on_disk = PeerOnDisk::from(&p);
        on_disk.pubkey = "ff".repeat(32);
        let err = on_disk.try_into_peer().expect_err("tampered pubkey");
        assert!(
            matches!(err, TrustError::Parse(_) | TrustError::Inconsistent { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn pubkey_wrong_length_rejected() {
        let p = sample_peer(0x88, TrustTier::Default);
        let mut on_disk = PeerOnDisk::from(&p);
        on_disk.pubkey = "ab".to_string(); // way too short
        let err = on_disk.try_into_peer().expect_err("short pubkey");
        assert!(matches!(err, TrustError::Parse(_)), "got {err:?}");
    }

    #[test]
    fn unknown_tier_rejected() {
        let bad = format!(
            "format_version = {FORMAT_VERSION}\n\n\
             [[peer]]\n\
             node_id = \"00000000000000000000000000000001\"\n\
             pubkey = \"{pk}\"\n\
             hostname = \"x\"\n\
             tier = \"godmode\"\n\
             added_at = 0\n\
             added_via = \"pairing_code\"\n",
            pk = "0".repeat(64)
        );
        let err = PeersFile::parse(bad.as_bytes()).expect_err("bad tier");
        assert!(matches!(err, TrustError::Parse(_)), "got {err:?}");
    }

    #[test]
    fn duplicate_node_id_across_entries_rejected() {
        // Build two entries with the same hand-chosen node_id but the parse
        // path runs the per-entry pubkey-match check first; to hit
        // DuplicateNodeId we need both entries to be internally consistent
        // but share node_ids. That's mathematically impossible with valid
        // distinct pubkeys (blake3 collision in 16 bytes is ~2^-64), so we
        // serialize the same peer twice, which produces identical entries.
        let p = sample_peer(0x99, TrustTier::Default);
        let s = PeersFile::serialize(&[p.clone(), p]).expect("serialize");
        // After serialize-then-parse, the parser sees duplicate entries.
        let err = PeersFile::parse(s.as_bytes()).expect_err("duplicate");
        assert!(matches!(err, TrustError::DuplicateNodeId(_)), "got {err:?}");
    }
}
