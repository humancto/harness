//! `node_manifests` table — most-recent verified manifest per peer.

use harness_core::{NodeId, NodeManifest, Signable, Signature};
use rusqlite::{params, OptionalExtension};

use crate::error::StoreError;
use crate::open::Store;

impl Store {
    /// Upsert a verified manifest. The caller is expected to have already
    /// verified the signature against the peer's public key — this function
    /// just persists.
    pub fn upsert_manifest(&self, manifest: &NodeManifest) -> Result<(), StoreError> {
        let cbor = manifest
            .canonical_bytes()
            .map_err(|e| StoreError::Cbor(e.to_string()))?;
        let sig = manifest.sig_field().to_bytes();
        let pubkey_hex = hex::encode(manifest.pubkey.as_bytes());
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO node_manifests (
                    node_id, pubkey_hex, hostname, online_since,
                    canonical_cbor, signature, received_at
                 ) VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))
                 ON CONFLICT(node_id) DO UPDATE SET
                    pubkey_hex     = excluded.pubkey_hex,
                    hostname       = excluded.hostname,
                    online_since   = excluded.online_since,
                    canonical_cbor = excluded.canonical_cbor,
                    signature      = excluded.signature,
                    received_at    = excluded.received_at",
                params![
                    manifest.node_id.as_bytes(),
                    pubkey_hex,
                    manifest.hostname,
                    i64::try_from(manifest.online_since).unwrap_or(i64::MAX),
                    cbor,
                    sig.as_slice(),
                ],
            )?;
            Ok(())
        })
    }

    /// Reconstruct a manifest from its stored canonical bytes + signature.
    pub fn load_manifest(&self, node_id: NodeId) -> Result<Option<NodeManifest>, StoreError> {
        self.with_conn(|c| {
            let row = c
                .query_row(
                    "SELECT canonical_cbor, signature FROM node_manifests WHERE node_id = ?",
                    params![node_id.as_bytes()],
                    |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
                .optional()?;
            let Some((cbor, sig_bytes)) = row else {
                return Ok(None);
            };
            let mut m: NodeManifest = ciborium::de::from_reader(cbor.as_slice())
                .map_err(|e| StoreError::Cbor(e.to_string()))?;
            let sig_arr: [u8; 64] = sig_bytes
                .as_slice()
                .try_into()
                .map_err(|_| StoreError::Cbor("signature must be 64 bytes".into()))?;
            *m.sig_field_mut() = Signature::from_bytes(sig_arr);
            Ok(Some(m))
        })
    }

    /// Drop a manifest and (via FK CASCADE) all derived index rows.
    pub fn delete_manifest(&self, node_id: NodeId) -> Result<(), StoreError> {
        self.with_conn(|c| {
            c.execute(
                "DELETE FROM node_manifests WHERE node_id = ?",
                params![node_id.as_bytes()],
            )?;
            Ok(())
        })
    }

    /// Count of manifests on disk — for diagnostics + tests.
    pub fn manifest_count(&self) -> Result<usize, StoreError> {
        self.with_conn(|c| {
            let n: i64 = c.query_row("SELECT count(*) FROM node_manifests", [], |r| r.get(0))?;
            Ok(usize::try_from(n).unwrap_or(0))
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Identity, NodeManifest, Resources, SemVer};

    fn signed_manifest(id: &Identity) -> NodeManifest {
        let mut m = NodeManifest {
            node_id: id.public_key().node_id(),
            hostname: "h".into(),
            pubkey: *id.public_key(),
            capabilities: vec![],
            scopes: vec![],
            resources: Resources {
                cpu_cores: 0,
                ram_total_mb: 0,
                gpu: None,
                os: "test".into(),
                arch: "test".into(),
            },
            online_since: 1_700_000_000,
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            sig: Signature::from_bytes([0; 64]),
        };
        m.sign(id).expect("sign");
        m
    }

    #[test]
    fn upsert_then_load() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let m = signed_manifest(&id);
        s.upsert_manifest(&m).expect("upsert");
        let loaded = s.load_manifest(m.node_id).expect("load").expect("present");
        assert_eq!(loaded, m);
        assert!(loaded.verify_signature(id.public_key()).is_ok());
    }

    #[test]
    fn upsert_replaces_existing() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let mut m = signed_manifest(&id);
        s.upsert_manifest(&m).expect("upsert v1");
        m.hostname = "renamed".into();
        m.sign(&id).expect("re-sign");
        s.upsert_manifest(&m).expect("upsert v2");
        let loaded = s.load_manifest(m.node_id).expect("load").expect("present");
        assert_eq!(loaded.hostname, "renamed");
        assert_eq!(s.manifest_count().unwrap(), 1);
    }

    #[test]
    fn delete_removes_row() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let m = signed_manifest(&id);
        s.upsert_manifest(&m).expect("upsert");
        s.delete_manifest(m.node_id).expect("delete");
        assert!(s.load_manifest(m.node_id).expect("load").is_none());
    }

    #[test]
    fn load_missing_returns_none() {
        let s = Store::open_memory().expect("open");
        let missing = NodeId::from_bytes([7; 16]);
        assert!(s.load_manifest(missing).expect("load").is_none());
    }
}
