//! Persistent `capability_index` table — `(capability_id, major) → node_id`.
//!
//! This is the on-disk twin of `harness_orchestrator::CapabilityIndex`.
//! Phase 2.4 keeps both in sync: the in-memory index is rebuilt from this
//! table on startup, and a manifest gossip event updates both atomically.

use harness_core::{Capability, NodeId};
use rusqlite::params;

use crate::error::StoreError;
use crate::open::Store;

/// Discriminator stored in the `cardinality` column. The full payload
/// (Owner's `scope_field`, Federated's merge strategy) lives in the
/// manifest's CBOR blob; this column is just for fast filter joins.
fn cardinality_tag(c: &harness_core::Cardinality) -> i64 {
    match c {
        // Cardinality is #[non_exhaustive]; unknown variants store as
        // Anyone (the dispatcher rejects them at routing time).
        harness_core::Cardinality::Owner { .. } => 1,
        harness_core::Cardinality::Federated { .. } => 2,
        _ => 0,
    }
}

fn cost_hint_str(c: harness_core::protocol::CostHint) -> &'static str {
    match c {
        harness_core::protocol::CostHint::LocalSlow => "local_slow",
        harness_core::protocol::CostHint::Gpu => "gpu",
        harness_core::protocol::CostHint::CloudPaid => "cloud_paid",
        // LocalFast + future #[non_exhaustive] variants.
        _ => "local_fast",
    }
}

impl Store {
    /// Replace the capability rows for a single node. Idempotent —
    /// re-calling with a new capability set drops the old rows first
    /// inside the same transaction.
    pub fn replace_node_capabilities(
        &self,
        node_id: NodeId,
        caps: &[Capability],
    ) -> Result<(), StoreError> {
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            tx.execute(
                "DELETE FROM capability_index WHERE node_id = ?",
                params![node_id.as_bytes()],
            )?;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO capability_index (
                        capability_id, version_major, version_minor, version_patch,
                        node_id, cardinality, cost_hint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)",
                )?;
                for cap in caps {
                    stmt.execute(params![
                        cap.id,
                        i64::from(cap.version.major),
                        i64::from(cap.version.minor),
                        i64::from(cap.version.patch),
                        node_id.as_bytes(),
                        cardinality_tag(&cap.cardinality),
                        cost_hint_str(cap.cost_hint),
                    ])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
    }

    /// Look up which trusted, manifest-known nodes advertise `capability`.
    /// Phase 3.x will add a typed `(id, major)` lookup once versioned
    /// capabilities coexist on the same mesh.
    pub fn capability_advertisers(&self, capability: &str) -> Result<Vec<NodeId>, StoreError> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT node_id FROM capability_index WHERE capability_id = ? ORDER BY node_id",
            )?;
            let ids = stmt
                .query_map(params![capability], |r| r.get::<_, Vec<u8>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            ids.into_iter()
                .map(|b| {
                    <[u8; 16]>::try_from(b.as_slice())
                        .map(NodeId::from_bytes)
                        .map_err(|_| StoreError::Cbor("node_id must be 16 bytes".into()))
                })
                .collect()
        })
    }

    /// Convenience for tests / diagnostics.
    pub fn count_capability_rows(&self) -> Result<usize, StoreError> {
        self.with_conn(|c| {
            let n: i64 = c.query_row("SELECT count(*) FROM capability_index", [], |r| r.get(0))?;
            Ok(usize::try_from(n).unwrap_or(0))
        })
    }
}

/// Look up by `(capability_id, major)`. Phase 3.x typed surface.
pub fn nodes_for_versioned_capability(
    store: &Store,
    capability: &str,
    major: u16,
) -> Result<Vec<NodeId>, StoreError> {
    store.with_conn(|c| {
        let mut stmt = c.prepare(
            "SELECT node_id FROM capability_index
              WHERE capability_id = ? AND version_major = ?
              ORDER BY node_id",
        )?;
        let ids = stmt
            .query_map(params![capability, i64::from(major)], |r| {
                r.get::<_, Vec<u8>>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|b| {
                <[u8; 16]>::try_from(b.as_slice())
                    .map(NodeId::from_bytes)
                    .map_err(|_| StoreError::Cbor("node_id must be 16 bytes".into()))
            })
            .collect()
    })
}

#[doc(hidden)]
pub use rusqlite as _rusqlite;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{
        Cardinality, Identity, NodeManifest, ResourceHints, Resources, SemVer, Signable, Signature,
    };

    fn empty_hints() -> ResourceHints {
        ResourceHints {
            cpu_class: harness_core::protocol::CpuClass::Light,
            memory_mb: None,
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: harness_core::protocol::NetworkClass::None,
            disk_io_class: harness_core::protocol::DiskIoClass::None,
            estimated_duration_ms: None,
        }
    }

    fn cap(id: &str, major: u16) -> Capability {
        Capability {
            id: id.into(),
            version: SemVer {
                major,
                minor: 0,
                patch: 0,
            },
            cardinality: Cardinality::Anyone,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            cost_hint: harness_core::protocol::CostHint::LocalFast,
            tags: vec![],
            rate_limit: None,
            resource_hints: empty_hints(),
            requires_secrets: vec![],
        }
    }

    fn signed_manifest(id: &Identity, caps: Vec<Capability>) -> NodeManifest {
        let mut m = NodeManifest {
            node_id: id.public_key().node_id(),
            hostname: "h".into(),
            pubkey: *id.public_key(),
            capabilities: caps,
            scopes: vec![],
            resources: Resources {
                cpu_cores: 0,
                ram_total_mb: 0,
                gpu: None,
                os: "test".into(),
                arch: "test".into(),
            },
            online_since: 0,
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
    fn replace_then_lookup() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let m = signed_manifest(&id, vec![cap("echo", 0)]);
        s.upsert_manifest(&m).expect("manifest");
        s.replace_node_capabilities(m.node_id, &m.capabilities)
            .expect("caps");
        assert_eq!(s.capability_advertisers("echo").unwrap(), vec![m.node_id]);
    }

    #[test]
    fn replace_drops_old_rows() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let m = signed_manifest(&id, vec![cap("echo", 0)]);
        s.upsert_manifest(&m).expect("manifest");
        s.replace_node_capabilities(m.node_id, &m.capabilities)
            .expect("caps v1");

        let new_caps = vec![cap("shell.exec", 0)];
        s.replace_node_capabilities(m.node_id, &new_caps)
            .expect("caps v2");
        assert!(s.capability_advertisers("echo").unwrap().is_empty());
        assert_eq!(
            s.capability_advertisers("shell.exec").unwrap(),
            vec![m.node_id]
        );
    }

    #[test]
    fn cascade_delete_clears_capability_rows() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let m = signed_manifest(&id, vec![cap("echo", 0)]);
        s.upsert_manifest(&m).expect("manifest");
        s.replace_node_capabilities(m.node_id, &m.capabilities)
            .expect("caps");
        assert_eq!(s.count_capability_rows().unwrap(), 1);
        s.delete_manifest(m.node_id).expect("delete");
        assert_eq!(s.count_capability_rows().unwrap(), 0);
    }

    #[test]
    fn versioned_lookup() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let m = signed_manifest(&id, vec![cap("echo", 1)]);
        s.upsert_manifest(&m).expect("manifest");
        s.replace_node_capabilities(m.node_id, &m.capabilities)
            .expect("caps");
        assert_eq!(
            nodes_for_versioned_capability(&s, "echo", 1).unwrap(),
            vec![m.node_id]
        );
        assert!(nodes_for_versioned_capability(&s, "echo", 0)
            .unwrap()
            .is_empty());
    }
}
