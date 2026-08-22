//! Persistent `scope_index` table — `scope_id → node_id` for `Owner`
//! cardinality routing.

use harness_core::{NodeId, Scope};
use rusqlite::params;

use crate::error::StoreError;
use crate::open::Store;

impl Store {
    /// Replace the scope rows for a single node. Idempotent.
    pub fn replace_node_scopes(&self, node_id: NodeId, scopes: &[Scope]) -> Result<(), StoreError> {
        self.with_conn(|c| {
            let tx = c.unchecked_transaction()?;
            tx.execute(
                "DELETE FROM scope_index WHERE node_id = ?",
                params![node_id.as_bytes()],
            )?;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO scope_index (
                        scope_id, scope_kind, label, node_id, indexed, last_indexed
                    ) VALUES (?, ?, ?, ?, ?, ?)",
                )?;
                for scope in scopes {
                    stmt.execute(params![
                        scope.id,
                        scope.kind,
                        scope.label,
                        node_id.as_bytes(),
                        i64::from(scope.indexed),
                        scope
                            .last_indexed
                            .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
                    ])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
    }

    /// Owners of `scope_id`, sorted ascending by `node_id`.
    pub fn scope_owners(&self, scope_id: &str) -> Result<Vec<NodeId>, StoreError> {
        self.with_conn(|c| {
            let mut stmt =
                c.prepare("SELECT node_id FROM scope_index WHERE scope_id = ? ORDER BY node_id")?;
            let ids = stmt
                .query_map(params![scope_id], |r| r.get::<_, Vec<u8>>(0))?
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

    pub fn count_scope_rows(&self) -> Result<usize, StoreError> {
        self.with_conn(|c| {
            let n: i64 = c.query_row("SELECT count(*) FROM scope_index", [], |r| r.get(0))?;
            Ok(usize::try_from(n).unwrap_or(0))
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{Identity, NodeManifest, Resources, Scope, SemVer, Signable, Signature};

    fn signed_manifest(id: &Identity, scopes: Vec<Scope>) -> NodeManifest {
        let mut m = NodeManifest {
            node_id: id.public_key().node_id(),
            hostname: "h".into(),
            pubkey: *id.public_key(),
            capabilities: vec![],
            scopes,
            secret_tags: vec![],
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

    fn scope(id: &str) -> Scope {
        Scope {
            kind: "directory".into(),
            id: id.into(),
            label: id.into(),
            indexed: false,
            last_indexed: None,
        }
    }

    #[test]
    fn replace_then_lookup() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let m = signed_manifest(&id, vec![scope("~/work")]);
        s.upsert_manifest(&m).expect("manifest");
        s.replace_node_scopes(m.node_id, &m.scopes).expect("scopes");
        assert_eq!(s.scope_owners("~/work").unwrap(), vec![m.node_id]);
    }

    #[test]
    fn cascade_delete_clears_scope_rows() {
        let s = Store::open_memory().expect("open");
        let id = Identity::generate();
        let m = signed_manifest(&id, vec![scope("~/work"), scope("~/photos")]);
        s.upsert_manifest(&m).expect("manifest");
        s.replace_node_scopes(m.node_id, &m.scopes).expect("scopes");
        assert_eq!(s.count_scope_rows().unwrap(), 2);
        s.delete_manifest(m.node_id).expect("delete");
        assert_eq!(s.count_scope_rows().unwrap(), 0);
    }
}
