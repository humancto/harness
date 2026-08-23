//! Built-in `mesh.info` capability — the first real
//! `Cardinality::Federated` capability (roadmap 4.5, ADR-0027).
//!
//! `harness submit mesh.info` = fleet inventory: the federated
//! coordinator fans the (empty) input to every live node, each node
//! answers one item describing itself, and `Concat` merges them into
//! `{"items": [...]}` with per-node provenance.
//!
//! Privacy note: `node_name` (mesh hostname), `os` and `arch` are
//! already broadcast in every signed `NodeManifest` (`hostname`,
//! `Resources::{os,arch}`) — this capability exposes nothing new, it
//! just makes the inventory queryable through the task lifecycle.

use async_trait::async_trait;
use harness_core::Capability as ManifestEntry;
use serde_json::Value as JsonValue;

use crate::traits::{Capability, CapabilityError, ExecutionContext};

pub const MESH_INFO_ID: &str = "mesh.info";

/// `mesh.info` — each node returns `{"items": [<self-description>]}`
/// (the `harness-merge` item convention, so `Concat` accounting is
/// exactly one item per node). `on_node_failure: ReturnPartial`: a
/// fleet inventory with one node missing is still useful — the missing
/// node is visible in `merge.failures` + provenance.
#[derive(Debug, Default, Clone)]
pub struct MeshInfoCapability;

impl MeshInfoCapability {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Capability for MeshInfoCapability {
    fn id(&self) -> &str {
        MESH_INFO_ID
    }

    fn manifest(&self) -> ManifestEntry {
        ManifestEntry {
            id: MESH_INFO_ID.to_string(),
            version: harness_core::SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            cardinality: harness_core::Cardinality::Federated {
                merge: harness_core::MergeStrategy::Concat,
                on_node_failure: harness_core::PartialPolicy::ReturnPartial,
            },
            input_schema: serde_json::json!({
                "type": "object",
                "additionalProperties": true,
                "description": "No input required.",
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "required": ["items"],
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["node_id", "node_name", "os", "arch"],
                        },
                    },
                },
            }),
            cost_hint: harness_core::protocol::CostHint::LocalFast,
            tags: vec![],
            rate_limit: None,
            resource_hints: harness_core::ResourceHints {
                cpu_class: harness_core::protocol::CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: harness_core::protocol::NetworkClass::None,
                disk_io_class: harness_core::protocol::DiskIoClass::None,
                estimated_duration_ms: Some(1),
            },
            requires_secrets: vec![],
        }
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        _input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        Ok(serde_json::json!({
            "items": [{
                "node_id": ctx.local_node.to_string(),
                "node_name": ctx.local_node_name.as_ref(),
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH,
            }],
        }))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use harness_core::{NodeId, TaskId};

    fn ctx() -> ExecutionContext {
        ExecutionContext {
            local_node: NodeId::from_bytes([1; 16]),
            local_node_name: std::sync::Arc::from("laptop-a"),
            issued_by: NodeId::from_bytes([2; 16]),
            issued_by_name: std::sync::Arc::from("issuer"),
            task_id: TaskId::new_v7(),
            tags: std::sync::Arc::from(Vec::<String>::new()),
        }
    }

    #[tokio::test]
    async fn returns_one_item_describing_self() {
        let out = MeshInfoCapability::new()
            .execute(&ctx(), serde_json::json!({}))
            .await
            .expect("execute");
        let items = out["items"].as_array().expect("items");
        assert_eq!(items.len(), 1, "exactly one item per node");
        assert_eq!(items[0]["node_name"], "laptop-a");
        assert_eq!(items[0]["os"], std::env::consts::OS);
        assert_eq!(items[0]["arch"], std::env::consts::ARCH);
        assert_eq!(
            items[0]["node_id"],
            NodeId::from_bytes([1; 16]).to_string()
        );
    }

    #[test]
    fn manifest_declares_federated_concat_return_partial() {
        let m = MeshInfoCapability::new().manifest();
        assert_eq!(m.id, "mesh.info");
        let harness_core::Cardinality::Federated {
            merge,
            on_node_failure,
        } = m.cardinality
        else {
            panic!("mesh.info must be Federated");
        };
        assert!(matches!(merge, harness_core::MergeStrategy::Concat));
        assert!(matches!(
            on_node_failure,
            harness_core::PartialPolicy::ReturnPartial
        ));
    }
}
