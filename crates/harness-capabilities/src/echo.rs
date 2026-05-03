//! Built-in `echo` capability — the simplest possible `Anyone` worker.
//!
//! Phase 2.8 demo gate: a Phase-2 user submits `harness submit echo
//! --input '{"msg": "hi"}'`, the dispatcher routes to any node, the
//! worker calls [`EchoCapability::execute`], and the output is the
//! input wrapped as `{"echoed": <input>}`.

use async_trait::async_trait;
use harness_core::Capability as ManifestEntry;
use serde_json::Value as JsonValue;

use crate::traits::{Capability, CapabilityError, ExecutionContext};

/// Capability id constant — re-used by the daemon when wiring up the
/// `harness submit echo ...` flow.
pub const ECHO_ID: &str = "echo";

/// `echo` — returns `{ "echoed": <input> }`. Cardinality `Anyone`,
/// `cost_hint = LocalFast`, no tags, version `0.1.0`.
#[derive(Debug, Default, Clone)]
pub struct EchoCapability;

impl EchoCapability {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// JSON Schema for the input. Permissive: `object` with no required
    /// fields. The wider mesh sees this in `Capability::input_schema`.
    fn input_schema() -> JsonValue {
        serde_json::json!({
            "type": "object",
            "additionalProperties": true,
            "description": "Anything goes — echo will return it wrapped under \"echoed\".",
        })
    }

    fn output_schema() -> JsonValue {
        serde_json::json!({
            "type": "object",
            "required": ["echoed"],
            "properties": { "echoed": {} },
        })
    }
}

#[async_trait]
impl Capability for EchoCapability {
    fn id(&self) -> &str {
        ECHO_ID
    }

    fn manifest(&self) -> ManifestEntry {
        ManifestEntry {
            id: ECHO_ID.to_string(),
            version: harness_core::SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            cardinality: harness_core::Cardinality::Anyone,
            input_schema: Self::input_schema(),
            output_schema: Self::output_schema(),
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
        }
    }

    async fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        Ok(serde_json::json!({ "echoed": input }))
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
            local_node_name: std::sync::Arc::from("self"),
            issued_by: NodeId::from_bytes([2; 16]),
            issued_by_name: std::sync::Arc::from("issuer"),
            task_id: TaskId::new_v7(),
        }
    }

    #[tokio::test]
    async fn echo_wraps_input_in_echoed_key() {
        let cap = EchoCapability::new();
        let out = cap
            .execute(&ctx(), serde_json::json!({"msg": "hi"}))
            .await
            .expect("execute");
        assert_eq!(out, serde_json::json!({"echoed": {"msg": "hi"}}));
    }

    #[tokio::test]
    async fn echo_handles_null_input() {
        let cap = EchoCapability::new();
        let out = cap
            .execute(&ctx(), serde_json::Value::Null)
            .await
            .expect("execute");
        assert_eq!(out, serde_json::json!({"echoed": null}));
    }

    #[tokio::test]
    async fn echo_handles_array_input() {
        let cap = EchoCapability::new();
        let out = cap
            .execute(&ctx(), serde_json::json!([1, 2, 3]))
            .await
            .expect("execute");
        assert_eq!(out, serde_json::json!({"echoed": [1, 2, 3]}));
    }

    #[test]
    fn manifest_advertises_anyone_cardinality_and_local_fast_cost() {
        let cap = EchoCapability::new();
        let m = cap.manifest();
        assert_eq!(m.id, "echo");
        assert!(matches!(m.cardinality, harness_core::Cardinality::Anyone));
        assert!(matches!(
            m.cost_hint,
            harness_core::protocol::CostHint::LocalFast
        ));
    }

    #[test]
    fn id_constant_matches_manifest_id() {
        assert_eq!(EchoCapability::new().id(), ECHO_ID);
    }
}
