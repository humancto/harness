//! `NodeManifest` and its supporting types: `Resources`, `Scope`,
//! `ResourceHints`, `Capability` (PRD §13.2).
//!
//! `Capability` is `PartialEq` only because `JsonValue` (used for
//! `input_schema` / `output_schema`) is not `Eq` (NaN). `NodeManifest`
//! inherits the same constraint via `Vec<Capability>`.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::identity::{NodeId, PublicKey, Signature};
use crate::ids::SemVer;
use crate::protocol::cardinality::Cardinality;
use crate::protocol::signable::Signable;
use crate::protocol::support::{CostHint, CpuClass, DiskIoClass, GpuInfo, NetworkClass, RateLimit};

/// Hardware advertised by a node in its manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resources {
    pub cpu_cores: u8,
    pub ram_total_mb: u32,
    pub gpu: Option<GpuInfo>,
    /// `"macos"` | `"linux"` | `"windows"`.
    pub os: String,
    /// `"x86_64"` | `"aarch64"`.
    pub arch: String,
}

/// A data domain the node owns. PRD §13.2.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Scope {
    /// `"directory"` | `"repo"` | `"photo_library"` | `"mailbox"`.
    pub kind: String,
    pub id: String,
    pub label: String,
    pub indexed: bool,
    /// Unix seconds.
    pub last_indexed: Option<u64>,
}

/// Resource declaration on a single task / capability call.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceHints {
    pub cpu_class: CpuClass,
    pub memory_mb: Option<u32>,
    pub gpu_required: bool,
    pub gpu_memory_mb: Option<u32>,
    pub network_class: NetworkClass,
    pub disk_io_class: DiskIoClass,
    pub estimated_duration_ms: Option<u32>,
}

/// A typed unit of work a node advertises that it can perform.
///
/// Note: `PartialEq` only — `input_schema`/`output_schema` are
/// `serde_json::Value` which is not `Eq` (NaN in numbers). `==` works for
/// non-NaN inputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capability {
    pub id: String,
    pub version: SemVer,
    pub cardinality: Cardinality,
    /// JSON Schema describing valid task inputs. Opaque on the wire;
    /// validated in `harness-capabilities` (Phase 2+).
    pub input_schema: JsonValue,
    pub output_schema: JsonValue,
    pub cost_hint: CostHint,
    pub tags: Vec<String>,
    pub rate_limit: Option<RateLimit>,
    pub resource_hints: ResourceHints,
    /// Secret tags this capability needs from the local credential store
    /// (PRD §10.5). Phase 3.6a: surfaces in the manifest only — the
    /// dispatcher's `requires_secrets`-aware routing (skip nodes missing
    /// the tag) lands with 3.6-encrypted.
    ///
    /// Wire-level back-compat: `serde(default)` lets old daemons that
    /// pre-date this field decode a manifest as `vec![]`;
    /// `skip_serializing_if = "Vec::is_empty"` keeps the CBOR diff
    /// clean for capabilities that don't read secrets (no extra bytes
    /// on the wire vs. the prior shape).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires_secrets: Vec<String>,
}

/// Reference to a capability *available on a node* — the projection of
/// a [`Capability`] manifest entry that planners and routers care about
/// (id + major-version compatibility, nothing schema-shaped).
///
/// Used by `harness-brain::PlannerBackend::plan` (PRD §15.3
/// `available_capabilities`) and by
/// [`harness_capabilities::WeakCapabilityRegistry::refs`]. Lives in
/// `harness-core` rather than `harness-brain` because non-planning
/// paths (a future `mesh.peers` introspection capability, for example)
/// will want the same type without dragging the planner crate in.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CapabilityRef {
    pub id: String,
    pub version_major: u16,
}

/// A node's signed declaration of identity, capabilities, scopes, and
/// resources. Gossiped to the mesh on change. PRD §13.2.
///
/// `pubkey: PublicKey` (not `[u8; 32]`) — leans on 1.1's curve-membership
/// check at deserialize time, so a peer that sends a manifest with a junk
/// pubkey gets `serde::de::Error` at decode, before any verify call.
///
/// Note: `PartialEq` only — `Vec<Capability>` is not `Eq`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeManifest {
    pub node_id: NodeId,
    pub hostname: String,
    pub pubkey: PublicKey,
    pub capabilities: Vec<Capability>,
    pub scopes: Vec<Scope>,
    pub resources: Resources,
    /// Unix seconds when this node first joined the mesh.
    pub online_since: u64,
    /// Harness binary version emitting this manifest.
    pub version: SemVer,
    pub sig: Signature,
}

impl Signable for NodeManifest {
    fn sig_field_mut(&mut self) -> &mut Signature {
        &mut self.sig
    }
    fn sig_field(&self) -> &Signature {
        &self.sig
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::protocol::cardinality::{MergeStrategy, PartialPolicy};

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(value, &mut buf).expect("serialize");
        ciborium::de::from_reader(buf.as_slice()).expect("deserialize")
    }

    fn sample_resource_hints() -> ResourceHints {
        ResourceHints {
            cpu_class: CpuClass::Heavy,
            memory_mb: Some(4096),
            gpu_required: false,
            gpu_memory_mb: None,
            network_class: NetworkClass::Light,
            disk_io_class: DiskIoClass::None,
            estimated_duration_ms: Some(150),
        }
    }

    fn sample_capability() -> Capability {
        Capability {
            id: "shell.exec".into(),
            version: SemVer::new(0, 1, 0),
            cardinality: Cardinality::Federated {
                merge: MergeStrategy::Concat,
                on_node_failure: PartialPolicy::ReturnPartial,
            },
            input_schema: serde_json::json!({"type": "object"}),
            output_schema: serde_json::json!({"type": "string"}),
            cost_hint: CostHint::LocalFast,
            tags: vec!["utility".into(), "shell".into()],
            rate_limit: Some(RateLimit {
                per_second: 10,
                burst: 20,
            }),
            resource_hints: sample_resource_hints(),
            requires_secrets: vec![],
        }
    }

    #[test]
    fn resources_round_trip() {
        let r = Resources {
            cpu_cores: 12,
            ram_total_mb: 32_768,
            gpu: Some(GpuInfo {
                vendor: "apple".into(),
                model: "M2 Pro".into(),
                vram_mb: 16_384,
            }),
            os: "macos".into(),
            arch: "aarch64".into(),
        };
        assert_eq!(r, round_trip(&r));
    }

    #[test]
    fn resources_round_trip_no_gpu() {
        let r = Resources {
            cpu_cores: 4,
            ram_total_mb: 16_384,
            gpu: None,
            os: "linux".into(),
            arch: "x86_64".into(),
        };
        assert_eq!(r, round_trip(&r));
    }

    #[test]
    fn scope_round_trip() {
        let s = Scope {
            kind: "directory".into(),
            id: "/Users/me/Documents".into(),
            label: "Documents".into(),
            indexed: true,
            last_indexed: Some(1_700_000_000),
        };
        assert_eq!(s, round_trip(&s));
    }

    #[test]
    fn resource_hints_round_trip() {
        let h = sample_resource_hints();
        assert_eq!(h, round_trip(&h));
    }

    #[test]
    fn capability_round_trip() {
        let c = sample_capability();
        assert_eq!(c, round_trip(&c));
    }

    #[test]
    fn capability_ref_round_trip() {
        let r = CapabilityRef {
            id: "shell.exec".into(),
            version_major: 0,
        };
        assert_eq!(r, round_trip(&r));
    }

    /// Wire-level back-compat for `Capability::requires_secrets`:
    ///
    /// 1. A new daemon with `requires_secrets: vec![]` produces CBOR
    ///    that does NOT carry the `requires_secrets` key (size parity
    ///    with pre-3.6a manifests).
    /// 2. CBOR produced WITHOUT the field decodes to `vec![]`.
    /// 3. CBOR with a non-empty `requires_secrets` round-trips
    ///    losslessly.
    #[test]
    fn capability_requires_secrets_round_trip() {
        // 1: empty vec is not serialized.
        let mut empty = sample_capability();
        empty.requires_secrets.clear();
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&empty, &mut buf).expect("ser");
        // CBOR map keys are encoded as text strings; search the encoded
        // bytes for the literal field name. If `skip_serializing_if`
        // works the bytes do not contain it.
        let needle = b"requires_secrets";
        assert!(
            !buf.windows(needle.len()).any(|w| w == needle),
            "empty requires_secrets must not appear in the CBOR bytes"
        );

        // 2: that same CBOR decodes back to `vec![]`.
        let decoded: Capability =
            ciborium::de::from_reader(buf.as_slice()).expect("de empty-shape");
        assert_eq!(decoded.requires_secrets, Vec::<String>::new());

        // 3: non-empty round-trip.
        let mut with = sample_capability();
        with.requires_secrets = vec!["secret/claude-api-key".to_string()];
        let rt = round_trip(&with);
        assert_eq!(
            rt.requires_secrets,
            vec!["secret/claude-api-key".to_string()]
        );
        assert_eq!(rt, with);
    }

    #[test]
    fn node_manifest_round_trip() {
        let id = crate::Identity::from_secret_bytes(&[0x42u8; 32]);
        let m = NodeManifest {
            node_id: id.node_id(),
            hostname: "macbook-archy".into(),
            pubkey: *id.public_key(),
            capabilities: vec![sample_capability()],
            scopes: vec![Scope {
                kind: "repo".into(),
                id: "/Users/me/dev/harness".into(),
                label: "harness".into(),
                indexed: false,
                last_indexed: None,
            }],
            resources: Resources {
                cpu_cores: 12,
                ram_total_mb: 32_768,
                gpu: None,
                os: "macos".into(),
                arch: "aarch64".into(),
            },
            online_since: 1_700_000_000,
            version: SemVer::new(0, 0, 0),
            sig: Signature::from_bytes([0u8; 64]),
        };
        assert_eq!(m, round_trip(&m));
    }

    #[test]
    fn node_manifest_sign_then_verify() {
        let id = crate::Identity::from_secret_bytes(&[0x37u8; 32]);
        let mut m = NodeManifest {
            node_id: id.node_id(),
            hostname: "test".into(),
            pubkey: *id.public_key(),
            capabilities: vec![],
            scopes: vec![],
            resources: Resources {
                cpu_cores: 1,
                ram_total_mb: 1,
                gpu: None,
                os: "linux".into(),
                arch: "x86_64".into(),
            },
            online_since: 0,
            version: SemVer::new(0, 0, 0),
            sig: Signature::from_bytes([0u8; 64]),
        };
        m.sign(&id).expect("sign");
        m.verify_signature(id.public_key())
            .expect("self-signature must verify");
    }
}
