//! `fs.read` — bounded file read under a scope. See ADR-0015.

use std::io::Read;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, Cardinality, SemVer};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::fs::scope::{parse_relative_path, ScopeRegistry};
use crate::traits::{Capability, CapabilityError, ExecutionContext};

pub const ID: &str = "fs.read";
pub const SCOPE_FIELD: &str = "scope";

const DEFAULT_MAX_BYTES: u64 = 1 << 20; // 1 MiB
pub const HARD_MAX_BYTES: u64 = 16 << 20; // 16 MiB

/// `fs.read` — bounded file read under an operator-configured scope.
#[derive(Debug, Clone)]
pub struct FsReadCapability {
    scopes: Arc<ScopeRegistry>,
}

impl FsReadCapability {
    #[must_use]
    pub fn new(scopes: Arc<ScopeRegistry>) -> Self {
        Self { scopes }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FsReadInput {
    scope: String,
    path: String,
    #[serde(default)]
    max_bytes: Option<u64>,
    #[serde(default)]
    encoding: Option<String>,
}

#[derive(Debug, Serialize)]
struct FsReadOutput {
    scope: String,
    path: String,
    /// Reported size on disk (from `metadata().len()`), even when the
    /// returned content is truncated. Lets callers decide whether to
    /// re-fetch with a higher `max_bytes` or chunk via repeated calls.
    size_bytes: u64,
    encoding: &'static str,
    content: String,
    truncated: bool,
}

#[async_trait]
impl Capability for FsReadCapability {
    fn id(&self) -> &str {
        ID
    }

    fn manifest(&self) -> ManifestEntry {
        ManifestEntry {
            id: ID.to_string(),
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            cardinality: Cardinality::Owner {
                scope_field: SCOPE_FIELD.to_string(),
            },
            input_schema: json!({
                "type": "object",
                "required": [SCOPE_FIELD, "path"],
                "additionalProperties": false,
                "properties": {
                    "scope":     { "type": "string", "minLength": 1 },
                    "path":      { "type": "string", "minLength": 1 },
                    "max_bytes": { "type": "integer", "minimum": 1, "maximum": HARD_MAX_BYTES },
                    "encoding":  { "type": "string", "enum": ["utf8", "base64"] },
                },
            }),
            output_schema: json!({
                "type": "object",
                "required": ["scope", "path", "size_bytes", "encoding", "content", "truncated"],
                "properties": {
                    "scope":      { "type": "string" },
                    "path":       { "type": "string" },
                    "size_bytes": { "type": "integer", "minimum": 0 },
                    "encoding":   { "type": "string" },
                    "content":    { "type": "string" },
                    "truncated":  { "type": "boolean" },
                },
            }),
            cost_hint: CostHint::LocalFast,
            tags: vec!["fs".to_string()],
            rate_limit: Some(RateLimit {
                per_second: 10,
                burst: 20,
            }),
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: NetworkClass::None,
                disk_io_class: DiskIoClass::Heavy,
                estimated_duration_ms: None,
            },
            requires_secrets: vec![],
        }
    }

    async fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        let input: FsReadInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;

        let scope = self.scopes.get(&input.scope).ok_or_else(|| {
            CapabilityError::InvalidInput(format!("unknown scope {:?}", input.scope))
        })?;

        parse_relative_path(&input.path)
            .map_err(|e| CapabilityError::InvalidInput(format!("invalid path: {e}")))?;

        let encoding_kind = match input.encoding.as_deref().unwrap_or("utf8") {
            "utf8" => Encoding::Utf8,
            "base64" => Encoding::Base64,
            other => {
                return Err(CapabilityError::InvalidInput(format!(
                    "unknown encoding {other:?}; want utf8 or base64"
                )));
            }
        };

        // is_file() gate via stat-first — FIFO / device / socket / dir
        // → InvalidInput. Critical: we must NOT `open()` first because
        // opening a FIFO with no writer blocks indefinitely (and the
        // sync open syscall blocks the runtime worker thread, defeating
        // tokio::time::timeout). `Dir::metadata` uses fstatat(2) which
        // never blocks.
        //
        // cap-std confines to the scope root on metadata/open alike;
        // symlink-to-outside is rejected at the metadata step itself,
        // not at a separate canonical-path check — TOCTOU-free.
        let meta = scope
            .root_dir
            .metadata(&input.path)
            .map_err(|e| CapabilityError::Failed(format!("stat under scope: {e}")))?;
        if !meta.is_file() {
            return Err(CapabilityError::InvalidInput(format!(
                "not a regular file: {kind}",
                kind = describe_file_type(&meta)
            )));
        }

        // Now open — `meta.is_file()` guarantees this is a regular file
        // and won't block. cap-std re-confines on this open.
        let mut file = scope
            .root_dir
            .open(&input.path)
            .map_err(|e| CapabilityError::Failed(format!("open file under scope: {e}")))?;

        let size_bytes = meta.len();
        let effective_cap = input
            .max_bytes
            .unwrap_or(DEFAULT_MAX_BYTES)
            .min(HARD_MAX_BYTES);

        // Stat-first allocation: reserve at most min(file size, cap)
        // + 1 byte (the +1 detects truncation). A 1 GiB file with a
        // 16 MiB cap allocates 16 MiB + 1 byte, never 1 GiB.
        // `usize::try_from` for 32-bit-pointer hosts; we reserve up to
        // HARD_MAX_BYTES (16 MiB) which fits in usize on all supported
        // platforms.
        let alloc_hint =
            usize::try_from(std::cmp::min(size_bytes, effective_cap)).unwrap_or(usize::MAX);
        let mut buf: Vec<u8> = Vec::with_capacity(alloc_hint.saturating_add(1));
        let read_limit = effective_cap.saturating_add(1);
        let bytes_read = (&mut file)
            .take(read_limit)
            .read_to_end(&mut buf)
            .map_err(|e| CapabilityError::Failed(format!("read: {e}")))?;

        let truncated = (bytes_read as u64) > effective_cap;
        if truncated {
            buf.truncate(effective_cap as usize);
        }

        let (encoding_str, content) = match encoding_kind {
            Encoding::Utf8 => match String::from_utf8(buf) {
                Ok(s) => ("utf8", s),
                Err(_) => {
                    return Err(CapabilityError::InvalidInput(
                        "file is not UTF-8; request encoding=base64".to_string(),
                    ));
                }
            },
            Encoding::Base64 => {
                let s = base64::engine::general_purpose::STANDARD.encode(&buf);
                ("base64", s)
            }
        };

        let out = FsReadOutput {
            scope: input.scope,
            path: input.path,
            size_bytes,
            encoding: encoding_str,
            content,
            truncated,
        };
        serde_json::to_value(&out)
            .map_err(|e| CapabilityError::Failed(format!("encode output: {e}")))
    }
}

#[derive(Debug, Clone, Copy)]
enum Encoding {
    Utf8,
    Base64,
}

fn describe_file_type(meta: &cap_std::fs::Metadata) -> &'static str {
    let ft = meta.file_type();
    if ft.is_dir() {
        "directory"
    } else if ft.is_symlink() {
        "symlink"
    } else if ft.is_file() {
        "file"
    } else {
        // FIFO / socket / device / unknown.
        "special"
    }
}
