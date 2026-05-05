//! `fs.list` — bounded directory listing under a scope. See ADR-0015.

use std::sync::Arc;

use async_trait::async_trait;
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, Cardinality, SemVer};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::fs::scope::{parse_relative_path, ScopeRegistry};
use crate::traits::{Capability, CapabilityError, ExecutionContext};

pub const ID: &str = "fs.list";
pub const SCOPE_FIELD: &str = "scope";

const DEFAULT_MAX_ENTRIES: u32 = 1_000;
pub const HARD_MAX_ENTRIES: u32 = 10_000;
const DEFAULT_MAX_DEPTH: u32 = 1;
pub const HARD_MAX_DEPTH: u32 = 8;

/// `fs.list` — directory listing bounded by depth + entry count caps.
#[derive(Debug, Clone)]
pub struct FsListCapability {
    scopes: Arc<ScopeRegistry>,
}

impl FsListCapability {
    #[must_use]
    pub fn new(scopes: Arc<ScopeRegistry>) -> Self {
        Self { scopes }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FsListInput {
    scope: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    max_entries: Option<u32>,
    #[serde(default)]
    max_depth: Option<u32>,
}

#[derive(Debug, Serialize)]
struct FsListOutput {
    scope: String,
    path: String,
    entries: Vec<EntryDto>,
    truncated: bool,
    skipped_non_utf8: u32,
}

#[derive(Debug, Serialize)]
struct EntryDto {
    name: String,
    /// Relative path from `path` (so a depth-2 listing is unambiguous).
    relative: String,
    kind: &'static str, // "file" | "dir" | "symlink"
    size_bytes: Option<u64>,
}

#[async_trait]
impl Capability for FsListCapability {
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
            // Forward-compat for 3.3-fanout. 3.10a's runtime path requires
            // the operator to submit on the owning node — see ADR-0015 §2.
            cardinality: Cardinality::Owner {
                scope_field: SCOPE_FIELD.to_string(),
            },
            input_schema: json!({
                "type": "object",
                "required": [SCOPE_FIELD],
                "additionalProperties": false,
                "properties": {
                    "scope":       { "type": "string", "minLength": 1 },
                    "path":        { "type": "string" },
                    "max_entries": { "type": "integer", "minimum": 1, "maximum": HARD_MAX_ENTRIES },
                    "max_depth":   { "type": "integer", "minimum": 1, "maximum": HARD_MAX_DEPTH },
                },
            }),
            output_schema: json!({
                "type": "object",
                "required": ["scope", "path", "entries", "truncated", "skipped_non_utf8"],
                "properties": {
                    "scope":            { "type": "string" },
                    "path":             { "type": "string" },
                    "entries":          { "type": "array" },
                    "truncated":        { "type": "boolean" },
                    "skipped_non_utf8": { "type": "integer", "minimum": 0 },
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
                disk_io_class: DiskIoClass::Light,
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
        let input: FsListInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;

        let scope = self.scopes.get(&input.scope).ok_or_else(|| {
            CapabilityError::InvalidInput(format!("unknown scope {:?}", input.scope))
        })?;

        parse_relative_path(&input.path)
            .map_err(|e| CapabilityError::InvalidInput(format!("invalid path: {e}")))?;

        let max_entries = input
            .max_entries
            .map_or(DEFAULT_MAX_ENTRIES, |v| v.min(HARD_MAX_ENTRIES));
        let max_depth = input
            .max_depth
            .map_or(DEFAULT_MAX_DEPTH, |v| v.min(HARD_MAX_DEPTH));

        // Resolve the starting directory under the scope's Dir handle.
        // cap-std confines to the scope root; symlink-to-outside fails
        // here.
        let start = if input.path.is_empty() || input.path == "." {
            scope
                .root_dir
                .try_clone()
                .map_err(|e| CapabilityError::Failed(format!("clone scope dir: {e}")))?
        } else {
            scope
                .root_dir
                .open_dir(&input.path)
                .map_err(|e| CapabilityError::Failed(format!("open dir under scope: {e}")))?
        };

        let path_label = if input.path.is_empty() {
            ".".to_string()
        } else {
            input.path.clone()
        };

        let (entries, truncated, skipped) = list_bfs(&start, max_entries, max_depth);

        let out = FsListOutput {
            scope: input.scope,
            path: path_label,
            entries,
            truncated,
            skipped_non_utf8: skipped,
        };
        serde_json::to_value(&out)
            .map_err(|e| CapabilityError::Failed(format!("encode output: {e}")))
    }
}

/// Bounded BFS over the cap-std `Dir`. Each level pushes at most
/// `(max_entries - already_collected) + 1` to the queue so the +1
/// sentinel detects truncation without inflating the response.
fn list_bfs(
    start: &cap_std::fs::Dir,
    max_entries: u32,
    max_depth: u32,
) -> (Vec<EntryDto>, bool, u32) {
    let mut entries: Vec<EntryDto> = Vec::new();
    let mut skipped_non_utf8: u32 = 0;
    let mut truncated = false;

    // (Dir handle, relative-prefix-from-start, depth — 1-indexed where 1 = top level)
    let mut queue: std::collections::VecDeque<(cap_std::fs::Dir, String, u32)> =
        std::collections::VecDeque::new();
    let Ok(start_clone) = start.try_clone() else {
        return (entries, false, skipped_non_utf8);
    };
    queue.push_back((start_clone, String::new(), 1));

    while let Some((dir, prefix, depth)) = queue.pop_front() {
        // Width capped at HARD_MAX_ENTRIES (10_000), well under u32::MAX.
        #[allow(clippy::cast_possible_truncation)]
        if entries.len() as u32 >= max_entries {
            truncated = true;
            break;
        }
        let read_dir = match dir.entries() {
            Ok(rd) => rd,
            Err(err) => {
                tracing::debug!(?err, "fs.list: read_dir failed; skipping subtree");
                continue;
            }
        };

        for de in read_dir {
            let de = match de {
                Ok(d) => d,
                Err(err) => {
                    tracing::debug!(?err, "fs.list: dir entry read failed; skipping");
                    continue;
                }
            };

            let raw_name = de.file_name();
            let Some(name) = raw_name.to_str().map(str::to_owned) else {
                skipped_non_utf8 = skipped_non_utf8.saturating_add(1);
                continue;
            };

            let relative = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };

            // `symlink_metadata` does NOT follow the link; that's what
            // we want — symlinks should NOT be traversed during
            // enumeration (loop risk + scope-leak).
            let meta = match de.metadata() {
                Ok(m) => m,
                Err(err) => {
                    tracing::debug!(?err, name = %name, "fs.list: metadata failed; skipping");
                    continue;
                }
            };

            let (kind, size_bytes) = if meta.is_file() {
                ("file", Some(meta.len()))
            } else if meta.is_dir() {
                ("dir", None)
            } else if meta.file_type().is_symlink() {
                // No `target` field returned — the symlink target
                // string can leak filesystem layout outside the scope.
                // ADR-0015 §5.
                ("symlink", None)
            } else {
                // FIFO, socket, block / char device, etc. Listed but
                // marked as a generic "other".
                ("other", None)
            };

            entries.push(EntryDto {
                name,
                relative,
                kind,
                size_bytes,
            });

            #[allow(clippy::cast_possible_truncation)]
            if entries.len() as u32 > max_entries {
                truncated = true;
                entries.truncate(max_entries as usize);
                break;
            }

            if kind == "dir" && depth < max_depth {
                let child_dir = match de.open_dir() {
                    Ok(d) => d,
                    Err(err) => {
                        tracing::debug!(?err, "fs.list: open child dir failed; skipping");
                        continue;
                    }
                };
                let child_prefix = entries
                    .last()
                    .map(|e| e.relative.clone())
                    .unwrap_or_default();
                queue.push_back((child_dir, child_prefix, depth + 1));
            }
        }

        if truncated {
            break;
        }
    }

    (entries, truncated, skipped_non_utf8)
}
