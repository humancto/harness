//! `fs.search` — FTS5 full-text query over a scope's sidecar index.
//! See ADR-0016.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, Cardinality, SemVer};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::fs::index::{self, IndexError, IndexStats, SearchHit};
use crate::fs::scope::ScopeRegistry;
use crate::traits::{Capability, CapabilityError, ExecutionContext};

pub const ID: &str = "fs.search";
pub const SCOPE_FIELD: &str = "scope";

const DEFAULT_LIMIT: u32 = 20;
pub const HARD_MAX_LIMIT: u32 = 100;

/// `fs.search` — bm25-ranked FTS5 MATCH over the per-scope sidecar
/// index (`<index_dir>/<scope>.fts.db`). Owner cardinality
/// (`scope_field: "scope"`), same policy posture as `fs.read`
/// (ADR-0015 §2, ADR-0016 §6).
///
/// The index is built lazily on the first query and refreshed
/// incrementally (mtime-compared) when the caller passes
/// `reindex: true`.
#[derive(Debug, Clone)]
pub struct FsSearchCapability {
    scopes: Arc<ScopeRegistry>,
    index_dir: PathBuf,
}

impl FsSearchCapability {
    /// `index_dir` is where sidecar DBs live — the daemon passes
    /// `<harness_root>/index`.
    #[must_use]
    pub fn new(scopes: Arc<ScopeRegistry>, index_dir: PathBuf) -> Self {
        Self { scopes, index_dir }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FsSearchInput {
    scope: String,
    /// FTS5 MATCH syntax (bare terms, `"phrase"`, `AND`/`OR`/`NOT`,
    /// `prefix*`).
    query: String,
    #[serde(default)]
    limit: Option<u32>,
    /// `true` → refresh the index (incremental, mtime-compared) before
    /// querying. Default `false`: query the existing index as-is
    /// (built lazily if missing).
    #[serde(default)]
    reindex: bool,
}

#[derive(Debug, Serialize)]
struct FsSearchOutput {
    scope: String,
    query: String,
    hits: Vec<SearchHit>,
    /// Whether this call ran an index pass (lazy first build or
    /// `reindex: true`).
    reindexed: bool,
    /// Present when `reindexed` — counters from the index pass.
    #[serde(skip_serializing_if = "Option::is_none")]
    index_stats: Option<IndexStats>,
}

#[async_trait]
impl Capability for FsSearchCapability {
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
            // Same forward-compat routing story as fs.list / fs.read —
            // ADR-0015 §2.
            cardinality: Cardinality::Owner {
                scope_field: SCOPE_FIELD.to_string(),
            },
            input_schema: json!({
                "type": "object",
                "required": [SCOPE_FIELD, "query"],
                "additionalProperties": false,
                "properties": {
                    "scope":   { "type": "string", "minLength": 1 },
                    "query":   { "type": "string", "minLength": 1 },
                    "limit":   { "type": "integer", "minimum": 1, "maximum": HARD_MAX_LIMIT },
                    "reindex": { "type": "boolean" },
                },
            }),
            output_schema: json!({
                "type": "object",
                "required": ["scope", "query", "hits", "reindexed"],
                "properties": {
                    "scope":       { "type": "string" },
                    "query":       { "type": "string" },
                    "hits":        { "type": "array" },
                    "reindexed":   { "type": "boolean" },
                    "index_stats": { "type": "object" },
                },
            }),
            // First call on a big scope pays an index build; steady
            // state is an indexed lookup. LocalFast still holds for the
            // common case; the build is bounded (4 MiB/file, 100k files).
            cost_hint: CostHint::LocalFast,
            tags: vec!["fs".to_string()],
            rate_limit: Some(RateLimit {
                per_second: 5,
                burst: 10,
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
        let input: FsSearchInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;

        let scope = self.scopes.get(&input.scope).ok_or_else(|| {
            CapabilityError::InvalidInput(format!("unknown scope {:?}", input.scope))
        })?;

        if input.query.is_empty() {
            return Err(CapabilityError::InvalidInput("empty query".to_string()));
        }
        let limit = input.limit.map_or(DEFAULT_LIMIT, |v| v.min(HARD_MAX_LIMIT));

        let db_path = index::index_db_path(&self.index_dir, &input.scope);
        let want_reindex = input.reindex;
        let query_str = input.query.clone();

        // Index build + FTS query are synchronous SQLite work — run on
        // the blocking pool.
        let (hits, reindexed, stats) =
            tokio::task::spawn_blocking(move || -> Result<_, IndexError> {
                // Lazy first build: no sidecar DB (or an empty one from
                // an interrupted first run) → build now.
                let needs_build =
                    want_reindex || index::indexed_file_count(&db_path)?.unwrap_or(0) == 0;
                let stats = if needs_build {
                    Some(index::reindex(&scope, &db_path)?)
                } else {
                    None
                };
                let hits = index::query(&db_path, &query_str, limit)?;
                Ok((hits, needs_build, stats))
            })
            .await
            .map_err(|e| CapabilityError::Failed(format!("search task join: {e}")))?
            .map_err(map_index_err)?;

        let out = FsSearchOutput {
            scope: input.scope,
            query: input.query,
            hits,
            reindexed,
            index_stats: stats,
        };
        serde_json::to_value(&out)
            .map_err(|e| CapabilityError::Failed(format!("encode output: {e}")))
    }
}

fn map_index_err(e: IndexError) -> CapabilityError {
    match e {
        // fts5 MATCH syntax error — the caller's query is malformed.
        IndexError::Query(inner) => {
            CapabilityError::InvalidInput(format!("fts5 query error: {inner}"))
        }
        other => CapabilityError::Failed(other.to_string()),
    }
}
