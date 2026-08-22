//! `fs.grep` — bounded regex / literal scan under a scope. No index —
//! streams files through the cap-std `Dir` walk (ADR-0016 §4).

use std::io::Read;
use std::sync::Arc;

use async_trait::async_trait;
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, Cardinality, SemVer};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::fs::scope::ScopeRegistry;
use crate::fs::walk::{glob_matches, glob_to_regex, looks_binary, walk_files, BINARY_SNIFF_BYTES};
use crate::traits::{Capability, CapabilityError, ExecutionContext};

pub const ID: &str = "fs.grep";
pub const SCOPE_FIELD: &str = "scope";

const DEFAULT_MAX_RESULTS: u32 = 100;
pub const HARD_MAX_RESULTS: u32 = 1_000;
/// Match lines longer than this are truncated (UTF-8-boundary safe).
pub const MAX_LINE_BYTES: usize = 512;
/// Files larger than this are skipped entirely (counted).
pub const MAX_SCAN_FILE_BYTES: u64 = 8 * 1024 * 1024;

/// `fs.grep` — regex / literal pattern match over scope files.
/// Owner cardinality (`scope_field: "scope"`), same policy posture as
/// `fs.read`: confinement is the scope registry itself; no separate
/// policy hook (ADR-0015 §2, ADR-0016 §6).
#[derive(Debug, Clone)]
pub struct FsGrepCapability {
    scopes: Arc<ScopeRegistry>,
}

impl FsGrepCapability {
    #[must_use]
    pub fn new(scopes: Arc<ScopeRegistry>) -> Self {
        Self { scopes }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FsGrepInput {
    scope: String,
    pattern: String,
    /// Treat `pattern` as a literal string instead of a regex.
    #[serde(default)]
    literal: bool,
    #[serde(default)]
    ignore_case: bool,
    #[serde(default)]
    max_results: Option<u32>,
    /// Shell-style glob filter over relative paths (`*.rs`,
    /// `src/**/*.toml`). No `/` in the glob → matches file names.
    #[serde(default)]
    file_glob: Option<String>,
}

#[derive(Debug, Serialize)]
struct FsGrepOutput {
    scope: String,
    pattern: String,
    matches: Vec<MatchDto>,
    /// `true` when the match cap, the walk bound, or a depth cut-off
    /// elided results.
    truncated: bool,
    files_scanned: u32,
    files_skipped_binary: u32,
    files_skipped_too_large: u32,
}

#[derive(Debug, Serialize)]
struct MatchDto {
    /// Relative to the scope root, `/`-separated. Never absolute.
    path: String,
    /// 1-based.
    line_number: u32,
    /// Match line, lossy-decoded, truncated to [`MAX_LINE_BYTES`].
    line: String,
    line_truncated: bool,
}

#[async_trait]
impl Capability for FsGrepCapability {
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
                "required": [SCOPE_FIELD, "pattern"],
                "additionalProperties": false,
                "properties": {
                    "scope":       { "type": "string", "minLength": 1 },
                    "pattern":     { "type": "string", "minLength": 1 },
                    "literal":     { "type": "boolean" },
                    "ignore_case": { "type": "boolean" },
                    "max_results": { "type": "integer", "minimum": 1, "maximum": HARD_MAX_RESULTS },
                    "file_glob":   { "type": "string", "minLength": 1 },
                },
            }),
            output_schema: json!({
                "type": "object",
                "required": ["scope", "pattern", "matches", "truncated"],
                "properties": {
                    "scope":     { "type": "string" },
                    "pattern":   { "type": "string" },
                    "matches":   { "type": "array" },
                    "truncated": { "type": "boolean" },
                    "files_scanned":           { "type": "integer", "minimum": 0 },
                    "files_skipped_binary":    { "type": "integer", "minimum": 0 },
                    "files_skipped_too_large": { "type": "integer", "minimum": 0 },
                },
            }),
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
        let input: FsGrepInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;

        let scope = self.scopes.get(&input.scope).ok_or_else(|| {
            CapabilityError::InvalidInput(format!("unknown scope {:?}", input.scope))
        })?;

        if input.pattern.is_empty() {
            return Err(CapabilityError::InvalidInput("empty pattern".to_string()));
        }

        let pattern_src = if input.literal {
            regex::escape(&input.pattern)
        } else {
            input.pattern.clone()
        };
        let matcher = regex::RegexBuilder::new(&pattern_src)
            .case_insensitive(input.ignore_case)
            .build()
            .map_err(|e| CapabilityError::InvalidInput(format!("invalid pattern: {e}")))?;

        let glob = match &input.file_glob {
            Some(g) => {
                let re = glob_to_regex(g, false).map_err(|e| {
                    CapabilityError::InvalidInput(format!("invalid file_glob: {e}"))
                })?;
                Some((re, g.contains('/')))
            }
            None => None,
        };

        let max_results = input
            .max_results
            .map_or(DEFAULT_MAX_RESULTS, |v| v.min(HARD_MAX_RESULTS));

        // The walk + regex scan is synchronous disk-bound work — run it
        // on the blocking pool so the executor's async worker threads
        // stay responsive.
        let scope_id = input.scope.clone();
        let pattern_echo = input.pattern.clone();
        let out = tokio::task::spawn_blocking(move || {
            grep_scope(&scope.root_dir, &matcher, glob.as_ref(), max_results)
        })
        .await
        .map_err(|e| CapabilityError::Failed(format!("grep task join: {e}")))?;

        let out = FsGrepOutput {
            scope: scope_id,
            pattern: pattern_echo,
            matches: out.matches,
            truncated: out.truncated,
            files_scanned: out.files_scanned,
            files_skipped_binary: out.files_skipped_binary,
            files_skipped_too_large: out.files_skipped_too_large,
        };
        serde_json::to_value(&out)
            .map_err(|e| CapabilityError::Failed(format!("encode output: {e}")))
    }
}

struct GrepResult {
    matches: Vec<MatchDto>,
    truncated: bool,
    files_scanned: u32,
    files_skipped_binary: u32,
    files_skipped_too_large: u32,
}

fn grep_scope(
    root: &cap_std::fs::Dir,
    matcher: &regex::Regex,
    glob: Option<&(regex::Regex, bool)>,
    max_results: u32,
) -> GrepResult {
    let mut found: Vec<MatchDto> = Vec::new();
    let mut files_scanned: u32 = 0;
    let mut files_skipped_binary: u32 = 0;
    let mut files_skipped_too_large: u32 = 0;
    let mut hit_cap = false;

    let stats = walk_files(root, |file| {
        if let Some((re, has_slash)) = glob {
            if !glob_matches(re, *has_slash, file.relative) {
                return true;
            }
        }
        if file.metadata.len() > MAX_SCAN_FILE_BYTES {
            files_skipped_too_large = files_skipped_too_large.saturating_add(1);
            return true;
        }
        // metadata.is_file() held at walk time; a racing swap to a
        // FIFO between stat and open re-resolves under cap-std and, in
        // the worst case, fails the read — never escapes the scope.
        let fh = match file.parent.open(file.name) {
            Ok(f) => f,
            Err(err) => {
                tracing::debug!(?err, path = %file.relative, "fs.grep: open failed; skipping");
                return true;
            }
        };
        let mut buf: Vec<u8> =
            Vec::with_capacity(usize::try_from(file.metadata.len()).unwrap_or(0));
        // +1 sentinel: a file that grew past the cap since stat is
        // detected and skipped rather than scanned half-way.
        if let Err(err) = fh.take(MAX_SCAN_FILE_BYTES + 1).read_to_end(&mut buf) {
            tracing::debug!(?err, path = %file.relative, "fs.grep: read failed; skipping");
            return true;
        }
        if buf.len() as u64 > MAX_SCAN_FILE_BYTES {
            files_skipped_too_large = files_skipped_too_large.saturating_add(1);
            return true;
        }
        if looks_binary(&buf[..buf.len().min(BINARY_SNIFF_BYTES)]) {
            files_skipped_binary = files_skipped_binary.saturating_add(1);
            return true;
        }
        files_scanned = files_scanned.saturating_add(1);
        if buf.is_empty() {
            return true; // no lines — even `.*` must not match an empty file
        }
        // Strip one trailing newline so the phantom empty segment after
        // the final `\n` is not treated as a matchable line.
        let content = buf.strip_suffix(b"\n").unwrap_or(&buf);

        for (idx, raw_line) in content.split(|b| *b == b'\n').enumerate() {
            let raw_line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
            let line = String::from_utf8_lossy(raw_line);
            if !matcher.is_match(&line) {
                continue;
            }
            let (text, line_truncated) = truncate_line(&line);
            // Files are ≤ 8 MiB so line counts fit u32 comfortably.
            let line_number = u32::try_from(idx.saturating_add(1)).unwrap_or(u32::MAX);
            found.push(MatchDto {
                path: file.relative.to_string(),
                line_number,
                line: text,
                line_truncated,
            });
            if found.len() as u64 >= u64::from(max_results) {
                hit_cap = true;
                return false; // stop the walk
            }
        }
        true
    });

    GrepResult {
        matches: found,
        truncated: hit_cap || stats.truncated,
        files_scanned,
        files_skipped_binary,
        files_skipped_too_large,
    }
}

/// Truncate to [`MAX_LINE_BYTES`] on a char boundary.
fn truncate_line(line: &str) -> (String, bool) {
    if line.len() <= MAX_LINE_BYTES {
        return (line.to_string(), false);
    }
    let mut cut = MAX_LINE_BYTES;
    while cut > 0 && !line.is_char_boundary(cut) {
        cut -= 1;
    }
    (line[..cut].to_string(), true)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn truncate_line_respects_char_boundary() {
        let s = "é".repeat(300); // 600 bytes of 2-byte chars
        let (t, truncated) = truncate_line(&s);
        assert!(truncated);
        assert!(t.len() <= MAX_LINE_BYTES);
        assert!(t.is_char_boundary(t.len()));
    }

    #[test]
    fn truncate_line_short_passthrough() {
        let (t, truncated) = truncate_line("short");
        assert_eq!(t, "short");
        assert!(!truncated);
    }
}
