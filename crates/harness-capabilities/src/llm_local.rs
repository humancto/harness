//! `llm.local.<model>` — Phase 3.4. One capability per locally-installed
//! Ollama model, discovered via `GET {OLLAMA_HOST}/api/tags` at daemon
//! startup. Sync request/response only (streaming is a future
//! enhancement; the parent roadmap text doesn't promise it).
//!
//! Discovery and execution use the same HTTP transport (`OLLAMA_HOST`)
//! so there's exactly one config and one set of failure modes. The
//! subprocess `ollama list` parse path was rejected because:
//! - `ollama list` output isn't a stable contract.
//! - HTTP `/api/tags` returns structured JSON with the canonical model
//!   names that `/api/generate` expects.
//! - Daemon launched via launchd may not have `ollama` on PATH but will
//!   still reach `127.0.0.1:11434`.
//!
//! See ADR-0010 for the policy and DoS-mitigation rationale.

#![allow(
    clippy::result_unit_err,
    clippy::items_after_statements,
    clippy::unreadable_literal
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, Cardinality, SemVer};
use harness_policy::{Action, Decision, EvalContext, PolicyEngine};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use url::Url;

use crate::traits::{Capability, CapabilityError, ExecutionContext};

pub const ID_PREFIX: &str = "llm.local.";

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MIN_TIMEOUT_MS: u64 = 100;
const MAX_TOKENS_CAP: u64 = 4096;
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(2);

/// One LLM capability backed by a locally-installed Ollama model.
///
/// The capability id is `format!("llm.local.{model}")` — the colon in
/// model tags like `"llama3:latest"` is preserved verbatim and lands
/// in the registry as a string key.
///
/// `client` is shared across all capabilities created from a single
/// `enrich_with_llm_local` call. `reqwest::Client::clone()` shares the
/// underlying connection pool — see reqwest docs.
#[derive(Clone, Debug)]
pub struct LlmLocalCapability {
    model: String,
    id: String,
    policy: Arc<PolicyEngine>,
    host: Url,
    client: reqwest::Client,
    batcher: Arc<crate::llm_batcher::LlmBatcher>,
}

impl LlmLocalCapability {
    #[must_use]
    pub fn new(
        policy: Arc<PolicyEngine>,
        model: String,
        host: Url,
        client: reqwest::Client,
        batcher: Arc<crate::llm_batcher::LlmBatcher>,
    ) -> Self {
        let id = format!("{ID_PREFIX}{model}");
        Self {
            model,
            id,
            policy,
            host,
            client,
            batcher,
        }
    }
}

// FINGERPRINT_FIELDS — every output-affecting field listed below must
// also appear in `fingerprint_for` so the batcher correctly distinguishes
// requests with different output. Adding `seed`, `top_p`, `top_k`, etc.
// in the future REQUIRES updating both this struct AND `fingerprint_for`.
// `timeout_ms` is excluded (a wait-bound, not output-determining).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct LlmLocalInput {
    prompt: String,
    #[serde(default)]
    system: Option<String>,
    #[serde(default)]
    temperature: Option<f64>,
    #[serde(default)]
    max_tokens: Option<u64>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct LlmLocalOutput {
    text: String,
    model: String,
    duration_ms: u64,
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
}

#[async_trait]
impl Capability for LlmLocalCapability {
    fn id(&self) -> &str {
        &self.id
    }

    fn manifest(&self) -> ManifestEntry {
        ManifestEntry {
            id: self.id.clone(),
            version: SemVer {
                major: 0,
                minor: 1,
                patch: 0,
            },
            cardinality: Cardinality::Anyone,
            input_schema: serde_json::json!({
                "type": "object",
                "required": ["prompt"],
                "additionalProperties": false,
                "properties": {
                    "prompt":      { "type": "string", "minLength": 1 },
                    "system":      { "type": "string" },
                    "temperature": { "type": "number", "minimum": 0, "maximum": 2 },
                    "max_tokens":  { "type": "integer", "minimum": 1, "maximum": 4096 },
                    "timeout_ms":  { "type": "integer", "minimum": 100, "maximum": 600000 },
                },
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "required": ["text", "model", "duration_ms"],
                "properties": {
                    "text":              { "type": "string" },
                    "model":             { "type": "string" },
                    "duration_ms":       { "type": "integer" },
                    "prompt_tokens":     { "type": ["integer", "null"] },
                    "completion_tokens": { "type": ["integer", "null"] },
                },
            }),
            cost_hint: CostHint::LocalSlow,
            tags: vec!["llm".to_string(), "local".to_string()],
            // R1: tighter than shell.exec because LLM inference is
            // expensive. An operator who wants more reconfigures.
            rate_limit: Some(RateLimit {
                per_second: 1,
                burst: 2,
            }),
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Heavy,
                memory_mb: Some(8192),
                // Ollama auto-falls-back to CPU. Lying via `true` would
                // mis-route the scheduler in 3.5+ on CPU-only nodes.
                gpu_required: false,
                gpu_memory_mb: None,
                network_class: NetworkClass::None,
                disk_io_class: DiskIoClass::Light,
                estimated_duration_ms: None,
            },
        }
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        let input: LlmLocalInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;
        validate_input(&input)?;

        // Policy gate. Inline against the engine — same pattern as shell.
        let decision = self.policy.evaluate(&EvalContext {
            from_node: ctx.issued_by_name.as_ref(),
            local_node: ctx.local_node_name.as_ref(),
            action: Action::Llm { model: &self.model },
        });
        match decision {
            Decision::Allow => {}
            Decision::Deny { reason } => {
                return Err(CapabilityError::Failed(format!("policy denied: {reason}")));
            }
            _ => {
                return Err(CapabilityError::Failed(
                    "policy returned unknown decision (fail-closed)".to_string(),
                ));
            }
        }

        // Phase 3.5: tag:interactive bypasses the batcher. Otherwise
        // submit to the batcher; identical (model, fingerprint) calls
        // within the window coalesce into one backend invocation.
        let interactive = ctx.tags.iter().any(|t| t == "interactive");
        if interactive {
            return dispatch_direct_owned(
                self.model.clone(),
                self.host.clone(),
                self.client.clone(),
                input,
            )
            .await;
        }

        let fp = fingerprint_for(&self.model, &input);
        // Capture for the dispatch closure: clone the cheap bits so the
        // closure has 'static lifetime and can be moved into the
        // batcher's spawned timer task.
        let model = self.model.clone();
        let host = self.host.clone();
        let client = self.client.clone();
        self.batcher
            .submit(fp, move || async move {
                dispatch_direct_owned(model, host, client, input).await
            })
            .await
    }
}

fn validate_input(input: &LlmLocalInput) -> Result<(), CapabilityError> {
    if input.prompt.trim().is_empty() {
        return Err(CapabilityError::InvalidInput("prompt is empty".to_string()));
    }
    if let Some(t) = input.max_tokens {
        if !(1..=MAX_TOKENS_CAP).contains(&t) {
            return Err(CapabilityError::InvalidInput(format!(
                "max_tokens must be in 1..={MAX_TOKENS_CAP}"
            )));
        }
    }
    if let Some(t) = input.timeout_ms {
        if !(MIN_TIMEOUT_MS..=MAX_TIMEOUT_MS).contains(&t) {
            return Err(CapabilityError::InvalidInput(format!(
                "timeout_ms must be in {MIN_TIMEOUT_MS}..={MAX_TIMEOUT_MS}"
            )));
        }
    }
    Ok(())
}

/// Compute the batcher fingerprint for `(model, input)`. Two callers
/// with the same fingerprint share a backend call.
///
/// **Hand-listed fields** — see the `// FINGERPRINT_FIELDS` marker on
/// `LlmLocalInput`. `timeout_ms` is excluded (wait-bound, not output-
/// determining); `tags` is on `ExecutionContext`, not the input.
fn fingerprint_for(model: &str, input: &LlmLocalInput) -> crate::llm_batcher::Fingerprint {
    // Fixed-shape `json!` literal: serializing produces deterministic
    // bytes per build. The `serde_json::Map` underlying `json!` is
    // BTreeMap-backed unless `preserve_order` is enabled (we don't
    // enable it in the workspace), so key order is deterministic.
    let canonical = serde_json::json!({
        "model":       model,
        "prompt":      input.prompt,
        "system":      input.system,
        "temperature": input.temperature,
        "max_tokens":  input.max_tokens,
    });
    // Encoding a fixed-shape json! literal cannot fail in practice
    // (serde_json::Value is always serializable; the only failure mode
    // is non-string Map keys, which we don't have). Use unwrap_or to
    // produce a degenerate-but-deterministic empty hash if it ever
    // does — siblings still coalesce, and the output is wrong for
    // exactly that pathological input. Better than panicking the
    // daemon's hot path.
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    let hash = blake3::hash(&bytes);
    crate::llm_batcher::Fingerprint(*hash.as_bytes())
}

/// `dispatch_direct` extracted to a free fn so the batcher's spawned
/// task can call it without borrowing `self`. All inputs owned.
async fn dispatch_direct_owned(
    model: String,
    host: Url,
    client: reqwest::Client,
    input: LlmLocalInput,
) -> Result<JsonValue, CapabilityError> {
    let timeout = Duration::from_millis(input.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));
    let url = host
        .join("api/generate")
        .map_err(|e| CapabilityError::Failed(format!("bad ollama host: {e}")))?;

    let body = serde_json::json!({
        "model":   &model,
        "prompt":  input.prompt,
        "stream":  false,
        "options": ollama_options(&input),
    });

    let started = Instant::now();
    let resp = client
        .post(url)
        .json(&body)
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| CapabilityError::Failed(format!("ollama serve not reachable: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(CapabilityError::Failed(format!(
            "ollama returned {status}: {text}"
        )));
    }

    #[derive(Deserialize)]
    struct OllamaResp {
        response: String,
        #[serde(default)]
        prompt_eval_count: Option<u64>,
        #[serde(default)]
        eval_count: Option<u64>,
    }
    let r: OllamaResp = resp
        .json()
        .await
        .map_err(|e| CapabilityError::Failed(format!("decode ollama response: {e}")))?;

    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let out = LlmLocalOutput {
        text: r.response,
        model,
        duration_ms,
        prompt_tokens: r.prompt_eval_count,
        completion_tokens: r.eval_count,
    };
    serde_json::to_value(&out).map_err(|e| CapabilityError::Failed(format!("encode output: {e}")))
}

fn ollama_options(input: &LlmLocalInput) -> JsonValue {
    let mut opts = serde_json::Map::new();
    if let Some(t) = input.temperature {
        opts.insert("temperature".to_string(), JsonValue::from(t));
    }
    if let Some(m) = input.max_tokens {
        opts.insert("num_predict".to_string(), JsonValue::from(m));
    }
    if let Some(s) = &input.system {
        opts.insert("system".to_string(), JsonValue::from(s.clone()));
    }
    JsonValue::Object(opts)
}

/// Discover locally-available Ollama models via `GET /api/tags`.
/// Best-effort: any failure (network, decode, bad status) returns an
/// empty Vec and the daemon continues without `llm.local.*` caps.
pub async fn discover_local_models(host: &Url, client: &reqwest::Client) -> Vec<String> {
    let Ok(url) = host.join("api/tags") else {
        return Vec::new();
    };
    let resp = match client.get(url).timeout(DISCOVERY_TIMEOUT).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            tracing::info!(
                target: "harness.llm.local",
                status = %r.status(),
                "ollama /api/tags returned non-2xx"
            );
            return Vec::new();
        }
        Err(e) => {
            tracing::info!(
                target: "harness.llm.local",
                ?e,
                "ollama unreachable; no llm.local.* caps will register"
            );
            return Vec::new();
        }
    };

    #[derive(Deserialize)]
    struct Tags {
        #[serde(default)]
        models: Vec<TagEntry>,
    }
    #[derive(Deserialize)]
    struct TagEntry {
        name: String,
    }

    match resp.json::<Tags>().await {
        Ok(t) => t
            .models
            .into_iter()
            .map(|e| e.name)
            .filter(|n| !n.is_empty())
            .collect(),
        Err(e) => {
            tracing::warn!(target: "harness.llm.local", ?e, "decode /api/tags");
            Vec::new()
        }
    }
}

/// Parse `OLLAMA_HOST` into a `Url`. Accepts:
/// - `<http://host:port>` (full URL)
/// - `host:port` (defaults to `http://`)
/// - `host` (defaults to `http://host:11434`)
///
/// Returns `Err(())` for malformed input. Caller decides fallback.
pub fn parse_ollama_host(s: &str) -> Result<Url, ()> {
    let s = s.trim();
    if s.is_empty() {
        return Err(());
    }
    let with_scheme = if s.contains("://") {
        s.to_string()
    } else {
        format!("http://{s}")
    };
    let mut url = Url::parse(&with_scheme).map_err(|_| ())?;
    if url.port().is_none() {
        url.set_port(Some(11434))?;
    }
    Ok(url)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn parse_full_url() {
        let u = parse_ollama_host("http://example.com:9999").unwrap();
        assert_eq!(u.scheme(), "http");
        assert_eq!(u.host_str(), Some("example.com"));
        assert_eq!(u.port(), Some(9999));
    }

    #[test]
    fn parse_host_port() {
        let u = parse_ollama_host("example.com:9999").unwrap();
        assert_eq!(u.scheme(), "http");
        assert_eq!(u.port(), Some(9999));
    }

    #[test]
    fn parse_host_only_uses_default_port() {
        let u = parse_ollama_host("example.com").unwrap();
        assert_eq!(u.port(), Some(11434));
    }

    #[test]
    fn parse_empty_returns_err() {
        assert!(parse_ollama_host("").is_err());
        assert!(parse_ollama_host("   ").is_err());
    }

    #[test]
    fn parse_malformed_returns_err() {
        assert!(parse_ollama_host("http://").is_err());
    }
}
