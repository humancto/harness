//! `llm.cloud.claude` — Anthropic Messages API capability.
//!
//! Same input/output contract as `llm.local.<model>` (3.4) so callers
//! can swap a tag without rewriting their plan template. Uses the
//! shared [`LlmBatcher`] (3.5) for dedup-within-window. Policy gate is
//! `Action::Llm { model: &input.model }` — same shape as `llm.local`,
//! so an operator's `[llm].deny = ["claude-3-opus"]` rule applies.
//!
//! ## Anthropic API contract
//!
//! Verified against the Messages API documentation cached on
//! 2026-04-15:
//!
//! - Endpoint: `POST {base_url}/messages` (default `base_url`
//!   `https://api.anthropic.com/v1/`).
//! - Headers: `x-api-key`, `anthropic-version: 2023-06-01`,
//!   `content-type: application/json`.
//! - Request body: `{"model","messages":[{"role":"user","content":...}],"max_tokens",...}`.
//! - Response: `content: [{"type":"text","text":...}, ...]` plus
//!   `usage: {"input_tokens","output_tokens"}`.
//!
//! Future-revisitation date documented in ADR-0012.

#![allow(clippy::items_after_statements)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use harness_core::protocol::{
    CostHint, CpuClass, DiskIoClass, NetworkClass, RateLimit, ResourceHints,
};
use harness_core::{Capability as ManifestEntry, Cardinality, SemVer};
use harness_policy::{Action, Decision, EvalContext, PolicyEngine};
use harness_vault::{SecretValue, SecretsStore};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use url::Url;

use crate::llm_batcher::{Fingerprint, LlmBatcher};
use crate::traits::{Capability, CapabilityError, ExecutionContext};

pub const ID: &str = "llm.cloud.claude";

/// Tag the capability reads from the vault.
pub const SECRET_TAG: &str = "secret/claude-api-key";

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
/// Cap shorter than `llm.local` (600s) — a cloud network call hung for
/// ten minutes is almost certainly a wedged connection, not legitimate
/// inference. Operators who need longer can increase via input.
const MAX_TIMEOUT_MS: u64 = 120_000;
const MIN_TIMEOUT_MS: u64 = 100;
/// Cap higher than `llm.local` (4096) — Anthropic's Sonnet/Opus models
/// support 8K+ output tokens; bound at 16K to refuse pathological calls.
const MAX_TOKENS_CAP: u64 = 16_000;
const DEFAULT_MAX_TOKENS: u64 = 1024;

/// Default upstream — overridden by tests via [`LlmCloudClaudeCapability::with_base_url`].
pub const DEFAULT_BASE_URL: &str = "https://api.anthropic.com/v1/";

/// `llm.cloud.claude` capability — one per daemon (model lives in the
/// task input, not the capability id).
#[derive(Clone, Debug)]
pub struct LlmCloudClaudeCapability {
    secrets: Arc<dyn SecretsStore>,
    policy: Arc<PolicyEngine>,
    batcher: Arc<LlmBatcher>,
    client: reqwest::Client,
    base_url: Url,
}

impl LlmCloudClaudeCapability {
    /// Build with the production Anthropic base URL.
    ///
    /// # Panics
    /// Panics only if [`DEFAULT_BASE_URL`] fails to parse, which is a
    /// build-time invariant (literal-string parse).
    #[must_use]
    pub fn new(
        secrets: Arc<dyn SecretsStore>,
        policy: Arc<PolicyEngine>,
        batcher: Arc<LlmBatcher>,
        client: reqwest::Client,
    ) -> Self {
        #[allow(clippy::expect_used)]
        let base_url = Url::parse(DEFAULT_BASE_URL).expect("DEFAULT_BASE_URL parses");
        Self::with_base_url(secrets, policy, batcher, client, base_url)
    }

    /// Test-friendly constructor: explicit base URL (used to point at a
    /// `wiremock::MockServer`).
    #[must_use]
    pub fn with_base_url(
        secrets: Arc<dyn SecretsStore>,
        policy: Arc<PolicyEngine>,
        batcher: Arc<LlmBatcher>,
        client: reqwest::Client,
        base_url: Url,
    ) -> Self {
        Self {
            secrets,
            policy,
            batcher,
            client,
            base_url,
        }
    }
}

// FINGERPRINT_FIELDS — the canonical fingerprint must include every
// output-affecting field. Adding `top_p`, `tool_choice`, etc. here in
// the future REQUIRES updating `fingerprint_for`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaudeInput {
    model: String,
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

#[async_trait]
impl Capability for LlmCloudClaudeCapability {
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
            cardinality: Cardinality::Anyone,
            input_schema: json!({
                "type": "object",
                "required": ["model", "prompt"],
                "additionalProperties": false,
                "properties": {
                    "model":       { "type": "string", "minLength": 1 },
                    "prompt":      { "type": "string", "minLength": 1 },
                    "system":      { "type": "string" },
                    "temperature": { "type": "number", "minimum": 0, "maximum": 2 },
                    "max_tokens":  { "type": "integer", "minimum": 1, "maximum": MAX_TOKENS_CAP },
                    "timeout_ms":  { "type": "integer", "minimum": MIN_TIMEOUT_MS, "maximum": MAX_TIMEOUT_MS },
                },
            }),
            output_schema: json!({
                "type": "object",
                "required": ["text", "model", "duration_ms"],
                "properties": {
                    "text":              { "type": "string" },
                    "model":             { "type": "string" },
                    "duration_ms":       { "type": "integer" },
                    "prompt_tokens":     { "type": ["integer", "null"] },
                    "completion_tokens": { "type": ["integer", "null"] },
                    "cost_usd":          { "type": ["number", "null"] },
                },
            }),
            cost_hint: CostHint::CloudPaid,
            tags: vec!["llm".to_string(), "cloud".to_string(), "claude".to_string()],
            // Anthropic tier limits vary by plan; pick a defensive
            // conservative default. Operators on paid tiers should raise
            // via configuration when that lever ships (3.6-encrypted /
            // ADR-0012 follow-up).
            rate_limit: Some(RateLimit {
                per_second: 1,
                burst: 5,
            }),
            resource_hints: ResourceHints {
                cpu_class: CpuClass::Light,
                memory_mb: None,
                gpu_required: false,
                gpu_memory_mb: None,
                // No `Wan` variant in `NetworkClass` (None/Light/Heavy
                // only). `Heavy` is the right scheduler hint for a
                // cloud round-trip — it costs real WAN bandwidth and
                // is bound by upstream rate limits.
                network_class: NetworkClass::Heavy,
                disk_io_class: DiskIoClass::None,
                estimated_duration_ms: None,
            },
            requires_secrets: vec![SECRET_TAG.to_string()],
        }
    }

    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: JsonValue,
    ) -> Result<JsonValue, CapabilityError> {
        let input: ClaudeInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;
        validate_input(&input)?;

        // Policy gate. Same shape as llm.local — operators write
        // `[llm].deny = ["claude-3-opus-..."]` and the same rule applies
        // here.
        let decision = self.policy.evaluate(&EvalContext {
            from_node: ctx.issued_by_name.as_ref(),
            local_node: ctx.local_node_name.as_ref(),
            action: Action::Llm {
                model: &input.model,
            },
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

        // Resolve the API key. We do not log it under any circumstances.
        let api_key = self.secrets.get(SECRET_TAG).ok_or_else(|| {
            CapabilityError::Failed(format!(
                "{SECRET_TAG} not configured \
                 (set in ~/.harness/secrets.toml or HARNESS_SECRET_CLAUDE_API_KEY)"
            ))
        })?;

        // Phase 3.5: tag:interactive bypasses the batcher.
        let interactive = ctx.tags.iter().any(|t| t == "interactive");
        if interactive {
            return dispatch_claude(&self.client, &self.base_url, &api_key, input).await;
        }

        let fp = fingerprint_for(&input);
        let client = self.client.clone();
        let base_url = self.base_url.clone();
        // Cancellation safety: the batcher's spawned timer task owns
        // `dispatch_claude` after submit. The interactive path above is
        // a single-await on dispatch with no partial-side-effect
        // recovery to worry about.
        self.batcher
            .submit(fp, move || async move {
                dispatch_claude(&client, &base_url, &api_key, input).await
            })
            .await
    }
}

fn validate_input(input: &ClaudeInput) -> Result<(), CapabilityError> {
    if input.model.trim().is_empty() {
        return Err(CapabilityError::InvalidInput("model is empty".to_string()));
    }
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

/// Hash `(provider, model, prompt, system, temperature, max_tokens)`
/// into a [`Fingerprint`] for the batcher. `provider` is pinned at
/// `"claude"` so a future provider with the same model name can never
/// coalesce. `timeout_ms` is excluded (wait-bound, not output-affecting).
fn fingerprint_for(input: &ClaudeInput) -> Fingerprint {
    let canonical = json!({
        "provider":    "claude",
        "model":       input.model,
        "prompt":      input.prompt,
        "system":      input.system,
        "temperature": input.temperature,
        "max_tokens":  input.max_tokens,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    Fingerprint(*blake3::hash(&bytes).as_bytes())
}

async fn dispatch_claude(
    client: &reqwest::Client,
    base_url: &Url,
    api_key: &SecretValue,
    input: ClaudeInput,
) -> Result<JsonValue, CapabilityError> {
    let url = base_url
        .join("messages")
        .map_err(|e| CapabilityError::Failed(format!("bad base_url: {e}")))?;

    // `HeaderValue::from_bytes` preserves bytes verbatim. A `from_str`
    // path would lossily UTF-8-decode and could silently corrupt a
    // non-ASCII key. We propagate the parse error so a malformed key
    // returns `Failed("api key contains invalid header bytes")` rather
    // than panicking the daemon's hot path.
    let mut auth = reqwest::header::HeaderValue::from_bytes(api_key.as_bytes()).map_err(|_| {
        CapabilityError::Failed("api key contains invalid header bytes".to_string())
    })?;
    // Mark the header value as sensitive so axum/hyper redact it in
    // any generic header-dumping path the request happens to traverse.
    auth.set_sensitive(true);

    let mut body = json!({
        "model": &input.model,
        "messages": [{"role": "user", "content": input.prompt}],
        "max_tokens": input.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
    });
    if let Some(s) = &input.system {
        body["system"] = json!(s);
    }
    if let Some(t) = input.temperature {
        body["temperature"] = json!(t);
    }

    let started = Instant::now();
    let resp = client
        .post(url)
        .header("x-api-key", auth)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .timeout(Duration::from_millis(
            input.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
        ))
        .send()
        .await
        .map_err(|e| CapabilityError::Failed(format!("anthropic api unreachable: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(CapabilityError::Failed(format!(
            "anthropic returned {status}: {text}"
        )));
    }

    #[derive(Deserialize)]
    struct AnthropicResp {
        #[serde(default)]
        content: Vec<ContentBlock>,
        #[serde(default)]
        usage: Option<Usage>,
    }
    #[derive(Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum ContentBlock {
        Text {
            text: String,
        },
        // Other variants (tool_use, image, ...) are silently ignored.
        // 3.6-tool-use will give them their own variants.
        #[serde(other)]
        Other,
    }
    #[derive(Deserialize)]
    struct Usage {
        #[serde(default)]
        input_tokens: Option<u64>,
        #[serde(default)]
        output_tokens: Option<u64>,
    }
    let r: AnthropicResp = resp
        .json()
        .await
        .map_err(|e| CapabilityError::Failed(format!("decode anthropic response: {e}")))?;

    let text: String = r
        .content
        .into_iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text),
            ContentBlock::Other => None,
        })
        .collect();

    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    // 5.9 (ADR-0037): price the call from provider-reported usage.
    // Unknown model or absent usage → null, never a guess.
    let pt = r.usage.as_ref().and_then(|u| u.input_tokens);
    let ct = r.usage.as_ref().and_then(|u| u.output_tokens);
    let cost_usd = match (pt, ct) {
        (Some(p), Some(c)) => harness_cost::price_usd(&input.model, p, c),
        _ => None,
    };
    Ok(json!({
        "text":              text,
        "model":             input.model,
        "duration_ms":       duration_ms,
        "prompt_tokens":     pt,
        "completion_tokens": ct,
        "cost_usd":          cost_usd,
    }))
}

/// Register the single `llm.cloud.claude` capability in `registry`.
/// Idempotent only for fresh registries: a duplicate `register` call
/// panics with `BUG: enrich_with_llm_cloud_claude called twice`,
/// matching the `enrich_with_llm_local` pattern.
///
/// `client` is shared with the `llm.local.*` execution client to reuse
/// the connection pool. `policy` and `batcher` are likewise shared.
pub fn enrich_with_llm_cloud_claude(
    registry: &crate::registry::CapabilityRegistry,
    secrets: Arc<dyn SecretsStore>,
    policy: Arc<PolicyEngine>,
    batcher: Arc<LlmBatcher>,
    client: reqwest::Client,
) {
    let cap = LlmCloudClaudeCapability::new(secrets, policy, batcher, client);
    #[allow(clippy::expect_used)]
    registry
        .register(Arc::new(cap))
        .expect("BUG: enrich_with_llm_cloud_claude called twice");
}
