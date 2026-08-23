//! `llm.cloud.openai` — `OpenAI` Chat Completions API capability.
//!
//! Mechanical mirror of `llm.cloud.claude` (3.6a / ADR-0012): same
//! input/output contract as `llm.local.<model>` so callers can swap a
//! tag without rewriting their plan template, same shared
//! [`LlmBatcher`] dedup-within-window, same `Action::Llm` policy gate
//! so an operator's `[llm].deny = ["gpt-4o"]` rule applies here too.
//!
//! One deliberate delta from 3.6a: `model` is optional in the input
//! and defaults to [`DEFAULT_MODEL`] (`gpt-4o-mini`). The resolved
//! model still feeds the policy evaluator and the batcher fingerprint.
//!
//! ## `OpenAI` API contract
//!
//! Verified against the Chat Completions API documentation cached on
//! 2026-08-22:
//!
//! - Endpoint: `POST {base_url}/chat/completions` (default `base_url`
//!   `https://api.openai.com/v1/`).
//! - Headers: `Authorization: Bearer <key>`,
//!   `content-type: application/json`.
//! - Request body: `{"model","messages":[{"role":"user","content":...}],
//!   "max_completion_tokens",...}` (`max_completion_tokens` is the
//!   current name; the legacy `max_tokens` field is deprecated and
//!   rejected by o-series models).
//! - Response: `choices: [{"message":{"content":...}}, ...]` plus
//!   `usage: {"prompt_tokens","completion_tokens"}`.
//!
//! ## Cost assumption
//!
//! `CostHint::CloudPaid`. Pricing assumption documented for future
//! cost-tracking integration (v2 §17.8): the default model
//! `gpt-4o-mini` bills ~$0.15 per 1M input tokens and ~$0.60 per 1M
//! output tokens (public list price as of 2026-08). Larger SKUs
//! (`gpt-4o`, o-series) cost 10-100x more; real-time per-model pricing
//! tables are a `harness-cost` concern, not baked in here.
//!
//! Future-revisitation date discipline follows ADR-0012.

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

pub const ID: &str = "llm.cloud.openai";

/// Tag the capability reads from the vault.
pub const SECRET_TAG: &str = "secret/openai-api-key";

/// Model used when the input omits `model`.
pub const DEFAULT_MODEL: &str = "gpt-4o-mini";

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
/// Cap shorter than `llm.local` (600s) — a cloud network call hung for
/// ten minutes is almost certainly a wedged connection, not legitimate
/// inference. Operators who need longer can increase via input.
const MAX_TIMEOUT_MS: u64 = 120_000;
const MIN_TIMEOUT_MS: u64 = 100;
/// Same cap as `llm.cloud.claude` — `gpt-4o-mini` supports 16K output
/// tokens; bound at 16K to refuse pathological calls.
const MAX_TOKENS_CAP: u64 = 16_000;
const DEFAULT_MAX_TOKENS: u64 = 1024;

/// Default upstream — overridden by tests via
/// [`LlmCloudOpenaiCapability::with_base_url`].
pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1/";

/// `llm.cloud.openai` capability — one per daemon (model lives in the
/// task input, not the capability id — see ADR-0012).
#[derive(Clone, Debug)]
pub struct LlmCloudOpenaiCapability {
    secrets: Arc<dyn SecretsStore>,
    policy: Arc<PolicyEngine>,
    batcher: Arc<LlmBatcher>,
    client: reqwest::Client,
    base_url: Url,
}

impl LlmCloudOpenaiCapability {
    /// Build with the production `OpenAI` base URL.
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
struct OpenaiInput {
    #[serde(default)]
    model: Option<String>,
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

impl OpenaiInput {
    fn resolved_model(&self) -> &str {
        self.model.as_deref().unwrap_or(DEFAULT_MODEL)
    }
}

#[async_trait]
impl Capability for LlmCloudOpenaiCapability {
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
                "required": ["prompt"],
                "additionalProperties": false,
                "properties": {
                    "model":       { "type": "string", "minLength": 1,
                                     "description": "OpenAI model SKU; defaults to gpt-4o-mini" },
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
            tags: vec!["llm".to_string(), "cloud".to_string(), "openai".to_string()],
            // OpenAI tier limits vary by plan; pick a defensive
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
        let input: OpenaiInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;
        validate_input(&input)?;

        // Policy gate. Same shape as llm.local — operators write
        // `[llm].deny = ["gpt-4o"]` and the same rule applies here.
        let decision = self.policy.evaluate(&EvalContext {
            from_node: ctx.issued_by_name.as_ref(),
            local_node: ctx.local_node_name.as_ref(),
            action: Action::Llm {
                model: input.resolved_model(),
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
                 (set in ~/.harness/secrets.toml or HARNESS_SECRET_OPENAI_API_KEY)"
            ))
        })?;

        // Phase 3.5: tag:interactive bypasses the batcher.
        let interactive = ctx.tags.iter().any(|t| t == "interactive");
        if interactive {
            return dispatch_openai(&self.client, &self.base_url, &api_key, input).await;
        }

        let fp = fingerprint_for(&input);
        let client = self.client.clone();
        let base_url = self.base_url.clone();
        // Cancellation safety: the batcher's spawned timer task owns
        // `dispatch_openai` after submit. The interactive path above is
        // a single-await on dispatch with no partial-side-effect
        // recovery to worry about.
        self.batcher
            .submit(fp, move || async move {
                dispatch_openai(&client, &base_url, &api_key, input).await
            })
            .await
    }
}

fn validate_input(input: &OpenaiInput) -> Result<(), CapabilityError> {
    if let Some(m) = &input.model {
        if m.trim().is_empty() {
            return Err(CapabilityError::InvalidInput("model is empty".to_string()));
        }
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
/// `"openai"` so a different provider with the same model name can
/// never coalesce. The *resolved* model is hashed so an explicit
/// `"gpt-4o-mini"` and an omitted model (same default) do coalesce.
/// `timeout_ms` is excluded (wait-bound, not output-affecting).
fn fingerprint_for(input: &OpenaiInput) -> Fingerprint {
    let canonical = json!({
        "provider":    "openai",
        "model":       input.resolved_model(),
        "prompt":      input.prompt,
        "system":      input.system,
        "temperature": input.temperature,
        "max_tokens":  input.max_tokens,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    Fingerprint(*blake3::hash(&bytes).as_bytes())
}

async fn dispatch_openai(
    client: &reqwest::Client,
    base_url: &Url,
    api_key: &SecretValue,
    input: OpenaiInput,
) -> Result<JsonValue, CapabilityError> {
    let url = base_url
        .join("chat/completions")
        .map_err(|e| CapabilityError::Failed(format!("bad base_url: {e}")))?;

    // `HeaderValue::from_bytes` preserves bytes verbatim (see 3.6a
    // rationale — a `from_str` path could silently corrupt a
    // non-ASCII key). We propagate the parse error so a malformed key
    // returns `Failed("api key contains invalid header bytes")` rather
    // than panicking the daemon's hot path.
    let mut bearer = Vec::with_capacity("Bearer ".len() + api_key.as_bytes().len());
    bearer.extend_from_slice(b"Bearer ");
    bearer.extend_from_slice(api_key.as_bytes());
    let mut auth = reqwest::header::HeaderValue::from_bytes(&bearer).map_err(|_| {
        CapabilityError::Failed("api key contains invalid header bytes".to_string())
    })?;
    // Mark the header value as sensitive so axum/hyper redact it in
    // any generic header-dumping path the request happens to traverse.
    auth.set_sensitive(true);

    let model = input.resolved_model().to_string();
    let mut messages = Vec::new();
    if let Some(s) = &input.system {
        messages.push(json!({"role": "system", "content": s}));
    }
    messages.push(json!({"role": "user", "content": input.prompt}));

    let mut body = json!({
        "model": model,
        "messages": messages,
        "max_completion_tokens": input.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
    });
    if let Some(t) = input.temperature {
        body["temperature"] = json!(t);
    }

    let started = Instant::now();
    let resp = client
        .post(url)
        .header(reqwest::header::AUTHORIZATION, auth)
        .header("content-type", "application/json")
        .json(&body)
        .timeout(Duration::from_millis(
            input.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
        ))
        .send()
        .await
        .map_err(|e| CapabilityError::Failed(format!("openai api unreachable: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(CapabilityError::Failed(format!(
            "openai returned {status}: {text}"
        )));
    }

    #[derive(Deserialize)]
    struct OpenaiResp {
        #[serde(default)]
        choices: Vec<Choice>,
        #[serde(default)]
        usage: Option<Usage>,
    }
    #[derive(Deserialize)]
    struct Choice {
        #[serde(default)]
        message: Option<Message>,
    }
    #[derive(Deserialize)]
    struct Message {
        // `content` is `null` for pure tool-call responses; 3.6-tool-use
        // will give those their own handling.
        #[serde(default)]
        content: Option<String>,
    }
    #[derive(Deserialize)]
    struct Usage {
        #[serde(default)]
        prompt_tokens: Option<u64>,
        #[serde(default)]
        completion_tokens: Option<u64>,
    }
    let r: OpenaiResp = resp
        .json()
        .await
        .map_err(|e| CapabilityError::Failed(format!("decode openai response: {e}")))?;

    let text: String = r
        .choices
        .into_iter()
        .filter_map(|c| c.message.and_then(|m| m.content))
        .collect();

    let model_out = input.resolved_model().to_string();
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    // 5.9 (ADR-0037): price from provider-reported usage; null when
    // the model is unpriced or usage is absent.
    let pt = r.usage.as_ref().and_then(|u| u.prompt_tokens);
    let ct = r.usage.as_ref().and_then(|u| u.completion_tokens);
    let cost_usd = match (pt, ct) {
        (Some(p), Some(c)) => harness_cost::price_usd(&model_out, p, c),
        _ => None,
    };
    Ok(json!({
        "text":              text,
        "model":             model_out,
        "duration_ms":       duration_ms,
        "prompt_tokens":     pt,
        "completion_tokens": ct,
        "cost_usd":          cost_usd,
    }))
}

/// Register the single `llm.cloud.openai` capability in `registry`.
/// Idempotent only for fresh registries: a duplicate `register` call
/// panics with `BUG: enrich_with_llm_cloud_openai called twice`,
/// matching the `enrich_with_llm_cloud_claude` pattern.
///
/// `client` and `batcher` are shared with the other cloud providers to
/// reuse the connection pool; the fingerprint pins the provider so
/// cross-provider coalescing is impossible.
pub fn enrich_with_llm_cloud_openai(
    registry: &crate::registry::CapabilityRegistry,
    secrets: Arc<dyn SecretsStore>,
    policy: Arc<PolicyEngine>,
    batcher: Arc<LlmBatcher>,
    client: reqwest::Client,
) {
    let cap = LlmCloudOpenaiCapability::new(secrets, policy, batcher, client);
    #[allow(clippy::expect_used)]
    registry
        .register(Arc::new(cap))
        .expect("BUG: enrich_with_llm_cloud_openai called twice");
}
