//! `llm.cloud.gemini` — Google Generative Language API capability.
//!
//! Mechanical mirror of `llm.cloud.claude` (3.6a / ADR-0012): same
//! input/output contract as `llm.local.<model>` so callers can swap a
//! tag without rewriting their plan template, same shared
//! [`LlmBatcher`] dedup-within-window, same `Action::Llm` policy gate
//! so an operator's `[llm].deny = ["gemini-2.0-pro"]` rule applies.
//!
//! One deliberate delta from 3.6a: `model` is optional in the input
//! and defaults to [`DEFAULT_MODEL`] (`gemini-2.0-flash`). Because the
//! model name is interpolated into the request *path* (unlike
//! Claude/OpenAI where it rides in the JSON body), it is additionally
//! validated against `[A-Za-z0-9._-]+` before URL construction —
//! rejecting path traversal / query-string injection outright.
//!
//! ## Google API contract
//!
//! Verified against the Generative Language API documentation cached
//! on 2026-08-22:
//!
//! - Endpoint: `POST {base_url}/models/<model>:generateContent`
//!   (default `base_url`
//!   `https://generativelanguage.googleapis.com/v1beta/`).
//! - Auth: `x-goog-api-key: <key>` header. The API also accepts
//!   `?key=<KEY>` as a query parameter; we deliberately use the header
//!   form — query strings leak into access logs, proxies, and
//!   `Display` impls of URLs, while a sensitive-marked header does not.
//! - Request body: `{"contents":[{"role":"user","parts":[{"text":...}]}],
//!   "systemInstruction":{"parts":[{"text":...}]},
//!   "generationConfig":{"temperature","maxOutputTokens"}}`.
//! - Response: `candidates: [{"content":{"parts":[{"text":...}]}}]`
//!   plus `usageMetadata: {"promptTokenCount","candidatesTokenCount"}`.
//!
//! ## Cost assumption
//!
//! `CostHint::CloudPaid`. Pricing assumption documented for future
//! cost-tracking integration (v2 §17.8): the default model
//! `gemini-2.0-flash` bills ~$0.10 per 1M input tokens and ~$0.40 per
//! 1M output tokens (public list price as of 2026-08). Pro-tier SKUs
//! cost 10x+ more; real-time per-model pricing tables are a
//! `harness-cost` concern, not baked in here.
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

pub const ID: &str = "llm.cloud.gemini";

/// Tag the capability reads from the vault.
pub const SECRET_TAG: &str = "secret/gemini-api-key";

/// Model used when the input omits `model`.
pub const DEFAULT_MODEL: &str = "gemini-2.0-flash";

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
/// Cap shorter than `llm.local` (600s) — a cloud network call hung for
/// ten minutes is almost certainly a wedged connection, not legitimate
/// inference. Operators who need longer can increase via input.
const MAX_TIMEOUT_MS: u64 = 120_000;
const MIN_TIMEOUT_MS: u64 = 100;
/// Same cap as `llm.cloud.claude` (16K). Gemini 2.0 Flash tops out at
/// 8192 output tokens today; the API clamps server-side, and the cap
/// here exists to refuse pathological caller requests, not to track
/// per-SKU maxima.
const MAX_TOKENS_CAP: u64 = 16_000;
const DEFAULT_MAX_TOKENS: u64 = 1024;

/// Default upstream — overridden by tests via
/// [`LlmCloudGeminiCapability::with_base_url`].
pub const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta/";

/// `llm.cloud.gemini` capability — one per daemon (model lives in the
/// task input, not the capability id — see ADR-0012).
#[derive(Clone, Debug)]
pub struct LlmCloudGeminiCapability {
    secrets: Arc<dyn SecretsStore>,
    policy: Arc<PolicyEngine>,
    batcher: Arc<LlmBatcher>,
    client: reqwest::Client,
    base_url: Url,
}

impl LlmCloudGeminiCapability {
    /// Build with the production Google base URL.
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
struct GeminiInput {
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

impl GeminiInput {
    fn resolved_model(&self) -> &str {
        self.model.as_deref().unwrap_or(DEFAULT_MODEL)
    }
}

/// The model name rides in the URL path — restrict it to a charset
/// that cannot terminate the path (`?`, `#`), climb it (`/`, `..` is
/// harmless without `/`), or smuggle a scheme (`:`).
fn model_name_is_safe(model: &str) -> bool {
    !model.is_empty()
        && model
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

#[async_trait]
impl Capability for LlmCloudGeminiCapability {
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
                                     "pattern": "^[A-Za-z0-9._-]+$",
                                     "description": "Gemini model SKU; defaults to gemini-2.0-flash" },
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
            tags: vec!["llm".to_string(), "cloud".to_string(), "gemini".to_string()],
            // Google tier limits vary by plan; pick a defensive
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
        let input: GeminiInput = serde_json::from_value(input)
            .map_err(|e| CapabilityError::InvalidInput(format!("decode input: {e}")))?;
        validate_input(&input)?;

        // Policy gate. Same shape as llm.local — operators write
        // `[llm].deny = ["gemini-2.0-pro"]` and the same rule applies
        // here.
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
                 (set in ~/.harness/secrets.toml or HARNESS_SECRET_GEMINI_API_KEY)"
            ))
        })?;

        // Phase 3.5: tag:interactive bypasses the batcher.
        let interactive = ctx.tags.iter().any(|t| t == "interactive");
        if interactive {
            return dispatch_gemini(&self.client, &self.base_url, &api_key, input).await;
        }

        let fp = fingerprint_for(&input);
        let client = self.client.clone();
        let base_url = self.base_url.clone();
        // Cancellation safety: the batcher's spawned timer task owns
        // `dispatch_gemini` after submit. The interactive path above is
        // a single-await on dispatch with no partial-side-effect
        // recovery to worry about.
        self.batcher
            .submit(fp, move || async move {
                dispatch_gemini(&client, &base_url, &api_key, input).await
            })
            .await
    }
}

fn validate_input(input: &GeminiInput) -> Result<(), CapabilityError> {
    if let Some(m) = &input.model {
        if !model_name_is_safe(m) {
            return Err(CapabilityError::InvalidInput(
                "model must match [A-Za-z0-9._-]+ (it is embedded in the request path)".to_string(),
            ));
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
/// `"gemini"` so a different provider with the same model name can
/// never coalesce. The *resolved* model is hashed so an explicit
/// `"gemini-2.0-flash"` and an omitted model (same default) do
/// coalesce. `timeout_ms` is excluded (wait-bound, not
/// output-affecting).
fn fingerprint_for(input: &GeminiInput) -> Fingerprint {
    let canonical = json!({
        "provider":    "gemini",
        "model":       input.resolved_model(),
        "prompt":      input.prompt,
        "system":      input.system,
        "temperature": input.temperature,
        "max_tokens":  input.max_tokens,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    Fingerprint(*blake3::hash(&bytes).as_bytes())
}

async fn dispatch_gemini(
    client: &reqwest::Client,
    base_url: &Url,
    api_key: &SecretValue,
    input: GeminiInput,
) -> Result<JsonValue, CapabilityError> {
    let model = input.resolved_model().to_string();
    // Defense-in-depth: `validate_input` already rejected unsafe model
    // names; re-check here because this function is reachable from the
    // batcher closure and the invariant is cheap to re-assert.
    if !model_name_is_safe(&model) {
        return Err(CapabilityError::InvalidInput(
            "model must match [A-Za-z0-9._-]+ (it is embedded in the request path)".to_string(),
        ));
    }
    let url = base_url
        .join(&format!("models/{model}:generateContent"))
        .map_err(|e| CapabilityError::Failed(format!("bad base_url: {e}")))?;

    // `HeaderValue::from_bytes` preserves bytes verbatim (see 3.6a
    // rationale). We propagate the parse error so a malformed key
    // returns `Failed("api key contains invalid header bytes")` rather
    // than panicking the daemon's hot path. Header auth is used instead
    // of the documented `?key=` query parameter so the key never
    // appears in a URL (URLs leak into logs; sensitive headers don't).
    let mut auth = reqwest::header::HeaderValue::from_bytes(api_key.as_bytes()).map_err(|_| {
        CapabilityError::Failed("api key contains invalid header bytes".to_string())
    })?;
    // Mark the header value as sensitive so axum/hyper redact it in
    // any generic header-dumping path the request happens to traverse.
    auth.set_sensitive(true);

    let mut body = json!({
        "contents": [{"role": "user", "parts": [{"text": input.prompt}]}],
        "generationConfig": {
            "maxOutputTokens": input.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        },
    });
    if let Some(s) = &input.system {
        body["systemInstruction"] = json!({"parts": [{"text": s}]});
    }
    if let Some(t) = input.temperature {
        body["generationConfig"]["temperature"] = json!(t);
    }

    let started = Instant::now();
    let resp = client
        .post(url)
        .header("x-goog-api-key", auth)
        .header("content-type", "application/json")
        .json(&body)
        .timeout(Duration::from_millis(
            input.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
        ))
        .send()
        .await
        .map_err(|e| CapabilityError::Failed(format!("gemini api unreachable: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(CapabilityError::Failed(format!(
            "gemini returned {status}: {text}"
        )));
    }

    let r: GeminiResp = resp
        .json()
        .await
        .map_err(|e| CapabilityError::Failed(format!("decode gemini response: {e}")))?;

    // First candidate only — the API returns one unless candidateCount
    // is set (we never set it).
    let text: String = r
        .candidates
        .into_iter()
        .next()
        .and_then(|c| c.content)
        .map(|c| {
            c.parts
                .into_iter()
                .filter_map(|p| p.text)
                .collect::<String>()
        })
        .unwrap_or_default();

    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    // 5.9 (ADR-0037): price from provider-reported usage; null when
    // the model is unpriced or usage is absent.
    let pt = r.usage_metadata.as_ref().and_then(|u| u.prompt_token_count);
    let ct = r
        .usage_metadata
        .as_ref()
        .and_then(|u| u.candidates_token_count);
    let cost_usd = match (pt, ct) {
        (Some(p), Some(c)) => harness_cost::price_usd(&model, p, c),
        _ => None,
    };
    Ok(json!({
        "text":              text,
        "model":             model,
        "duration_ms":       duration_ms,
        "prompt_tokens":     pt,
        "completion_tokens": ct,
        "cost_usd":          cost_usd,
    }))
}

// Response DTOs for `generateContent` — module-private, decode-only.
#[derive(Deserialize)]
struct GeminiResp {
    #[serde(default)]
    candidates: Vec<Candidate>,
    #[serde(default)]
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<UsageMetadata>,
}
#[derive(Deserialize)]
struct Candidate {
    #[serde(default)]
    content: Option<Content>,
}
#[derive(Deserialize)]
struct Content {
    #[serde(default)]
    parts: Vec<Part>,
}
#[derive(Deserialize)]
struct Part {
    // Non-text parts (inlineData, functionCall, ...) decode with
    // `text: None` and are silently ignored. 3.6-tool-use will
    // give them their own variants.
    #[serde(default)]
    text: Option<String>,
}
#[derive(Deserialize)]
struct UsageMetadata {
    #[serde(default)]
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: Option<u64>,
    #[serde(default)]
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: Option<u64>,
}

/// Register the single `llm.cloud.gemini` capability in `registry`.
/// Idempotent only for fresh registries: a duplicate `register` call
/// panics with `BUG: enrich_with_llm_cloud_gemini called twice`,
/// matching the `enrich_with_llm_cloud_claude` pattern.
///
/// `client` and `batcher` are shared with the other cloud providers to
/// reuse the connection pool; the fingerprint pins the provider so
/// cross-provider coalescing is impossible.
pub fn enrich_with_llm_cloud_gemini(
    registry: &crate::registry::CapabilityRegistry,
    secrets: Arc<dyn SecretsStore>,
    policy: Arc<PolicyEngine>,
    batcher: Arc<LlmBatcher>,
    client: reqwest::Client,
) {
    let cap = LlmCloudGeminiCapability::new(secrets, policy, batcher, client);
    #[allow(clippy::expect_used)]
    registry
        .register(Arc::new(cap))
        .expect("BUG: enrich_with_llm_cloud_gemini called twice");
}
