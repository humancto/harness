//! `Cloud` planner backend (5.2, PRD §15.1 tier 3, ADR-0031) —
//! Anthropic Messages API, escalation-only by lineup position.
//!
//! The cloud tier reuses the whole LLM-planner pipeline from
//! [`crate::llm_common`] (prompt, JSON extraction, response rewrite);
//! only the transport differs from the local tiers:
//! `POST {base}/messages` with `x-api-key` + `anthropic-version`
//! headers, and the plan JSON arrives inside `content[].text` blocks.
//!
//! Escalation gating (the policy-driven rules, evaluated per request
//! BEFORE any I/O): `!constraints.allow_cloud` or
//! `constraints.must_be_local` → [`PlanOutcome::NoMatch`] — a gated
//! cloud tier is a clean skip, not an error. The `allow_cloud`
//! constraint reaching this backend already encodes policy approval
//! AND the per-task `cloud_ok` opt-in (enforced by the `brain.plan`
//! executor; ADR-0031).
//!
//! The API key never lives on this struct: a [`CloudKeyProvider`]
//! closure (the daemon closes over its vault) is invoked per request
//! and returns a ready, `set_sensitive(true)` [`HeaderValue`] — no
//! owned key bytes cross the crate boundary, and the key is NEVER
//! logged (the missing-key diagnostic names the secret TAG only).

#![allow(clippy::items_after_statements)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use harness_core::NodeId;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use crate::backend::{PlanOutcome, PlanRequest, PlannerBackend};
use crate::error::PlannerError;
use crate::llm_common::{build_prompt, build_response, extract_json_object, LlmPlanResponse};

// Re-exported so the daemon can construct header values for the
// provider closure without adding its own reqwest/http dependency.
pub use reqwest::header::HeaderValue;

/// Per-request API-key source. Returns a ready-to-send header value
/// (built from the vault secret with `set_sensitive(true)` already
/// applied) or `None` when the key is unconfigured or malformed.
pub type CloudKeyProvider = Arc<dyn Fn() -> Option<HeaderValue> + Send + Sync>;

/// Matches the `llm.cloud.claude` capability's request default; the
/// CLI plan budget (240 s as of 5.2) outer-bounds the full chain.
const CLOUD_TIMEOUT_MS: u64 = 60_000;
/// Same fuller projection as `LocalStrong` — frontier models afford it.
const CLOUD_PROMPT_BYTE_CAP: usize = 16 * 1024;
/// Response budget. Plan JSON is compact, but on current models the
/// (always-on, adaptive) thinking tokens count against `max_tokens`
/// too — a tight cap can truncate the plan mid-object. 16k leaves
/// ample headroom and stays well inside the 60 s timeout (diff
/// review MINOR-2).
const CLOUD_MAX_TOKENS: u64 = 16_384;
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// 5.2 `Cloud` backend (tier 3). Anthropic-only in 5.2 (ADR-0031).
#[derive(Clone)]
pub struct CloudBackend {
    base_url: Url,
    client: reqwest::Client,
    model: String,
    local_node: NodeId,
    /// `"cloud:<model>"`. Stored once so `id()` returns `&str` cheaply.
    id: String,
    key_provider: CloudKeyProvider,
}

// Manual impl: `PlannerBackend: Debug`, but the provider closure is
// not — and must never be — printable.
impl std::fmt::Debug for CloudBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudBackend")
            .field("base_url", &self.base_url.as_str())
            .field("model", &self.model)
            .field("id", &self.id)
            .field("key_provider", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl CloudBackend {
    /// Construct a `Cloud` backend.
    ///
    /// # Errors
    /// Returns `Err(reqwest::Error)` if the underlying HTTP client
    /// builder fails (rare; surfaces TLS-init / OS-resource issues).
    pub fn new(
        base_url: Url,
        model: String,
        local_node: NodeId,
        key_provider: CloudKeyProvider,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(60))
            .build()?;
        let id = format!("cloud:{model}");
        Ok(Self {
            base_url,
            client,
            model,
            local_node,
            id,
            key_provider,
        })
    }
}

#[async_trait]
impl PlannerBackend for CloudBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        // Escalation gate — before any I/O, before touching the key.
        if !req.constraints.allow_cloud || req.constraints.must_be_local {
            return Ok(PlanOutcome::NoMatch);
        }

        let Some(mut api_key) = (self.key_provider)() else {
            return Err(PlannerError::Internal(
                "cloud planner: secret/claude-api-key not configured or malformed \
                 (set in ~/.harness/secrets.toml or HARNESS_SECRET_CLAUDE_API_KEY)"
                    .to_string(),
            ));
        };
        // Defense-in-depth: the daemon's provider already marks the
        // value sensitive; re-assert in case of a custom provider.
        api_key.set_sensitive(true);

        let prompt = build_prompt(req, CLOUD_PROMPT_BYTE_CAP);
        let url = self
            .base_url
            .join("messages")
            .map_err(|e| PlannerError::Internal(format!("bad anthropic base url: {e}")))?;
        // NO sampling parameters (`temperature`/`top_p`/`top_k`):
        // current Anthropic models reject them with HTTP 400 (diff
        // review BLOCKER-1 — a hardcoded temperature would have made
        // every cloud attempt fail into Template). t01 pins their
        // absence.
        let body = json!({
            "model": &self.model,
            "max_tokens": CLOUD_MAX_TOKENS,
            "messages": [{"role": "user", "content": prompt}],
        });

        let resp = self
            .client
            .post(url)
            .header("x-api-key", api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&body)
            .timeout(Duration::from_millis(CLOUD_TIMEOUT_MS))
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    PlannerError::Timeout
                } else {
                    PlannerError::Transport(format!("anthropic api unreachable: {e}"))
                }
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(PlannerError::Transport(format!(
                "anthropic returned {status}: {text}"
            )));
        }

        #[derive(Deserialize)]
        struct AnthropicResp {
            #[serde(default)]
            content: Vec<ContentBlock>,
        }
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum ContentBlock {
            Text {
                text: String,
            },
            // tool_use, thinking, ... — irrelevant to plan extraction.
            #[serde(other)]
            Other,
        }
        let r: AnthropicResp = resp
            .json()
            .await
            .map_err(|e| PlannerError::Decode(format!("decode anthropic envelope: {e}")))?;

        let text: String = r
            .content
            .into_iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text),
                ContentBlock::Other => None,
            })
            .collect();

        let json_text = extract_json_object(&text).ok_or_else(|| {
            PlannerError::Decode("no JSON object found in LLM response".to_string())
        })?;

        let llm: LlmPlanResponse = serde_json::from_str(json_text)
            .map_err(|e| PlannerError::Decode(format!("decode plan response: {e}")))?;

        let response = build_response(llm, self.local_node)?;
        Ok(PlanOutcome::Confident(Box::new(response)))
    }
}
