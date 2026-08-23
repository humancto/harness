//! `LocalFast` planner backend (Phase 3.9, PRD §15.1 tier 1) —
//! Ollama-backed, sync `/api/generate`, ~5-30s per goal.
//!
//! Pipeline (helpers shared with the Cloud tier via [`crate::llm_common`]):
//! 1. `build_prompt` — byte-stable prompt with capability list
//!    (id + required-fields projection), constraints, two worked
//!    examples, and the user goal at the end. 8KB byte cap with
//!    "always-include" pin + sorted-id truncation.
//! 2. POST `/api/generate` with `{model, prompt, stream: false}`.
//! 3. [`extract_json_object`] — brace-balanced JSON extractor that
//!    tolerates prose-prefix, prose-suffix, fenced/unfenced output,
//!    and multi-block (first wins).
//! 4. `serde_json::from_str::<LlmPlanResponse>` (strict; trailing
//!    commas → `Decode` error → executor escalates).
//! 5. Server-side rewrite: mint fresh `TaskId`s, rewrite edges through
//!    a `String → TaskId` map, FLIP orientation (LLM emits `(A, B)`
//!    meaning "A runs before B"; harness emits `(from, to)` meaning
//!    "from depends on to" — so we emit `(B, A)`).
//!
//! Confidence is whatever the LLM reports, clamped to `[0.0, 1.0]`.
//! `estimated_cost_usd` defaults to `0.0` (Ollama is local; the
//! capability-execution layer charges nothing). `confidence_threshold`
//! enforcement lives at the brain.plan executor, NOT here.

#![allow(clippy::items_after_statements)]

use std::time::Duration;

use async_trait::async_trait;
use harness_core::NodeId;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use crate::backend::{PlanOutcome, PlanRequest, PlannerBackend};
use crate::error::PlannerError;
use crate::llm_common::{build_prompt, build_response, LlmPlanResponse};
// Public path preserved from 3.9 (the wiremock suite and downstream
// users import it from here); the implementation moved in 5.2.
pub use crate::llm_common::extract_json_object;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Byte cap on the prompt sent to Ollama. Bytes (not tokens) for
/// byte-stable cache friendliness; ~2300 tokens at 3.5 chars/token.
const PROMPT_BYTE_CAP: usize = 8 * 1024;

/// 5.1 (ADR-0030): tier-2 request timeout. A 32B–70B model on consumer
/// hardware streams slowly; the CLI's plan budget (240 s as of 5.2)
/// outer-bounds the whole escalation chain with room for Template.
const STRONG_TIMEOUT_MS: u64 = 120_000;
/// 5.1: strong models afford a fuller capability projection.
const STRONG_PROMPT_BYTE_CAP: usize = 16 * 1024;

/// Shared Ollama-backed planner core (5.1, ADR-0030): `LocalFast`
/// (tier 1) and `LocalStrong` (tier 2) differ ONLY in id prefix,
/// request timeout, and prompt byte cap — one implementation, two
/// thin public wrappers.
#[derive(Debug, Clone)]
struct LocalLlmCore {
    host: Url,
    client: reqwest::Client,
    model: String,
    local_node: NodeId,
    /// `"<tier>:<model>"`. Stored once at construction so `id()`
    /// returns `&str` cheaply.
    id: String,
    timeout_ms: u64,
    prompt_byte_cap: usize,
}

impl LocalLlmCore {
    fn new(
        host: Url,
        model: String,
        local_node: NodeId,
        id_prefix: &str,
        timeout_ms: u64,
        prompt_byte_cap: usize,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(60))
            .build()?;
        let id = format!("{id_prefix}:{model}");
        Ok(Self {
            host,
            client,
            model,
            local_node,
            id,
            timeout_ms,
            prompt_byte_cap,
        })
    }
}

/// Phase 3.9 `LocalFast` backend (tier 1).
#[derive(Debug, Clone)]
pub struct LocalFastBackend(LocalLlmCore);

impl LocalFastBackend {
    /// Construct a `LocalFast` backend.
    ///
    /// # Errors
    /// Returns `Err(reqwest::Error)` if the underlying HTTP client
    /// builder fails (rare; surfaces TLS-init / OS-resource issues).
    pub fn new(host: Url, model: String, local_node: NodeId) -> Result<Self, reqwest::Error> {
        LocalLlmCore::new(
            host,
            model,
            local_node,
            "localfast",
            DEFAULT_TIMEOUT_MS,
            PROMPT_BYTE_CAP,
        )
        .map(Self)
    }
}

/// 5.1 `LocalStrong` backend (tier 2, 32B–70B class). Same Ollama
/// plumbing as tier 1; slower budget, fuller prompt.
#[derive(Debug, Clone)]
pub struct LocalStrongBackend(LocalLlmCore);

impl LocalStrongBackend {
    /// Construct a `LocalStrong` backend.
    ///
    /// # Errors
    /// Returns `Err(reqwest::Error)` if the underlying HTTP client
    /// builder fails (rare; surfaces TLS-init / OS-resource issues).
    pub fn new(host: Url, model: String, local_node: NodeId) -> Result<Self, reqwest::Error> {
        LocalLlmCore::new(
            host,
            model,
            local_node,
            "localstrong",
            STRONG_TIMEOUT_MS,
            STRONG_PROMPT_BYTE_CAP,
        )
        .map(Self)
    }
}

#[async_trait]
impl PlannerBackend for LocalFastBackend {
    fn id(&self) -> &str {
        &self.0.id
    }
    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        self.0.plan(req).await
    }
}

#[async_trait]
impl PlannerBackend for LocalStrongBackend {
    fn id(&self) -> &str {
        &self.0.id
    }
    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        self.0.plan(req).await
    }
}

impl LocalLlmCore {
    async fn plan(&self, req: &PlanRequest) -> Result<PlanOutcome, PlannerError> {
        let prompt = build_prompt(req, self.prompt_byte_cap);
        let url = self
            .host
            .join("api/generate")
            .map_err(|e| PlannerError::Internal(format!("bad ollama host: {e}")))?;
        let body = json!({
            "model":  &self.model,
            "prompt": prompt,
            "stream": false,
        });

        let resp = self
            .client
            .post(url)
            .json(&body)
            .timeout(Duration::from_millis(self.timeout_ms))
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    PlannerError::Timeout
                } else {
                    PlannerError::Transport(format!("ollama unreachable: {e}"))
                }
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(PlannerError::Transport(format!(
                "ollama returned {status}: {text}"
            )));
        }

        #[derive(Deserialize)]
        struct OllamaResp {
            response: String,
        }
        let r: OllamaResp = resp
            .json()
            .await
            .map_err(|e| PlannerError::Decode(format!("decode /api/generate envelope: {e}")))?;

        let json_text = extract_json_object(&r.response).ok_or_else(|| {
            PlannerError::Decode("no JSON object found in LLM response".to_string())
        })?;

        let llm: LlmPlanResponse = serde_json::from_str(json_text)
            .map_err(|e| PlannerError::Decode(format!("decode plan response: {e}")))?;

        let response = build_response(llm, self.local_node)?;
        Ok(PlanOutcome::Confident(Box::new(response)))
    }
}
