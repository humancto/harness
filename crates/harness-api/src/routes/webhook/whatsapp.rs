//! `POST /webhook/whatsapp` — WhatsApp-via-Twilio adapter (5.5,
//! ADR-0033, PRD §20.2 + §26 walkthrough).
//!
//! Flow: validate the Twilio signature (fail-closed) → sender
//! allowlist (deny-all by default) → admission gate → mint a
//! `brain.plan` task from the message body → reply `TwiML` ack
//! in-channel (zero outbound API calls) → detached driver polls the
//! STORE, mints `plan.execute` from the planned JSON, and sends the
//! final reply through the Twilio Messages API when an account SID is
//! configured (degraded mode: work still runs, reply skipped).
//!
//! Webhook text is the least-trusted input in the system: minted
//! tasks carry NO `cloud_ok` tag and no constraints — the 5.2/5.3
//! gates keep cloud tiers shut, and the goal rides as a JSON string
//! (it cannot smuggle constraints). PRD §20.2's "forwarded to brain"
//! is realized by capability routing: `brain.plan` is
//! Anyone-cardinality, so the mesh places planning wherever the
//! election put it — equivalent-or-better than literal forwarding.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::IntoResponse,
};
use harness_store::TaskState;
use serde_json::json;

use super::twilio::{public_url, validate_twilio_signature};
use crate::routes::tasks::{check_admission, mint_task, SubmitRequest};
use crate::state::ApiState;

/// Vault tags (same tags the production cutover doc names).
pub const AUTH_TOKEN_TAG: &str = "secret/twilio-auth-token";
pub const ACCOUNT_SID_TAG: &str = "secret/twilio-account-sid";

/// CLI-parity input envelopes (crates/harness-cli/src/plan.rs).
const PLAN_TIMEOUT_MS: u64 = 240_000;
const EXEC_TIMEOUT_MS: u64 = 120_000;
const SLACK_MS: u64 = 5_000;
/// Overall driver deadline — `MAX_EXEC_TIMEOUT_MS`. A wedged mesh
/// must release the driver permit, not brick the adapter at 16
/// stuck conversations.
const DRIVER_DEADLINE_MS: u64 = 600_000;
const POLL_INTERVAL_MS: u64 = 500;
/// `WhatsApp` message body cap.
const REPLY_CHAR_CAP: usize = 1600;

fn twiml(message: Option<&str>) -> axum::response::Response {
    let body = match message {
        Some(m) => {
            let escaped = m
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response><Message>{escaped}</Message></Response>")
        }
        None => "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response/>".to_string(),
    };
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/xml")],
        body,
    )
        .into_response()
}

fn form_value<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// `POST /webhook/whatsapp`. Body arrives as
/// `application/x-www-form-urlencoded`; extracted as a raw `String`
/// so the SAME decoded pairs feed the signature and the fields (axum's
/// `Form` would consume the body and lose repeated keys).
pub async fn whatsapp_handler(
    State(state): State<ApiState>,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> axum::response::Response {
    // Fail closed: no auth token configured ⇒ the adapter does not
    // exist. Never accept an unsigned webhook.
    let Some(auth_token) = state.secrets.get(AUTH_TOKEN_TAG) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({ "error": "adapter_unconfigured", "missing": AUTH_TOKEN_TAG })),
        )
            .into_response();
    };

    let Ok(pairs) = serde_urlencoded::from_str::<Vec<(String, String)>>(&body) else {
        return (StatusCode::BAD_REQUEST, "malformed form body").into_response();
    };

    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost");
    let path_and_query = uri
        .path_and_query()
        .map_or_else(|| uri.path().to_string(), ToString::to_string);
    let signed_url = public_url(
        state.webhook.base_url_override.as_deref(),
        host,
        &path_and_query,
    );

    let signature = headers
        .get("x-twilio-signature")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if signature.is_empty()
        || !validate_twilio_signature(auth_token.as_bytes(), &signed_url, &pairs, signature)
    {
        tracing::warn!(target: "harness.api.webhook", %signed_url, "twilio signature rejected");
        return (StatusCode::FORBIDDEN, "signature mismatch").into_response();
    }

    // Sender gate (deny-all default): the signature authenticates
    // Twilio, not the human — anyone messaging the bot number gets
    // validly-signed webhooks.
    let from = form_value(&pairs, "From").unwrap_or("").to_string();
    let to = form_value(&pairs, "To").unwrap_or("").to_string();
    if !state.webhook.allow_from.permits(&from) {
        tracing::warn!(
            target: "harness.api.webhook",
            from = %from,
            "sender not in HARNESS_WEBHOOK_ALLOW_FROM; dropping message"
        );
        return twiml(None);
    }

    let goal = form_value(&pairs, "Body").unwrap_or("").trim().to_string();
    if goal.is_empty() {
        return twiml(Some("send a goal, e.g. \"run: uname -a\""));
    }

    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };
    // Root-level external input rides the 4.7 admission gate like any
    // other submitter (plan review MINOR-9) — in-channel "busy".
    if check_admission(store).is_some() {
        return twiml(Some("mesh busy — try again in a moment"));
    }
    let Ok(permit) = Arc::clone(&state.webhook.drivers).try_acquire_owned() else {
        return twiml(Some("mesh busy — try again in a moment"));
    };

    let plan_req = SubmitRequest {
        capability: "brain.plan".to_string(),
        input: json!({ "goal": goal }),
        constraints: None,
        execution: Some(exec_policy(PLAN_TIMEOUT_MS + SLACK_MS)),
        tags: vec!["webhook".to_string(), "whatsapp".to_string()],
        resource_hints: None,
    };
    let plan_id = match mint_task(&state, store, plan_req) {
        Ok(id) => id,
        Err(code) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({ "error": code })),
            )
                .into_response();
        }
    };

    let short = format!("{}", plan_id.0.as_hyphenated())
        .chars()
        .take(8)
        .collect::<String>();
    let driver_state = state.clone();
    tokio::spawn(async move {
        // The permit rides the driver and releases on drop.
        let _permit = permit;
        drive_conversation(driver_state, plan_id, from, to).await;
    });

    twiml(Some(&format!(
        "⏳ planning task {short} — I'll reply when it lands"
    )))
}

fn exec_policy(timeout_ms: u64) -> harness_core::ExecutionPolicy {
    harness_core::ExecutionPolicy {
        redundancy: 1,
        timeout_ms: u32::try_from(timeout_ms).unwrap_or(u32::MAX),
        on_partial: harness_core::protocol::PartialPolicy::FailFast,
        lease_ms: 10_000,
    }
}

/// Poll the STORE until `id` is terminal or the deadline passes.
/// Store-direct on purpose: the driver lives in the process that owns
/// the store, and HTTP-to-self cannot authenticate (no session).
async fn wait_terminal(
    state: &ApiState,
    id: harness_core::TaskId,
    deadline: tokio::time::Instant,
) -> Option<TaskState> {
    let store = state.store.as_ref()?;
    loop {
        match store.task_state(id) {
            Ok(Some(
                s @ (TaskState::Done
                | TaskState::Failed
                | TaskState::Cancelled
                | TaskState::Expired),
            )) => return Some(s),
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(target: "harness.api.webhook", ?err, "driver poll failed");
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
}

/// The execute-and-reply driver. Strictly Result-free control flow
/// (the release profile is panic=abort): every failure path becomes a
/// reply string.
async fn drive_conversation(
    state: ApiState,
    plan_id: harness_core::TaskId,
    inbound_from: String,
    inbound_to: String,
) {
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_millis(DRIVER_DEADLINE_MS);

    let reply = run_conversation(&state, plan_id, deadline, started).await;
    send_reply(&state, &inbound_from, &inbound_to, &reply).await;
}

async fn run_conversation(
    state: &ApiState,
    plan_id: harness_core::TaskId,
    deadline: tokio::time::Instant,
    started: tokio::time::Instant,
) -> String {
    let Some(store) = state.store.as_ref() else {
        return "❌ internal error: no store".to_string();
    };

    let Some(plan_state) = wait_terminal(state, plan_id, deadline).await else {
        return "⏳ timed out waiting for the planner — check the Runs page".to_string();
    };
    if plan_state != TaskState::Done {
        let diag = store
            .load_task_result(plan_id)
            .ok()
            .flatten()
            .and_then(|r| r.error)
            .unwrap_or_else(|| format!("planning {plan_state:?}"));
        return truncate_reply(&format!("❌ planning failed — {diag}"));
    }
    let Some(plan_json) = store
        .load_task_result(plan_id)
        .ok()
        .flatten()
        .and_then(|r| r.output)
        .and_then(|o| o.get("plan").cloned())
    else {
        return "❌ planner returned no plan".to_string();
    };
    let step_count = plan_json
        .get("tasks")
        .and_then(|t| t.as_object())
        .map_or(0, serde_json::Map::len);

    let exec_req = SubmitRequest {
        capability: "plan.execute".to_string(),
        input: json!({ "plan": plan_json, "timeout_ms": EXEC_TIMEOUT_MS }),
        constraints: None,
        execution: Some(exec_policy(EXEC_TIMEOUT_MS + SLACK_MS)),
        tags: vec!["webhook".to_string(), "whatsapp".to_string()],
        resource_hints: None,
    };
    let exec_id = match mint_task(state, store, exec_req) {
        Ok(id) => id,
        Err(code) => return format!("❌ could not start execution ({code})"),
    };

    let Some(exec_state) = wait_terminal(state, exec_id, deadline).await else {
        return "⏳ execution timed out — check the Runs page".to_string();
    };
    let secs = started.elapsed().as_secs();
    if exec_state == TaskState::Done {
        format!("✅ done — {step_count} steps in {secs}s")
    } else {
        let diag = store
            .load_task_result(exec_id)
            .ok()
            .flatten()
            .and_then(|r| r.error)
            .unwrap_or_else(|| format!("execution {exec_state:?}"));
        truncate_reply(&format!("❌ failed after {secs}s — {diag}"))
    }
}

fn truncate_reply(s: &str) -> String {
    if s.chars().count() <= REPLY_CHAR_CAP {
        return s.to_string();
    }
    let mut out: String = s.chars().take(REPLY_CHAR_CAP - 1).collect();
    out.push('…');
    out
}

/// Send the final reply through the Twilio Messages API. Outbound
/// `From` = the inbound `To` (our Twilio `WhatsApp` sender), outbound
/// `To` = the inbound `From` — no extra configuration. Missing account
/// SID degrades gracefully: the work already ran, the `TwiML` ack
/// already named the task id; we log and skip.
async fn send_reply(state: &ApiState, inbound_from: &str, inbound_to: &str, body: &str) {
    let Some(sid) = state.secrets.get(ACCOUNT_SID_TAG) else {
        tracing::warn!(
            target: "harness.api.webhook",
            "no {ACCOUNT_SID_TAG} configured; skipping outbound reply"
        );
        return;
    };
    let Some(token) = state.secrets.get(AUTH_TOKEN_TAG) else {
        return; // handler already required it; vanished mid-flight
    };
    let sid_str = String::from_utf8_lossy(sid.as_bytes()).to_string();
    let token_str = String::from_utf8_lossy(token.as_bytes()).to_string();

    let url = format!(
        "{}/2010-04-01/Accounts/{}/Messages.json",
        state.webhook.twilio_api_base.trim_end_matches('/'),
        sid_str
    );
    let form = [
        ("From", inbound_to),
        ("To", inbound_from),
        ("Body", &truncate_reply(body)),
    ];
    let res = state
        .webhook
        .http
        .post(&url)
        .basic_auth(&sid_str, Some(&token_str))
        .form(&form)
        .timeout(Duration::from_secs(15))
        .send()
        .await;
    match res {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => {
            tracing::warn!(
                target: "harness.api.webhook",
                status = %r.status(),
                "twilio reply rejected"
            );
        }
        Err(err) => {
            tracing::warn!(target: "harness.api.webhook", ?err, "twilio reply failed");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn twiml_escapes_message_content() {
        let r = twiml(Some("a<b>&c"));
        assert_eq!(r.status(), StatusCode::OK);
    }

    #[test]
    fn truncate_reply_caps_at_1600_chars() {
        let long = "x".repeat(4000);
        let t = truncate_reply(&long);
        assert_eq!(t.chars().count(), REPLY_CHAR_CAP);
        assert!(t.ends_with('…'));
        assert_eq!(truncate_reply("short"), "short");
    }
}
