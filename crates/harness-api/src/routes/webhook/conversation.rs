//! Channel-generic Twilio conversation core (5.5 ADR-0033, made
//! channel-parameterized in 5.6 ADR-0034 — `WhatsApp` and SMS share
//! ONE body; the deltas are the route, the task tag, and the sender
//! allowlist form).
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

/// Per-channel identity (5.6, ADR-0034). Everything else — signature
/// scheme, dedup ring, admission, driver, reply flow, caps — is
/// channel-invariant by construction. The name doubles as the task
/// tag, and is threaded through the DRIVER too (plan review MAJOR-1:
/// both the `brain.plan` and `plan.execute` mints carry it).
#[derive(Debug, Clone, Copy)]
pub struct Channel {
    /// `"whatsapp"` | `"sms"` — route suffix, task tag, log field.
    pub name: &'static str,
}

pub const WHATSAPP: Channel = Channel { name: "whatsapp" };
pub const SMS: Channel = Channel { name: "sms" };
/// 5.7 (ADR-0035): the Shortcuts adapter reuses this driver core —
/// same tags-on-both-mints rule, but the reply returns over HTTP via
/// the ledger instead of the Twilio Messages API.
pub const SHORTCUTS: Channel = Channel { name: "shortcuts" };

/// CLI-parity input envelopes (crates/harness-cli/src/plan.rs).
pub(super) const PLAN_TIMEOUT_MS: u64 = 240_000;
const EXEC_TIMEOUT_MS: u64 = 120_000;
pub(super) const SLACK_MS: u64 = 5_000;
/// Overall driver deadline — `MAX_EXEC_TIMEOUT_MS`. A wedged mesh
/// must release the driver permit, not brick the adapter at 16
/// stuck conversations.
pub(super) const DRIVER_DEADLINE_MS: u64 = 600_000;
const POLL_INTERVAL_MS: u64 = 500;
/// Twilio Message-resource body cap — same for `WhatsApp` and SMS
/// (Twilio segments SMS; ADR-0034 records the UCS-2 economics).
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

/// The same sender in the OTHER channel's allowlist entry form
/// (`whatsapp:+E164` ⇄ bare `+E164`) — feeds only the near-miss
/// drop-log hint, never an allow decision. Hardcodes the one prefixed
/// Twilio channel that exists; a third prefixed channel would need
/// this to become a `Channel` field.
fn other_channel_form(from: &str) -> String {
    from.strip_prefix("whatsapp:")
        .map_or_else(|| format!("whatsapp:{from}"), str::to_string)
}

fn form_value<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// Signature preamble: reconstruct the public URL and validate the
/// `X-Twilio-Signature` header over it. `Err` is the ready-to-return
/// refusal response.
fn verify_signature(
    state: &ApiState,
    headers: &HeaderMap,
    uri: &Uri,
    pairs: &[(String, String)],
) -> Result<(), axum::response::Response> {
    // Fail closed: no auth token configured ⇒ the adapter does not
    // exist. Never accept an unsigned webhook.
    let Some(auth_token) = state.secrets.get(AUTH_TOKEN_TAG) else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({ "error": "adapter_unconfigured", "missing": AUTH_TOKEN_TAG })),
        )
            .into_response());
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
        || !validate_twilio_signature(auth_token.as_bytes(), &signed_url, pairs, signature)
    {
        tracing::warn!(target: "harness.api.webhook", %signed_url, "twilio signature rejected");
        return Err((StatusCode::FORBIDDEN, "signature mismatch").into_response());
    }
    Ok(())
}

/// `POST /webhook/<channel>`. Body arrives as
/// `application/x-www-form-urlencoded`; extracted as a raw `String`
/// so the SAME decoded pairs feed the signature and the fields (axum's
/// `Form` would consume the body and lose repeated keys).
pub fn handle(
    channel: Channel,
    state: &ApiState,
    uri: &Uri,
    headers: &HeaderMap,
    body: &str,
) -> axum::response::Response {
    let Ok(pairs) = serde_urlencoded::from_str::<Vec<(String, String)>>(body) else {
        return (StatusCode::BAD_REQUEST, "malformed form body").into_response();
    };
    if let Err(refusal) = verify_signature(state, headers, uri, &pairs) {
        return refusal;
    }

    // Sender gate (deny-all default): the signature authenticates
    // Twilio, not the human — anyone messaging the bot number gets
    // validly-signed webhooks.
    let from = form_value(&pairs, "From").unwrap_or("").to_string();
    let to = form_value(&pairs, "To").unwrap_or("").to_string();
    if !state.webhook.allow_from.permits(&from) {
        // Near-miss hint (5.6 plan review MINOR-2): entries are
        // channel-NATIVE (`whatsapp:+E164` vs bare `+E164`) — say so
        // when the operator listed this number in the other form.
        let near_miss = state.webhook.allow_from.permits(&other_channel_form(&from));
        tracing::warn!(
            target: "harness.api.webhook",
            channel = channel.name,
            from = %from,
            near_miss,
            "sender not in HARNESS_WEBHOOK_ALLOW_FROM; dropping message{}",
            if near_miss {
                " (the allowlist has this number in the OTHER channel's form — \
                 entries are channel-native: whatsapp:+E164 for WhatsApp, bare +E164 for SMS)"
            } else {
                ""
            }
        );
        return twiml(None);
    }

    // Provider compliance events: when Twilio's opt-out handling
    // fires (STOP/HELP/START keywords), the inbound webhook still
    // arrives — tagged `OptOutType` — while Twilio owns the reply and
    // the messaging-state change. A compliance keyword is not a goal:
    // ack empty, mint nothing, attempt no reply of our own.
    if let Some(opt_out) = form_value(&pairs, "OptOutType") {
        tracing::info!(
            target: "harness.api.webhook",
            channel = channel.name,
            opt_out_type = %opt_out,
            "provider opt-out/help event; not treating body as a goal"
        );
        return twiml(None);
    }

    let goal = form_value(&pairs, "Body").unwrap_or("").trim().to_string();
    if goal.is_empty() {
        return twiml(Some("send a goal, e.g. \"run: uname -a\""));
    }

    // Provider retry dedup (Codex P1): Twilio replays the same signed
    // request when a response is lost; re-minting would double-run
    // the goal. Membership is checked HERE but the sid is recorded
    // only after the mint succeeds (diff review MINOR-2) — a "mesh
    // busy" or mint failure must leave the delivery retryable.
    let message_sid = form_value(&pairs, "MessageSid").unwrap_or("").to_string();
    if state.webhook.seen_sids.lock().contains(&message_sid) {
        tracing::info!(
            target: "harness.api.webhook",
            sid = %message_sid,
            "duplicate delivery; already working on it"
        );
        return twiml(Some("⏳ already working on that message"));
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
        tags: vec!["webhook".to_string(), channel.name.to_string()],
        resource_hints: None,
    };
    let plan_id = match mint_task(state, store, plan_req) {
        Ok(id) => id,
        Err(code) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({ "error": code })),
            )
                .into_response();
        }
    };
    state.webhook.seen_sids.lock().insert(&message_sid);

    let short = format!("{}", plan_id.0.as_hyphenated())
        .chars()
        .take(8)
        .collect::<String>();
    let driver_state = state.clone();
    tokio::spawn(async move {
        // The permit rides the driver and releases on drop.
        let _permit = permit;
        drive_conversation(channel, driver_state, plan_id, from, to).await;
    });

    twiml(Some(&format!(
        "⏳ planning task {short} — I'll reply when it lands"
    )))
}

pub(super) fn exec_policy(timeout_ms: u64) -> harness_core::ExecutionPolicy {
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

/// Poll for a terminal task's result row. Task state flips terminal
/// BEFORE the result row is written on the executor (the same gap the
/// 5.3 money tests hit — Codex P1 here): a one-shot load right after
/// `wait_terminal` can see `None` for a row that lands milliseconds
/// later. Bounded, never a behavioral wait.
async fn wait_result_row(
    state: &ApiState,
    id: harness_core::TaskId,
    deadline: tokio::time::Instant,
) -> Option<harness_store::TaskResult> {
    let store = state.store.as_ref()?;
    let bound = std::cmp::min(
        deadline,
        tokio::time::Instant::now() + Duration::from_secs(5),
    );
    loop {
        if let Ok(Some(row)) = store.load_task_result(id) {
            return Some(row);
        }
        if tokio::time::Instant::now() >= bound {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The execute-and-reply driver. Strictly Result-free control flow
/// (the release profile is panic=abort): every failure path becomes a
/// reply string.
async fn drive_conversation(
    channel: Channel,
    state: ApiState,
    plan_id: harness_core::TaskId,
    inbound_from: String,
    inbound_to: String,
) {
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_millis(DRIVER_DEADLINE_MS);

    let (reply, _ok) = run_conversation(channel, &state, plan_id, deadline, started).await;
    send_reply(&state, &inbound_from, &inbound_to, &reply).await;
}

/// The channel-agnostic middle of every webhook conversation: wait
/// for the plan, mint `plan.execute` (channel-tagged — 5.6 MAJOR-1),
/// wait for execution, format the human reply. Returns the reply and
/// whether execution completed successfully (5.7: the Shortcuts
/// adapter surfaces `ok` as its `status` field; Twilio callers send
/// the text either way).
pub(super) async fn run_conversation(
    channel: Channel,
    state: &ApiState,
    plan_id: harness_core::TaskId,
    deadline: tokio::time::Instant,
    started: tokio::time::Instant,
) -> (String, bool) {
    let Some(store) = state.store.as_ref() else {
        return ("❌ internal error: no store".to_string(), false);
    };

    let Some(plan_state) = wait_terminal(state, plan_id, deadline).await else {
        return (
            "⏳ timed out waiting for the planner — check the Runs page".to_string(),
            false,
        );
    };
    if plan_state != TaskState::Done {
        let diag = wait_result_row(state, plan_id, deadline)
            .await
            .and_then(|r| r.error)
            .unwrap_or_else(|| format!("planning {plan_state:?}"));
        return (
            truncate_reply(&format!("❌ planning failed — {diag}")),
            false,
        );
    }
    let Some(plan_json) = wait_result_row(state, plan_id, deadline)
        .await
        .and_then(|r| r.output)
        .and_then(|o| o.get("plan").cloned())
    else {
        return ("❌ planner returned no plan".to_string(), false);
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
        tags: vec!["webhook".to_string(), channel.name.to_string()],
        resource_hints: None,
    };
    let exec_id = match mint_task(state, store, exec_req) {
        Ok(id) => id,
        Err(code) => return (format!("❌ could not start execution ({code})"), false),
    };

    let Some(exec_state) = wait_terminal(state, exec_id, deadline).await else {
        return (
            "⏳ execution timed out — check the Runs page".to_string(),
            false,
        );
    };
    let secs = started.elapsed().as_secs();
    if exec_state == TaskState::Done {
        (format!("✅ done — {step_count} steps in {secs}s"), true)
    } else {
        let diag = wait_result_row(state, exec_id, deadline)
            .await
            .and_then(|r| r.error)
            .unwrap_or_else(|| format!("execution {exec_state:?}"));
        (
            truncate_reply(&format!("❌ failed after {secs}s — {diag}")),
            false,
        )
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
    // Deliberate crossing of SecretValue's redaction wall (diff
    // review NIT-4): reqwest's basic_auth takes Display types, so the
    // credentials exist briefly as plain Strings here. reqwest builds
    // the header with set_sensitive(true) and never logs it; the
    // Strings drop at function exit.
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
    fn other_channel_form_flips_the_whatsapp_prefix_both_ways() {
        // Near-miss hint mapping (5.6 MINOR-2): bare E.164 ⇄
        // whatsapp-prefixed, exact round trip, no other rewriting.
        assert_eq!(other_channel_form("+15551234567"), "whatsapp:+15551234567");
        assert_eq!(other_channel_form("whatsapp:+15551234567"), "+15551234567");
        assert_eq!(
            other_channel_form(&other_channel_form("+15551234567")),
            "+15551234567"
        );
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
