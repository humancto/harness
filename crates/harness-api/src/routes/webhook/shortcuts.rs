//! `POST /webhook/shortcuts` + `GET /webhook/shortcuts/result/:id` —
//! iOS Shortcuts adapter (5.7, ADR-0035, PRD §20.2).
//!
//! Shortcuts has no provider signature; auth is a signed JSON token
//! (`harness_vault::shortcuts_token`) minted by
//! `harness admin issue-shortcut-token` and carried as
//! `Authorization: Bearer <token>` — header only, never a query
//! param, so it cannot leak into request logs. Fail-closed: no
//! signing key in the vault (or a malformed one) means the adapter
//! does not exist (503).
//!
//! Unlike the fire-and-ack Twilio channels, a Shortcut is a
//! SYNCHRONOUS client: the handler waits (bounded `wait_ms`) for the
//! shared conversation driver and returns the reply in the response
//! body. Timeouts hand back `202 running` + the task id; the outcome
//! lands in the bounded [`ShortcutsLedger`] for the follow-up GET.
//! The ledger is also the GET's authorization scope: only
//! shortcuts-minted task ids exist in it, so a shortcut token can
//! never probe tasks minted by admins, plans, or other adapters.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;

use harness_vault::shortcuts_token::{parse_signing_key, verify_token, SHORTCUTS_KEY_TAG};

use super::conversation::{
    exec_policy, run_conversation, DRIVER_DEADLINE_MS, PLAN_TIMEOUT_MS, SHORTCUTS, SLACK_MS,
};
use crate::routes::tasks::{check_admission, mint_task, SubmitRequest};
use crate::state::ApiState;

/// Sync-wait bounds: default suits Shortcuts' ~60s HTTP timeout; the
/// clamp keeps a hostile token-holder from parking requests for the
/// driver's full 600s deadline.
const DEFAULT_WAIT_MS: u64 = 55_000;
const MIN_WAIT_MS: u64 = 1_000;
const MAX_WAIT_MS: u64 = 120_000;

/// Request body. `deny_unknown_fields` on purpose: a `constraints`
/// or `tags` field is a smuggling attempt and gets a 400, not a
/// silent drop (5.7 plan review NIT-14).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShortcutRequest {
    goal: String,
    /// Client-generated retry key: a Shortcut that timed out and
    /// re-runs with the same id gets the ORIGINAL task's status +
    /// `task_id` instead of a second mint.
    request_id: Option<String>,
    wait_ms: Option<u64>,
}

fn err_json(status: StatusCode, body: serde_json::Value) -> Response {
    (status, Json(body)).into_response()
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Bearer auth shared by POST and GET. `Err` is the ready-to-return
/// refusal. Logs carry the failure CLASS only — never token bytes.
fn authorize(state: &ApiState, headers: &HeaderMap) -> Result<String, Response> {
    // Fail closed: no (or malformed) signing key ⇒ the adapter does
    // not exist. Malformed is 503 too, never a panic (panic=abort).
    let Some(key_value) = state.secrets.get(SHORTCUTS_KEY_TAG) else {
        return Err(err_json(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "adapter_unconfigured", "missing": SHORTCUTS_KEY_TAG }),
        ));
    };
    let Ok(key) = std::str::from_utf8(key_value.as_bytes())
        .map_err(|_| ())
        .and_then(|s| parse_signing_key(s).map_err(|_| ()))
    else {
        tracing::warn!(
            target: "harness.api.webhook",
            channel = "shortcuts",
            "signing key at {SHORTCUTS_KEY_TAG} is not 64 hex chars; adapter disabled"
        );
        return Err(err_json(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "adapter_unconfigured", "missing": SHORTCUTS_KEY_TAG }),
        ));
    };
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or("");
    match verify_token(&key, token, unix_now()) {
        Ok(payload) => Ok(payload.sub),
        Err(class) => {
            tracing::warn!(
                target: "harness.api.webhook",
                channel = "shortcuts",
                %class,
                "shortcut token rejected"
            );
            Err(err_json(
                StatusCode::UNAUTHORIZED,
                json!({ "error": "invalid_token" }),
            ))
        }
    }
}

fn outcome_response(
    task_id: harness_core::TaskId,
    done: bool,
    ok: bool,
    reply: Option<&str>,
) -> Response {
    let id = format!("{}", task_id.0.as_hyphenated());
    if done {
        let status = if ok { "done" } else { "failed" };
        (
            StatusCode::OK,
            Json(json!({ "task_id": id, "status": status, "reply": reply })),
        )
            .into_response()
    } else {
        (
            StatusCode::ACCEPTED,
            Json(json!({
                "task_id": id,
                "status": "running",
                "reply": format!("⏳ still working — poll /webhook/shortcuts/result/{id}"),
            })),
        )
            .into_response()
    }
}

/// A remembered `request_id` answers with the ORIGINAL task's current
/// state — crucially including the `task_id` the timed-out client
/// never saw (plan review MAJOR-5).
fn duplicate_response(state: &ApiState, request_id: Option<&str>, sub: &str) -> Option<Response> {
    let rid = request_id.filter(|r| !r.is_empty())?;
    let ledger = state.webhook.shortcuts.lock();
    let task = ledger.lookup_request(rid)?;
    let o = ledger.get(task)?;
    tracing::info!(
        target: "harness.api.webhook",
        channel = "shortcuts",
        %sub,
        request_id = %rid,
        "duplicate request_id; returning original task state"
    );
    Some(outcome_response(task, o.done, o.ok, o.reply.as_deref()))
}

pub async fn shortcuts_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let sub = match authorize(&state, &headers) {
        Ok(sub) => sub,
        Err(refusal) => return refusal,
    };
    let Ok(req) = serde_json::from_str::<ShortcutRequest>(&body) else {
        return err_json(StatusCode::BAD_REQUEST, json!({ "error": "bad_request" }));
    };
    let goal = req.goal.trim().to_string();
    if goal.is_empty() {
        return err_json(StatusCode::BAD_REQUEST, json!({ "error": "empty_goal" }));
    }

    if let Some(dup) = duplicate_response(&state, req.request_id.as_deref(), &sub) {
        return dup;
    }

    let Some(store) = state.store.as_ref() else {
        return err_json(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "store_not_configured" }),
        );
    };
    // Root-level external input rides the 4.7 admission gate; the
    // driver semaphore bounds concurrent conversations (shared with
    // the Twilio channels on purpose — one global conversation cap).
    if check_admission(store).is_some() {
        return err_json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "error": "mesh_busy" }),
        );
    }
    let Ok(permit) = Arc::clone(&state.webhook.drivers).try_acquire_owned() else {
        return err_json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "error": "mesh_busy" }),
        );
    };

    let plan_req = SubmitRequest {
        capability: "brain.plan".to_string(),
        input: json!({ "goal": goal }),
        constraints: None,
        execution: Some(exec_policy(PLAN_TIMEOUT_MS + SLACK_MS)),
        tags: vec!["webhook".to_string(), SHORTCUTS.name.to_string()],
        resource_hints: None,
    };
    let plan_id = match mint_task(&state, store, plan_req) {
        Ok(id) => id,
        Err(code) => {
            return err_json(StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": code }));
        }
    };
    state
        .webhook
        .shortcuts
        .lock()
        .admit(plan_id, req.request_id.as_deref());
    tracing::info!(
        target: "harness.api.webhook",
        channel = "shortcuts",
        %sub,
        task = %plan_id.0.as_hyphenated(),
        "shortcut goal accepted"
    );

    // The permit moves INTO the spawned driver (plan review MINOR-8):
    // a client disconnect drops only this handler future — the driver
    // runs to its own deadline, the outcome still lands in the ledger
    // for a late GET, and the send into a dropped receiver is ignored.
    let (tx, rx) = tokio::sync::oneshot::channel::<(String, bool)>();
    let driver_state = state.clone();
    tokio::spawn(async move {
        let _permit = permit;
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_millis(DRIVER_DEADLINE_MS);
        let (reply, ok) =
            run_conversation(SHORTCUTS, &driver_state, plan_id, deadline, started).await;
        driver_state
            .webhook
            .shortcuts
            .lock()
            .complete(plan_id, ok, reply.clone());
        let _ = tx.send((reply, ok));
    });

    let wait_ms = req
        .wait_ms
        .unwrap_or(DEFAULT_WAIT_MS)
        .clamp(MIN_WAIT_MS, MAX_WAIT_MS);
    match tokio::time::timeout(Duration::from_millis(wait_ms), rx).await {
        Ok(Ok((reply, ok))) => outcome_response(plan_id, true, ok, Some(&reply)),
        // Timeout — or a dropped sender, which cannot outrun the
        // ledger write; either way the GET path picks it up.
        _ => outcome_response(plan_id, false, false, None),
    }
}

pub async fn shortcuts_result_handler(
    State(state): State<ApiState>,
    Path(id_str): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(refusal) = authorize(&state, &headers) {
        return refusal;
    }
    let Ok(uuid) = id_str.parse::<uuid::Uuid>() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "bad_task_id" })),
        )
            .into_response();
    };
    let task_id = harness_core::TaskId(uuid);
    let Some(outcome) = state.webhook.shortcuts.lock().get(task_id) else {
        // Unknown OR evicted OR not shortcuts-minted — indistinct on
        // purpose: this 404 is the confused-deputy boundary.
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown_or_expired" })),
        )
            .into_response();
    };
    outcome_response(task_id, outcome.done, outcome.ok, outcome.reply.as_deref())
}
