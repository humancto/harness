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

/// `request_id` bound (Codex P2 on #58): every distinct value is
/// cloned into two ledger structures, so a near-body-sized id from a
/// valid-token client could pin hundreds of large strings. A UUID is
/// 36 chars; 128 printable ASCII is generous and also keeps the
/// logged value injection-free.
const MAX_REQUEST_ID_LEN: usize = 128;

/// Goal cap (diff review MAJOR-1 second half): axum's default body
/// limit is ~2 MB; an authorized client must not push megabyte goals
/// into the store per mint. 4096 chars is far beyond any spoken or
/// typed Shortcut input.
const MAX_GOAL_LEN: usize = 4096;

fn request_id_ok(rid: &str) -> bool {
    rid.len() <= MAX_REQUEST_ID_LEN && rid.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

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

/// `None` on a broken clock — the ONE security check in this adapter
/// (token expiry) must fail closed, not open at `now = 0` (diff
/// review MINOR-2).
fn unix_now() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
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
    let Some(now) = unix_now() else {
        return Err(err_json(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "clock_unavailable" }),
        ));
    };
    // RFC 7235: the auth scheme is case-insensitive (diff review
    // NIT-6 — a hand-typed "bearer" in the Shortcuts header field
    // must not 401 mysteriously).
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Bearer"))
        .map_or("", |(_, t)| t.trim());
    match verify_token(&key, token, now) {
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
/// never saw (plan review MAJOR-5). Takes the CALLER-HELD ledger so
/// lookup and reservation are one atomic critical section (Codex P1
/// on #58: two concurrent same-id retries must not both mint).
/// A remembered id whose outcome was evicted is an EXPIRED duplicate
/// (410), never a fresh request (Codex P2: the two FIFO caps differ,
/// so the mapping can outlive the outcome).
fn duplicate_response(
    ledger: &super::ShortcutsLedger,
    request_id: Option<&str>,
    sub: &str,
) -> Option<Response> {
    let rid = request_id.filter(|r| !r.is_empty())?;
    let task = ledger.lookup_request(rid)?;
    let id = format!("{}", task.0.as_hyphenated());
    tracing::info!(
        target: "harness.api.webhook",
        channel = "shortcuts",
        %sub,
        request_id = %rid,
        task = %id,
        "duplicate request_id; returning original task state"
    );
    match ledger.get(task) {
        Some(o) => Some(outcome_response(task, o.done, o.ok, o.reply.as_deref())),
        None => Some(err_json(
            StatusCode::GONE,
            json!({
                "error": "result_expired",
                "task_id": id,
                "reply": "result expired — resubmit with a new request_id",
            }),
        )),
    }
}

/// The SYNC critical section: duplicate lookup → admission gate →
/// permit → mint → ledger reservation, all under ONE ledger lock
/// (Codex P1 on #58: concurrent retries with the same `request_id`
/// serialize here, so exactly one mints and the rest see the
/// reservation). Being a sync fn, the `parking_lot` guard structurally
/// cannot be held across an await. A refusal on any path returns
/// BEFORE the reservation, so the `request_id` stays retryable.
fn admit_goal(
    state: &ApiState,
    goal: &str,
    request_id: Option<&str>,
    sub: &str,
) -> Result<(harness_core::TaskId, tokio::sync::OwnedSemaphorePermit), Response> {
    let mut ledger = state.webhook.shortcuts.lock();
    if let Some(dup) = duplicate_response(&ledger, request_id, sub) {
        return Err(dup);
    }
    let Some(store) = state.store.as_ref() else {
        return Err(err_json(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "store_not_configured" }),
        ));
    };
    // Root-level external input rides the 4.7 admission gate; the
    // driver semaphore bounds concurrent conversations (shared with
    // the Twilio channels on purpose — one global conversation cap).
    if check_admission(store).is_some() {
        return Err(err_json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "error": "mesh_busy" }),
        ));
    }
    let Ok(permit) = Arc::clone(&state.webhook.drivers).try_acquire_owned() else {
        return Err(err_json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "error": "mesh_busy" }),
        ));
    };
    let plan_req = SubmitRequest {
        capability: "brain.plan".to_string(),
        input: json!({ "goal": goal }),
        constraints: None,
        execution: Some(exec_policy(PLAN_TIMEOUT_MS + SLACK_MS)),
        tags: vec!["webhook".to_string(), SHORTCUTS.name.to_string()],
        resource_hints: None,
    };
    let plan_id = mint_task(state, store, plan_req)
        .map_err(|code| err_json(StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": code })))?;
    ledger.admit(plan_id, request_id);
    Ok((plan_id, permit))
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
    if goal.chars().count() > MAX_GOAL_LEN {
        return err_json(StatusCode::BAD_REQUEST, json!({ "error": "goal_too_long" }));
    }

    if let Some(rid) = req.request_id.as_deref() {
        if !request_id_ok(rid) {
            return err_json(
                StatusCode::BAD_REQUEST,
                json!({ "error": "invalid_request_id" }),
            );
        }
    }

    let (plan_id, permit) = match admit_goal(&state, &goal, req.request_id.as_deref(), &sub) {
        Ok(admitted) => admitted,
        Err(refusal) => return refusal,
    };
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
