//! `WS /api/v1/runs/:task_id` — per-task result stream (3.3-gossip /
//! ADR-0019, carried from Phase 2 as STATE.md item 5).
//!
//! Wire shape: one JSON [`RunStreamEvent`] text frame per observed task
//! state change — `{ "state": "...", "output"?: ..., "error"?: ... }`.
//! `output` / `error` are OMITTED (not null) until the task is
//! terminal, mirroring `GET /api/v1/tasks/:id`. The first frame is the
//! task's current state at connect time; after pushing a terminal state
//! (`done` / `failed` / `expired` / `cancelled`, with the result row's
//! output or error attached) the server closes the socket with code
//! 1000.
//!
//! **4.2 (ADR-0024):** interleaved with state frames, the socket also
//! pushes [`RunPartialsEvent`] frames — `{ "partials": [{seq, stream,
//! line}, …] }` with only the frames not yet sent on this socket. This
//! carries every partial kind the ring holds: `progress` (per-target
//! fan-out telemetry from `mesh.*` wrappers) alongside `stdout` /
//! `stderr` shell lines — the same data `GET /tasks/:id` serves, live.
//! A final partials sweep runs BEFORE the terminal state frame, so
//! per-target results always precede the merged output. Bounded by the
//! ring (500 frames/task) per tick, same per-socket poll honesty as
//! the state stream.
//!
//! **Implementation honesty (ADR-0019):** the API layer has no
//! in-process event bus from the store — task transitions are written
//! by the executor / dispatch runtime straight into `SQLite`. This
//! route therefore polls the store every 250 ms server-side and pushes
//! only on change. That is the honest v1: one poller per open socket,
//! bounded by the connection count, no busy loop, no fake streaming
//! layer. A store-side notification bus can replace the poll without
//! changing the wire shape.
//!
//! Auth/origin discipline: loopback-only `Origin` when the header is
//! present (browser defense-in-depth, as `WS /api/v1/events`), PLUS —
//! 4.8 — session auth before the upgrade: this socket serves task
//! output and partials, the same data the bearer-gated `GET /tasks/:id`
//! serves, so it demands the same session. Browsers authenticate via
//! the `harness_session` cookie (sent on the upgrade request); CLI or
//! test clients set `Authorization: Bearer` on the handshake.

use std::time::Duration;

use axum::{
    extract::{
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header::ORIGIN, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use futures::{SinkExt, StreamExt};
use harness_core::TaskId;
use harness_store::TaskState;
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::state::ApiState;

use super::events::is_same_origin_loopback;

/// Server-side store poll cadence. See the module docs for why this is
/// a poll at all.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Partial-frame batch on the run stream (4.2). Mirrored in
/// `ui/src/lib/types.ts` (`RunPartialsEvent`).
#[derive(Debug, Serialize)]
pub struct RunPartialsEvent {
    pub partials: Vec<crate::partials::PartialFrame>,
}

/// One frame of the run stream. Mirrored in `ui/src/lib/types.ts`
/// (`RunStreamEvent`).
#[derive(Debug, Serialize)]
pub struct RunStreamEvent {
    /// `harness_store::TaskState::as_str()` value.
    pub state: &'static str,
    /// Result output — present only on a terminal `done` frame.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<JsonValue>,
    /// Failure reason — present only on terminal failure frames.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn is_terminal(state: TaskState) -> bool {
    matches!(
        state,
        TaskState::Done | TaskState::Failed | TaskState::Expired | TaskState::Cancelled
    )
}

pub async fn ws_run(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id_str): Path<String>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    if let Some(origin) = headers.get(ORIGIN) {
        let allowed = matches!(origin.to_str(), Ok(o) if is_same_origin_loopback(o));
        if !allowed {
            tracing::warn!(target: "harness.api.runs", ?origin, "ws origin rejected");
            return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
        }
    }
    // 4.8: same session gate as GET /tasks/:id (cookie or bearer on
    // the upgrade request) — this socket serves task output.
    if !crate::auth::is_authenticated(&state.auth, &headers) {
        return crate::auth::unauthorized();
    }
    let Some(store) = state.store.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };
    let Ok(uuid) = uuid::Uuid::parse_str(&id_str) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid_task_id" })),
        )
            .into_response();
    };
    let task_id = TaskId(uuid);
    // Reject unknown tasks BEFORE the upgrade so clients get a plain
    // 404 instead of an open-then-instantly-closed socket.
    match store.task_state(task_id) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "not_found" })),
            )
                .into_response();
        }
        Err(err) => {
            tracing::error!(target: "harness.api.runs", ?err, "task_state");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "state_failed" })),
            )
                .into_response();
        }
    }
    let partials = state.partials.clone();
    ws.on_upgrade(move |socket| stream_task(socket, store, partials, task_id))
}

/// Push frames with `seq > last_seq` (if any) as one
/// [`RunPartialsEvent`], advancing the cursor. `Err(())` = socket gone.
async fn push_new_partials(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    partials: &crate::partials::PartialBuffers,
    task_id: TaskId,
    last_seq: &mut Option<u64>,
) -> Result<(), ()> {
    let fresh: Vec<crate::partials::PartialFrame> = partials
        .frames(task_id)
        .into_iter()
        .filter(|f| last_seq.is_none_or(|s| f.seq > s))
        .collect();
    let Some(max) = fresh.iter().map(|f| f.seq).max() else {
        return Ok(());
    };
    *last_seq = Some(max);
    let event = RunPartialsEvent { partials: fresh };
    let payload = serde_json::to_string(&event).map_err(|err| {
        tracing::error!(target: "harness.api.runs", ?err, "serialize RunPartialsEvent");
    })?;
    sender.send(Message::Text(payload)).await.map_err(|_| ())
}

async fn stream_task(
    socket: WebSocket,
    store: harness_store::Store,
    partials: std::sync::Arc<crate::partials::PartialBuffers>,
    task_id: TaskId,
) {
    let (mut sender, mut receiver) = socket.split();
    let mut last_pushed: Option<TaskState> = None;
    // Highest partial-frame seq already sent on THIS socket.
    let mut last_seq: Option<u64> = None;
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    // First tick fires immediately → the connect-time state is pushed
    // without waiting a poll interval.

    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Partials first: on the terminal tick this is the
                // final sweep, so per-target results always precede
                // the terminal state frame (4.2, ADR-0024).
                if push_new_partials(&mut sender, &partials, task_id, &mut last_seq).await.is_err() {
                    break;
                }
                let task_state = match store.task_state(task_id) {
                    Ok(Some(s)) => s,
                    Ok(None) => {
                        // Row vanished mid-stream (future cancel/purge API).
                        let _ = sender
                            .send(Message::Close(Some(CloseFrame {
                                code: 1011,
                                reason: "task disappeared".into(),
                            })))
                            .await;
                        break;
                    }
                    Err(err) => {
                        tracing::error!(target: "harness.api.runs", ?err, "task_state poll");
                        let _ = sender
                            .send(Message::Close(Some(CloseFrame {
                                code: 1011,
                                reason: "store error".into(),
                            })))
                            .await;
                        break;
                    }
                };
                if last_pushed == Some(task_state) {
                    continue;
                }
                let terminal = is_terminal(task_state);
                let mut event = RunStreamEvent {
                    state: task_state.as_str(),
                    output: None,
                    error: None,
                };
                if terminal {
                    match store.load_task_result(task_id) {
                        Ok(Some(row)) => {
                            event.output = row.output;
                            event.error = row.error;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            tracing::warn!(target: "harness.api.runs", ?err, "load_task_result");
                        }
                    }
                }
                let payload = match serde_json::to_string(&event) {
                    Ok(p) => p,
                    Err(err) => {
                        tracing::error!(target: "harness.api.runs", ?err, "serialize RunStreamEvent");
                        break;
                    }
                };
                if sender.send(Message::Text(payload)).await.is_err() {
                    break;
                }
                last_pushed = Some(task_state);
                if terminal {
                    let _ = sender
                        .send(Message::Close(Some(CloseFrame {
                            code: 1000,
                            reason: "done".into(),
                        })))
                        .await;
                    break;
                }
            }
            // Drain client → server: no input is taken on this WS, but
            // close frames / errors must end the poller so sockets don't
            // leak store pollers.
            client = receiver.next() => {
                match client {
                    None | Some(Err(_) | Ok(Message::Close(_))) => break,
                    _ => {}
                }
            }
        }
    }

    let _ = sender.close().await;
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn terminal_states_match_store_semantics() {
        for s in [
            TaskState::Done,
            TaskState::Failed,
            TaskState::Expired,
            TaskState::Cancelled,
        ] {
            assert!(is_terminal(s));
        }
        for s in [
            TaskState::Submitted,
            TaskState::Planned,
            TaskState::Dispatched,
            TaskState::Claimed,
            TaskState::Running,
        ] {
            assert!(!is_terminal(s));
        }
    }

    #[test]
    fn event_omits_absent_output_and_error() {
        let e = RunStreamEvent {
            state: "running",
            output: None,
            error: None,
        };
        let json = serde_json::to_string(&e).expect("json");
        assert_eq!(json, r#"{"state":"running"}"#);
    }

    #[test]
    fn event_includes_output_when_present() {
        let e = RunStreamEvent {
            state: "done",
            output: Some(serde_json::json!({"ok": true})),
            error: None,
        };
        let json = serde_json::to_string(&e).expect("json");
        assert_eq!(json, r#"{"state":"done","output":{"ok":true}}"#);
    }
}
