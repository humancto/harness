//! `WS /api/v1/events` — live mesh events stream.
//!
//! Wire shape: one JSON `MeshEvent` per text frame. The server closes
//! the WebSocket with code 1011 + reason "lagged" if the client falls
//! behind the broadcast channel. The client's reconnect handler then
//! re-issues `GET /api/v1/peers` to resync state.
//!
//! Origin check: we reject connections whose `Origin` header does not
//! match a same-origin loopback URL. This is one-line defense-in-depth
//! against drive-by local exploits where another process opens a WS to
//! `ws://127.0.0.1:19198/api/v1/events` from a malicious page in a
//! browser pointed at a different origin.

use axum::{
    extract::{
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{header::ORIGIN, HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use tokio::sync::broadcast;

use crate::state::ApiState;

pub async fn ws_events(
    State(state): State<ApiState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if let Some(origin) = headers.get(ORIGIN) {
        let allowed = matches!(origin.to_str(), Ok(o) if is_same_origin_loopback(o));
        if !allowed {
            tracing::warn!(target: "harness.api.events", ?origin, "ws origin rejected");
            return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
        }
    }
    // No Origin header → non-browser client (curl, integration test).
    // Allow; same-origin policy is a browser-side defense.
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

fn is_same_origin_loopback(origin: &str) -> bool {
    // Allow http(s)://localhost(:port), http(s)://127.0.0.1(:port),
    // and http(s)://[::1](:port).
    let lower = origin.trim().to_ascii_lowercase();
    let stripped = lower
        .strip_prefix("http://")
        .or_else(|| lower.strip_prefix("https://"));
    let Some(rest) = stripped else { return false };
    let host_with_port = rest.split('/').next().unwrap_or("");

    // Bracketed IPv6: take the literal between [ and ].
    if let Some(after_open) = host_with_port.strip_prefix('[') {
        let host = after_open.split(']').next().unwrap_or("");
        return host == "::1";
    }

    let host_only = host_with_port.split(':').next().unwrap_or("");
    matches!(host_only, "localhost" | "127.0.0.1")
}

async fn handle_socket(socket: WebSocket, state: ApiState) {
    let (mut sender, mut receiver) = socket.split();
    let mut events = state.events.subscribe();

    loop {
        tokio::select! {
            biased;
            // Drain client → server: we don't take input on this WS,
            // but we must read so close frames + pings are processed.
            client = receiver.next() => {
                match client {
                    None | Some(Err(_) | Ok(Message::Close(_))) => break,
                    _ => {}
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        let payload = match serde_json::to_string(&event) {
                            Ok(s) => s,
                            Err(err) => {
                                tracing::error!(
                                    target: "harness.api.events",
                                    ?err,
                                    "failed to serialize MeshEvent — dropping frame"
                                );
                                continue;
                            }
                        };
                        if sender.send(Message::Text(payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            target: "harness.api.events",
                            dropped = n,
                            "ws subscriber lagged; closing for resync"
                        );
                        let _ = sender
                            .send(Message::Close(Some(CloseFrame {
                                code: 1011,
                                reason: "lagged".into(),
                            })))
                            .await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    let _ = sender.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_origins_accepted() {
        for ok in [
            "http://localhost",
            "http://localhost:5173",
            "http://127.0.0.1:19198",
            "http://[::1]:19198",
            "https://localhost",
        ] {
            assert!(is_same_origin_loopback(ok), "should accept {ok}");
        }
    }

    #[test]
    fn non_loopback_origins_rejected() {
        for bad in [
            "http://example.com",
            "https://evil.io:1234",
            "ws://localhost",
            "garbage",
            "",
        ] {
            assert!(!is_same_origin_loopback(bad), "should reject {bad}");
        }
    }
}
