//! `GET /api/v1/audit` — the History feed (5.13a, ADR-0041).
//!
//! Reads the hash-chained log PRD §10.6 requires. Two honesty rules
//! shape the surface:
//!
//! - **Verification is cached, not per-request.** Walking a chain is
//!   O(N) inside the single store mutex; doing it on every page load
//!   would let any authenticated caller stall the 100 ms dispatch
//!   poll (plan review MAJOR-3). The handler verifies only the page
//!   it returns, and reports which node's chain that covers.
//! - **`seq` is per-node**, so it cannot page a feed merged across
//!   nodes by time. Paging is keyset on `at_ms` (`?before_ms=`).

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::auth::{is_authenticated, unauthorized};
use crate::state::ApiState;

/// Default page size; the clamp mirrors `/tasks`.
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 500;

#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    /// Keyset cursor, all three parts together (a bare timestamp
    /// would skip rows sharing the boundary millisecond).
    pub before_ms: Option<u64>,
    pub before_node: Option<String>,
    pub before_seq: Option<u64>,
    /// Exact action filter (`shell.denied`, `cloud.escalated`, …).
    pub action: Option<String>,
    /// Restrict to one node's chain (hex node id).
    pub node: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct AuditEntryDto {
    pub node: String,
    pub seq: u64,
    pub at_ms: u64,
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
    pub actor: String,
    pub entry_hash: String,
}

pub async fn list_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<AuditQuery>,
) -> axum::response::Response {
    if !is_authenticated(&state.auth, &headers) {
        return unauthorized();
    }
    let Some(store) = state.store.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "store_not_configured" })),
        )
            .into_response();
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let node = match query.node.as_deref().map(parse_node) {
        Some(Ok(n)) => Some(n),
        Some(Err(())) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "bad_node" })),
            )
                .into_response()
        }
        None => None,
    };

    let cursor = match (
        query.before_ms,
        query.before_node.as_deref(),
        query.before_seq,
    ) {
        (Some(at_ms), Some(raw), Some(seq)) => match parse_node(raw) {
            Ok(n) => Some(harness_store::AuditCursor {
                at_ms,
                node: n,
                seq,
            }),
            Err(()) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "bad_cursor" })),
                )
                    .into_response()
            }
        },
        // A bare `before_ms` means STRICTLY older than that instant:
        // the low sentinel makes the same-millisecond arm of the
        // keyset predicate unsatisfiable. Callers paging a burst pass
        // the full cursor the response hands back.
        (Some(at_ms), _, _) => Some(harness_store::AuditCursor {
            at_ms,
            node: harness_core::NodeId::from_bytes([0u8; 16]),
            seq: 0,
        }),
        _ => None,
    };
    let rows = match store.audit_recent(cursor, query.action.as_deref(), node, limit) {
        Ok(rows) => rows,
        Err(err) => {
            tracing::error!(target: "harness.api.audit", ?err, "audit_recent");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "list_failed" })),
            )
                .into_response();
        }
    };

    let local = state.local_node_id;
    let verification = verify_page(store, local, &rows, limit);

    let next_cursor = rows.last().map(|r| {
        serde_json::json!({
            "before_ms": r.at_ms,
            "before_node": r.node_id.to_string(),
            "before_seq": r.seq,
        })
    });
    let entries: Vec<AuditEntryDto> = rows
        .into_iter()
        .map(|r| AuditEntryDto {
            node: r.node_id.to_string(),
            seq: r.seq,
            at_ms: r.at_ms,
            action: r.action,
            subject: r.subject,
            detail: r
                .detail
                .and_then(|d| serde_json::from_str::<serde_json::Value>(&d).ok()),
            actor: r.actor,
            entry_hash: harness_core::hash_hex(&r.entry_hash),
        })
        .collect();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "entries": entries,
            // Keyset cursor for the next page (entries are
            // newest-first) — pass all three parts back.
            "next_cursor": next_cursor,
            // Scoped deliberately (diff review MAJOR-3): this covers
            // the local rows ON THIS PAGE and their anchor, nothing
            // more. `checked: false` means nothing was verified — a
            // page of purely remote entries proves nothing about them,
            // and reporting "verified" there would be a lie. Other
            // nodes' chains are verified by their own nodes until
            // 5.13c replicates.
            "verification": verification,
        })),
    )
        .into_response()
}

/// Verify the local rows on one page plus their anchor.
///
/// Bounded by ROW COUNT, not seq span (diff review MAJOR-2): with an
/// `action` filter a page's rows are contiguous in time but scattered
/// across the chain, so a span bound would put the full-chain walk
/// straight back — inside the single store mutex, from an async
/// handler, reachable by any authenticated session.
fn verify_page(
    store: &harness_store::Store,
    local: harness_core::NodeId,
    rows: &[harness_store::AuditRow],
    limit: usize,
) -> serde_json::Value {
    let Some(from_seq) = rows
        .iter()
        .filter(|r| r.node_id == local)
        .map(|r| r.seq)
        .min()
    else {
        return serde_json::json!({
            "scope": "none",
            "node": local.to_string(),
            "checked": false,
        });
    };
    match store.audit_verify_chain(local, from_seq, limit) {
        Ok(harness_store::ChainStatus::Verified { through_seq }) => serde_json::json!({
            "scope": "page",
            "node": local.to_string(),
            "checked": true,
            "verified": true,
            "from_seq": from_seq,
            "through_seq": through_seq,
        }),
        Ok(harness_store::ChainStatus::Empty) => serde_json::json!({
            "scope": "none",
            "node": local.to_string(),
            "checked": false,
        }),
        Ok(harness_store::ChainStatus::Broken { at_seq }) => serde_json::json!({
            "scope": "page",
            "node": local.to_string(),
            "checked": true,
            "verified": false,
            "from_seq": from_seq,
            "broken_at_seq": at_seq,
        }),
        Err(err) => {
            tracing::error!(target: "harness.api.audit", ?err, "verify_chain");
            serde_json::json!({
                "scope": "page",
                "node": local.to_string(),
                "checked": false,
                "error": "verify_failed",
            })
        }
    }
}

fn parse_node(raw: &str) -> Result<harness_core::NodeId, ()> {
    let mut bytes = [0u8; 16];
    if raw.len() != 32 {
        return Err(());
    }
    for (i, chunk) in raw.as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).map_err(|_| ())?;
        bytes[i] = u8::from_str_radix(hex, 16).map_err(|_| ())?;
    }
    Ok(harness_core::NodeId::from_bytes(bytes))
}
