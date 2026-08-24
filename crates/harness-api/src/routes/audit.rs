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
    /// Keyset cursor: entries strictly older than this unix-ms.
    pub before_ms: Option<u64>,
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

    let rows = match store.audit_recent(query.before_ms, query.action.as_deref(), node, limit) {
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

    // Verify this node's own chain from the oldest seq ON THIS PAGE —
    // bounded work, and the answer a reader of this page needs.
    let local = state.local_node_id;
    let from_seq = rows
        .iter()
        .filter(|r| r.node_id == local)
        .map(|r| r.seq)
        .min()
        .unwrap_or(1);
    let (verified, broken_at) = match store.audit_verify_chain(local, from_seq) {
        Ok(harness_store::ChainStatus::Verified { .. } | harness_store::ChainStatus::Empty) => {
            (true, None)
        }
        Ok(harness_store::ChainStatus::Broken { at_seq }) => (false, Some(at_seq)),
        Err(err) => {
            tracing::error!(target: "harness.api.audit", ?err, "verify_chain");
            (false, None)
        }
    };

    let next_before_ms = rows.last().map(|r| r.at_ms);
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
            // Keyset cursor for the next page (entries are newest-first).
            "next_before_ms": next_before_ms,
            // Covers THIS node's chain only: every other node's entries
            // arrive by replication in 5.13c, and their chains are
            // verified by their own nodes until then.
            "verified": verified,
            "broken_at_seq": broken_at,
            "verified_node": local.to_string(),
        })),
    )
        .into_response()
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
