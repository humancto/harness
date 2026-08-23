//! External webhook adapters (PRD §20.2, roadmap 5.5+).
//!
//! `POST /webhook/<integration>` — each adapter validates the
//! provider's signature (fail-closed: no configured secret means
//! nothing validates) and converts the message into a Task/Plan
//! submission via the shared mint path in `routes::tasks`.

pub mod twilio;
pub mod whatsapp;

use std::collections::HashSet;
use std::sync::Arc;

/// 5.5 (ADR-0033): at most this many concurrent execute-and-reply
/// conversations. Over the cap the sender gets an in-channel "mesh
/// busy" — 4.7's bounded-everything discipline for the new surface.
pub const MAX_WEBHOOK_DRIVERS: usize = 16;

/// Sender allowlist posture. The Twilio signature authenticates
/// TWILIO, not the sender — Twilio validly signs a webhook for ANY
/// account that messages the bot number, so the default is deny-all
/// (plan review BLOCKER-3): an unset/empty allowlist drops every
/// message with a log hint naming the knob.
#[derive(Debug, Clone)]
pub enum AllowFrom {
    /// Explicit `HARNESS_WEBHOOK_ALLOW_FROM="*"`.
    All,
    /// Exact-match senders. `WhatsApp` numbers arrive PREFIXED
    /// (`whatsapp:+15551234567`) and are matched in that full form;
    /// SMS (5.6) uses bare E.164.
    Senders(HashSet<String>),
}

impl AllowFrom {
    fn from_env_value(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some("*") => Self::All,
            Some(list) if !list.is_empty() => Self::Senders(
                list.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect(),
            ),
            _ => Self::Senders(HashSet::new()),
        }
    }

    #[must_use]
    pub fn permits(&self, from: &str) -> bool {
        match self {
            Self::All => true,
            Self::Senders(set) => set.contains(from),
        }
    }
}

/// Shared webhook runtime, hung off [`crate::ApiState`]. Tests inject
/// their own via `ApiStateBuilder::with_webhook_runtime` (notably the
/// Twilio API base pointed at wiremock).
pub struct WebhookRuntime {
    /// `HARNESS_WEBHOOK_BASE_URL` — the public base Twilio signs
    /// against. REQUIRED in production behind a port forward / TLS
    /// terminator; the Host-header fallback serves direct exposure.
    pub base_url_override: Option<String>,
    /// `HARNESS_WEBHOOK_ALLOW_FROM` — deny-all by default.
    pub allow_from: AllowFrom,
    /// Outbound Twilio REST base (`https://api.twilio.com` in
    /// production; wiremock in tests).
    pub twilio_api_base: String,
    /// Driver concurrency bound. `OwnedSemaphorePermit`s move into
    /// the spawned drivers and release on drop (panic-safe; and the
    /// release profile is panic=abort anyway, so drivers are strictly
    /// Result-returning).
    pub drivers: Arc<tokio::sync::Semaphore>,
    /// Outbound HTTP client for the final reply.
    pub http: reqwest::Client,
    /// Recently-seen provider message ids (Codex P1: Twilio RETRIES
    /// deliveries — a lost response replays the same validly-signed
    /// request, and re-minting would double-execute the goal).
    /// Bounded in-memory ring; a daemon restart forgets it, which is
    /// coherent with the driver also being in-memory (ADR-0033
    /// restart-durability note).
    pub seen_sids: parking_lot::Mutex<SeenSids>,
}

/// Bounded insert-order dedup set.
#[derive(Debug, Default)]
pub struct SeenSids {
    order: std::collections::VecDeque<String>,
    set: HashSet<String>,
}

/// Twilio's retry window is minutes; 512 recent ids is ample for a
/// 16-driver adapter.
const SEEN_SIDS_CAP: usize = 512;

impl SeenSids {
    /// Record `sid`; returns `false` if it was already present (a
    /// provider retry — the caller must NOT mint again).
    pub fn insert(&mut self, sid: &str) -> bool {
        if sid.is_empty() {
            return true; // no id to dedup on — accept
        }
        if !self.set.insert(sid.to_string()) {
            return false;
        }
        self.order.push_back(sid.to_string());
        while self.order.len() > SEEN_SIDS_CAP {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}

impl std::fmt::Debug for WebhookRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookRuntime")
            .field("base_url_override", &self.base_url_override)
            .field("allow_from", &self.allow_from)
            .field("twilio_api_base", &self.twilio_api_base)
            .finish_non_exhaustive()
    }
}

impl WebhookRuntime {
    /// Production construction from the environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            base_url_override: std::env::var("HARNESS_WEBHOOK_BASE_URL")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            allow_from: AllowFrom::from_env_value(
                std::env::var("HARNESS_WEBHOOK_ALLOW_FROM").ok().as_deref(),
            ),
            twilio_api_base: "https://api.twilio.com".to_string(),
            drivers: Arc::new(tokio::sync::Semaphore::new(MAX_WEBHOOK_DRIVERS)),
            http: reqwest::Client::new(),
            seen_sids: parking_lot::Mutex::new(SeenSids::default()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn seen_sids_dedups_and_stays_bounded() {
        let mut s = SeenSids::default();
        assert!(s.insert("SM1"));
        assert!(!s.insert("SM1"), "retry detected");
        assert!(s.insert(""), "empty sid never dedups");
        assert!(s.insert(""), "empty sid never dedups");
        for i in 0..SEEN_SIDS_CAP {
            assert!(s.insert(&format!("SMx{i}")));
        }
        // SM1 evicted by the ring bound — accepted again.
        assert!(s.insert("SM1"));
        assert!(s.set.len() <= SEEN_SIDS_CAP + 1);
    }

    #[test]
    fn allow_from_is_deny_all_by_default() {
        for raw in [None, Some(""), Some("  ")] {
            let a = AllowFrom::from_env_value(raw);
            assert!(!a.permits("whatsapp:+15551234567"), "default must deny");
        }
        let star = AllowFrom::from_env_value(Some("*"));
        assert!(star.permits("whatsapp:+15551234567"));
        let list =
            AllowFrom::from_env_value(Some("whatsapp:+15551234567, whatsapp:+4915112345678"));
        assert!(list.permits("whatsapp:+15551234567"));
        assert!(list.permits("whatsapp:+4915112345678"));
        assert!(!list.permits("whatsapp:+19998887777"));
        assert!(!list.permits("+15551234567"), "prefix is part of the match");
    }
}
