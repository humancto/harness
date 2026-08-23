//! External webhook adapters (PRD §20.2, roadmap 5.5+).
//!
//! `POST /webhook/<integration>` — each adapter validates the
//! provider's signature (fail-closed: no configured secret means
//! nothing validates) and converts the message into a Task/Plan
//! submission via the shared mint path in `routes::tasks`.

pub mod conversation;
pub mod shortcuts;
pub mod sms;
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
    /// 5.7 (ADR-0035): outcome ledger for the SYNCHRONOUS Shortcuts
    /// adapter. Deliberately separate from `seen_sids` — a looping
    /// authorized Shortcut must not churn Twilio SIDs out of their
    /// retry-dedup ring (plan review MINOR-6).
    pub shortcuts: parking_lot::Mutex<ShortcutsLedger>,
}

/// Bounded in-memory ledger backing the Shortcuts adapter's
/// late-result GET, its `request_id` retry dedup, and — by
/// construction — its authorization scope: the GET serves ONLY task
/// ids present here (i.e. shortcuts-minted), so a shortcut token can
/// never probe tasks minted by admins, plans, or other adapters
/// (plan review BLOCKER-1 + MAJOR-4 + MAJOR-5). Restart forgets it,
/// coherent with the in-memory driver (ADR-0033 durability note).
#[derive(Debug, Default)]
pub struct ShortcutsLedger {
    outcomes: std::collections::HashMap<harness_core::TaskId, ShortcutOutcome>,
    outcome_order: std::collections::VecDeque<harness_core::TaskId>,
    requests: std::collections::HashMap<String, harness_core::TaskId>,
    request_order: std::collections::VecDeque<String>,
}

/// What the ledger knows about one shortcuts-minted conversation.
#[derive(Debug, Clone)]
pub struct ShortcutOutcome {
    /// `true` once the driver finished (either way); the reply is
    /// then present.
    pub done: bool,
    /// `false` until done; then: did the plan execute successfully?
    pub ok: bool,
    /// The human reply text, present once done.
    pub reply: Option<String>,
}

/// FIFO caps (bounded-everything): outcomes cover the poll window,
/// request ids cover the client-retry window.
const SHORTCUT_OUTCOMES_CAP: usize = 256;
const SHORTCUT_REQUESTS_CAP: usize = 512;

impl ShortcutsLedger {
    /// A prior mint for this client `request_id`, if remembered.
    #[must_use]
    pub fn lookup_request(&self, request_id: &str) -> Option<harness_core::TaskId> {
        self.requests.get(request_id).copied()
    }

    /// The recorded outcome for a shortcuts-minted plan task. `None`
    /// means unknown to this adapter: never minted here, or evicted.
    #[must_use]
    pub fn get(&self, task: harness_core::TaskId) -> Option<ShortcutOutcome> {
        self.outcomes.get(&task).cloned()
    }

    /// Record a fresh mint (running, no reply yet). Called AFTER the
    /// mint succeeds — a refused request must stay retryable.
    pub fn admit(&mut self, task: harness_core::TaskId, request_id: Option<&str>) {
        self.outcomes.insert(
            task,
            ShortcutOutcome {
                done: false,
                ok: false,
                reply: None,
            },
        );
        self.outcome_order.push_back(task);
        while self.outcome_order.len() > SHORTCUT_OUTCOMES_CAP {
            if let Some(old) = self.outcome_order.pop_front() {
                self.outcomes.remove(&old);
            }
        }
        if let Some(rid) = request_id.filter(|r| !r.is_empty()) {
            self.requests.insert(rid.to_string(), task);
            self.request_order.push_back(rid.to_string());
            while self.request_order.len() > SHORTCUT_REQUESTS_CAP {
                if let Some(old) = self.request_order.pop_front() {
                    self.requests.remove(&old);
                }
            }
        }
    }

    /// Record the driver's final outcome. A ledger entry evicted
    /// mid-flight is silently dropped (the GET reports it expired).
    pub fn complete(&mut self, task: harness_core::TaskId, ok: bool, reply: String) {
        if let Some(entry) = self.outcomes.get_mut(&task) {
            entry.done = true;
            entry.ok = ok;
            entry.reply = Some(reply);
        }
    }
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
    /// Is `sid` already recorded? (Diff review MINOR-2: the handler
    /// checks membership BEFORE the fallible admission/permit/mint
    /// steps and records only after the mint succeeds — a refused
    /// delivery must stay retryable.)
    #[must_use]
    pub fn contains(&self, sid: &str) -> bool {
        !sid.is_empty() && self.set.contains(sid)
    }

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
            shortcuts: parking_lot::Mutex::new(ShortcutsLedger::default()),
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
    fn shortcuts_ledger_bounds_and_scopes() {
        let mut l = ShortcutsLedger::default();
        let t1 = harness_core::TaskId::new_v7();
        l.admit(t1, Some("req-1"));
        assert_eq!(l.lookup_request("req-1"), Some(t1));
        assert!(!l.get(t1).expect("admitted").done);
        // Unknown task ids stay unknown — the GET's authz boundary.
        assert!(l.get(harness_core::TaskId::new_v7()).is_none());

        l.complete(t1, true, "✅ done".to_string());
        let o = l.get(t1).expect("entry");
        assert!(o.done && o.ok);
        assert_eq!(o.reply.as_deref(), Some("✅ done"));

        // FIFO eviction over the cap; completing an evicted id is a
        // no-op, and the evicted entry reads as unknown.
        for i in 0..SHORTCUT_OUTCOMES_CAP {
            l.admit(harness_core::TaskId::new_v7(), Some(&format!("r{i}")));
        }
        assert!(l.get(t1).is_none(), "t1 evicted");
        l.complete(t1, false, "late".to_string());
        assert!(l.get(t1).is_none());
        assert!(l.outcomes.len() <= SHORTCUT_OUTCOMES_CAP);
        assert!(l.requests.len() <= SHORTCUT_REQUESTS_CAP);
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
