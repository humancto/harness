//! Phase 3.5 — Local-LLM micro-batcher.
//!
//! Coalesces identical `(model, fingerprint)` requests within a window
//! into a single backend call. See ADR-0011 for the honest scope:
//! against Ollama (no multi-prompt batching support) this is dedup-
//! within-window, not true batched inference. The dispatch closure is
//! the forward-compat hook for future batched backends (vLLM, TGI).
//!
//! Fingerprinting hand-lists the output-affecting fields. Adding a new
//! field to `LlmLocalInput` (e.g. `seed`, `top_p`) MUST be reflected
//! in the `fingerprint` helper or two callers with different settings
//! get coalesced and one gets the wrong output. Look for the
//! `// FINGERPRINT_FIELDS` marker on `LlmLocalInput`.

#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::Value as JsonValue;
use tokio::sync::oneshot;

use crate::traits::CapabilityError;

const DEFAULT_BATCH_WINDOW_MS: u64 = 50;
const ENV_BATCH_WINDOW_MS: &str = "HARNESS_LLM_BATCH_WINDOW_MS";

/// 32-byte blake3 over the canonicalized output-determining fields of
/// the request, including the model id. Two callers with the same
/// fingerprint share a backend call.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub struct Fingerprint(pub [u8; 32]);

/// One in-flight batch slot.
struct BatchSlot {
    /// Senders attached to this batch. The spawned timer task drains
    /// this list and sends a clone of the dispatch result to each.
    /// Senders whose receivers have been dropped (caller cancelled)
    /// produce `Err` on `send`; we ignore.
    senders: Vec<oneshot::Sender<Result<JsonValue, CapabilityError>>>,
}

/// Per-daemon batcher. Construct once via `from_env` (or `with_window`
/// for tests); clone the `Arc<LlmBatcher>` into every capability that
/// wants to participate.
pub struct LlmBatcher {
    /// `Arc<Mutex<...>>` because the spawned timer task needs its own
    /// reference (clone + move into the spawn). The outer `LlmBatcher`
    /// is itself `Arc`'d by capability holders, but the spawned timer's
    /// reference is decoupled from the capability's lifetime — first-
    /// caller cancellation cannot strand siblings.
    slots: Arc<Mutex<HashMap<Fingerprint, BatchSlot>>>,
    window: Duration,
}

impl std::fmt::Debug for LlmBatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmBatcher")
            .field("window_ms", &self.window.as_millis())
            .finish_non_exhaustive()
    }
}

impl LlmBatcher {
    /// Construct from `HARNESS_LLM_BATCH_WINDOW_MS` env var.
    /// Default 50ms; `0` disables batching entirely (every submit calls
    /// dispatch directly with no spawn / no mutex).
    #[must_use]
    pub fn from_env() -> Self {
        let window_ms = std::env::var(ENV_BATCH_WINDOW_MS)
            .ok()
            .and_then(|s| parse_window_ms(&s))
            .unwrap_or(DEFAULT_BATCH_WINDOW_MS);
        Self::with_window(Duration::from_millis(window_ms))
    }

    /// Test-friendly constructor: explicit window, no env var.
    #[must_use]
    pub fn with_window(window: Duration) -> Self {
        Self {
            slots: Arc::new(Mutex::new(HashMap::new())),
            window,
        }
    }

    #[must_use]
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Submit a request. Three paths:
    ///
    /// **A: window=0** — batcher disabled, call `dispatch` directly.
    /// No spawn, no mutex.
    ///
    /// **B: first caller for this fingerprint** — insert a slot, spawn
    /// the window timer task, wait on the oneshot. The spawned task
    /// owns drain-and-dispatch; cancellation of the caller's future
    /// drops only the receiver, not the timer.
    ///
    /// **C: sibling caller** — attach our oneshot to the existing slot,
    /// wait. Drained when the timer fires.
    pub async fn submit<F, Fut>(
        &self,
        fp: Fingerprint,
        dispatch: F,
    ) -> Result<JsonValue, CapabilityError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<JsonValue, CapabilityError>> + Send + 'static,
    {
        // Path A: window disabled.
        if self.window.is_zero() {
            return dispatch().await;
        }

        let (tx, rx) = oneshot::channel();
        let is_first = {
            // Sync mutex; we never await with the lock held.
            let mut slots = self.slots.lock();
            if let Some(slot) = slots.get_mut(&fp) {
                slot.senders.push(tx);
                false
            } else {
                slots.insert(fp, BatchSlot { senders: vec![tx] });
                true
            }
        };

        if is_first {
            let slots = Arc::clone(&self.slots);
            let window = self.window;
            // Spawned task owns drain-and-dispatch — survives the first
            // caller's cancellation.
            tokio::spawn(async move {
                tokio::time::sleep(window).await;
                // Remove the slot atomically *before* awaiting dispatch
                // so siblings arriving during the dispatch start a fresh
                // batch instead of attaching to a slot that's about to
                // be drained.
                let slot = {
                    let mut slots = slots.lock();
                    slots.remove(&fp)
                };
                let Some(slot) = slot else {
                    return; // already drained — defensive
                };
                let result = dispatch().await;
                for sender in slot.senders {
                    // `Err(_)` only if the receiver was dropped — fine.
                    let _ = sender.send(clone_result(&result));
                }
            });
        }

        rx.await.map_err(|_| {
            CapabilityError::Failed("batcher dropped sender (no result)".to_string())
        })?
    }
}

/// Clone a `Result<JsonValue, CapabilityError>` for fanout. `JsonValue`
/// has `Clone`; `CapabilityError` doesn't (variants hold non-Clone
/// types in general). We project the error via `to_string` into a
/// `Failed(String)` variant. **This loses Failed/InvalidInput/Cancelled
/// discrimination for fanned-out siblings** — acceptable for 3.5
/// because the only realistic dispatch error is `Failed` (HTTP/decode).
/// TODO when retry-policy lands and needs the discrimination: switch
/// to `Arc<CapabilityError>` fanout.
fn clone_result(r: &Result<JsonValue, CapabilityError>) -> Result<JsonValue, CapabilityError> {
    match r {
        Ok(v) => Ok(v.clone()),
        Err(e) => Err(CapabilityError::Failed(e.to_string())),
    }
}

/// Parse `HARNESS_LLM_BATCH_WINDOW_MS`. Used by `from_env`; tested in
/// isolation (no env-mutation in tests). Returns `None` for non-integer
/// or negative input — caller falls back to the default.
pub(crate) fn parse_window_ms(s: &str) -> Option<u64> {
    s.trim().parse::<u64>().ok()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod unit_tests {
    use super::*;

    #[test]
    fn parse_window_valid_int() {
        assert_eq!(parse_window_ms("50"), Some(50));
        assert_eq!(parse_window_ms("  100  "), Some(100));
        assert_eq!(parse_window_ms("0"), Some(0));
    }

    #[test]
    fn parse_window_invalid() {
        assert_eq!(parse_window_ms(""), None);
        assert_eq!(parse_window_ms("abc"), None);
        assert_eq!(parse_window_ms("-1"), None);
        assert_eq!(parse_window_ms("12.5"), None);
    }

    #[test]
    fn fingerprint_eq_identity() {
        let a = Fingerprint([0u8; 32]);
        let b = Fingerprint([0u8; 32]);
        assert_eq!(a, b);
    }

    #[test]
    fn debug_does_not_leak_inner_state() {
        let b = LlmBatcher::with_window(Duration::from_millis(50));
        let s = format!("{b:?}");
        assert!(s.contains("LlmBatcher"));
        assert!(s.contains("50"));
    }

    #[tokio::test]
    async fn zero_window_bypasses_batcher() {
        let b = LlmBatcher::with_window(Duration::ZERO);
        let result = b
            .submit(Fingerprint([1u8; 32]), || async {
                Ok(serde_json::json!("hi"))
            })
            .await
            .expect("dispatch");
        assert_eq!(result, serde_json::json!("hi"));
    }
}
