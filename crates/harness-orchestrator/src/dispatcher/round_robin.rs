//! Round-robin selector. Persists the cursor in the store so daemon
//! restarts don't bias toward the lowest `NodeId` for the first few
//! dispatches. Reads + writes are serialized at the store mutex; for
//! Phase 2 (<<1k dispatches/sec) this is plenty.

use harness_core::NodeId;

use crate::error::DispatchError;

/// In-memory round-robin selector. The persistent variant lives in
/// `harness-store` and is wired to the dispatcher in 2.6 (where the
/// runtime owns a `Store` reference). 2.4 ships this minimal in-memory
/// shape so the dispatcher's `Anyone`-tiebreak logic is unit-testable
/// without standing up a database.
#[derive(Debug, Default)]
pub struct RoundRobin {
    /// Last-dispatched node per capability. `None` until the first
    /// dispatch. `parking_lot::Mutex` so this is `Send + Sync`.
    inner: parking_lot::Mutex<std::collections::HashMap<String, NodeId>>,
}

impl RoundRobin {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pick the next eligible node after the persisted cursor. `eligible`
    /// must already be sorted (the dispatcher keeps it sorted). Updates
    /// the cursor as a side-effect.
    ///
    /// # Errors
    /// `DispatchError::NoEligibleNodes` if `eligible` is empty.
    pub fn next(&self, capability: &str, eligible: &[NodeId]) -> Result<NodeId, DispatchError> {
        if eligible.is_empty() {
            return Err(DispatchError::NoEligibleNodes {
                capability: capability.to_string(),
            });
        }
        let mut g = self.inner.lock();
        let chosen = match g.get(capability).copied() {
            None => eligible[0],
            Some(prev) => eligible
                .iter()
                .copied()
                .find(|n| *n > prev)
                .unwrap_or(eligible[0]),
        };
        g.insert(capability.to_string(), chosen);
        Ok(chosen)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn nid(b: u8) -> NodeId {
        NodeId::from_bytes([b; 16])
    }

    #[test]
    fn empty_eligible_errors() {
        let rr = RoundRobin::new();
        let err = rr.next("echo", &[]).unwrap_err();
        assert!(matches!(err, DispatchError::NoEligibleNodes { .. }));
    }

    #[test]
    fn first_call_picks_lowest() {
        let rr = RoundRobin::new();
        let nodes = vec![nid(1), nid(2), nid(3)];
        assert_eq!(rr.next("echo", &nodes).unwrap(), nid(1));
    }

    #[test]
    fn second_call_advances() {
        let rr = RoundRobin::new();
        let nodes = vec![nid(1), nid(2), nid(3)];
        assert_eq!(rr.next("echo", &nodes).unwrap(), nid(1));
        assert_eq!(rr.next("echo", &nodes).unwrap(), nid(2));
        assert_eq!(rr.next("echo", &nodes).unwrap(), nid(3));
        assert_eq!(rr.next("echo", &nodes).unwrap(), nid(1)); // wrap
    }

    #[test]
    fn cursor_is_per_capability() {
        let rr = RoundRobin::new();
        let nodes = vec![nid(1), nid(2)];
        assert_eq!(rr.next("echo", &nodes).unwrap(), nid(1));
        assert_eq!(rr.next("shell.exec", &nodes).unwrap(), nid(1));
        assert_eq!(rr.next("echo", &nodes).unwrap(), nid(2));
    }

    #[test]
    fn shrinking_eligible_set_does_not_panic() {
        let rr = RoundRobin::new();
        // Cursor advances to nid(3), then nid(3) drops out of eligible.
        let three = vec![nid(1), nid(2), nid(3)];
        rr.next("echo", &three).unwrap();
        rr.next("echo", &three).unwrap();
        rr.next("echo", &three).unwrap(); // cursor = nid(3)
                                          // Now only nid(1) and nid(2) are eligible.
        let two = vec![nid(1), nid(2)];
        // No node > nid(3) in the new set, so wrap to nid(1).
        assert_eq!(rr.next("echo", &two).unwrap(), nid(1));
    }
}
