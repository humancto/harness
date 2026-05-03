//! Non-identity newtypes used across the wire protocol.
//!
//! `NodeId` lives in [`crate::identity`] (it's derived from the public key
//! and 1.1 owns it). This module hosts the rest: `TaskId`, `PlanId`, `SemVer`.

use serde::{Deserialize, Serialize};

/// Task identifier — a Uuid v7 (sortable by creation time).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub uuid::Uuid);

impl TaskId {
    /// Generate a fresh Uuid v7 (sortable by creation time).
    ///
    /// # Panics
    ///
    /// Inherits `uuid::Uuid::now_v7`'s panic semantics: if the underlying
    /// `getrandom` source fails (only realistic on no-std / embedded
    /// targets without OS randomness), this will panic. Phase 7 will
    /// document handling for those targets if/when they become a goal.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

/// Plan identifier — a Uuid v7.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlanId(pub uuid::Uuid);

impl PlanId {
    /// Generate a fresh Uuid v7.
    ///
    /// # Panics
    ///
    /// See [`TaskId::new_v7`] — same semantics.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

/// Semantic version triple. Wire-stable: three `u16` fields, encoded in
/// declaration order by `ciborium`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SemVer {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl SemVer {
    #[must_use]
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn task_id_cbor_is_17_bytes() {
        // 1 byte CBOR type tag (0x50 = byte string of length 16) + 16 bytes payload.
        // Catches a serde feature flip on `uuid` that would switch to string encoding
        // and silently break wire compat.
        let id = TaskId(uuid::Uuid::nil());
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&id, &mut buf).expect("serialize");
        assert_eq!(buf.len(), 17, "TaskId CBOR shape changed: {buf:?}");
        assert_eq!(buf[0], 0x50, "expected CBOR byte-string-of-16 tag");
    }

    #[test]
    fn plan_id_cbor_round_trip() {
        let id = PlanId(uuid::Uuid::from_u128(
            0xdead_beef_dead_beef_dead_beef_dead_beef,
        ));
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&id, &mut buf).expect("serialize");
        let back: PlanId = ciborium::de::from_reader(buf.as_slice()).expect("deserialize");
        assert_eq!(id, back);
    }

    #[test]
    fn semver_cbor_round_trip() {
        let v = SemVer::new(1, 2, 3);
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&v, &mut buf).expect("serialize");
        let back: SemVer = ciborium::de::from_reader(buf.as_slice()).expect("deserialize");
        assert_eq!(v, back);
    }

    #[test]
    fn semver_orders_lexicographically_on_field_order() {
        assert!(SemVer::new(1, 0, 0) < SemVer::new(2, 0, 0));
        assert!(SemVer::new(1, 9, 99) < SemVer::new(2, 0, 0));
        assert!(SemVer::new(1, 2, 3) < SemVer::new(1, 2, 4));
    }
}
