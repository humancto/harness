//! Weighted brain election (item 1.6).
//!
//! Each node computes its own [`brain_score`] from local hardware and
//! planner-availability inputs. The score travels in every
//! [`harness_core::Heartbeat::brain_score`] alongside the node's
//! current belief about who the brain is. The [`Election`] state
//! machine reads the local [`crate::PeerTable`] (populated by 1.5's
//! listener) and resolves "who's the brain right now?" deterministically:
//!
//! - Highest score wins.
//! - Ties broken by `NodeId` byte-order (lexicographic).
//! - Anti-flap: a node that was demoted from brain in the last 60s
//!   gets a -200 penalty so we don't ping-pong on a network blip.
//!
//! See PRD §12 for the design rationale.

mod scorer;
mod state;

pub use scorer::{brain_score, BrainScoreInput};
pub use state::{Election, ElectionConfig, ElectionResult, ANTI_FLAP_WINDOW};
