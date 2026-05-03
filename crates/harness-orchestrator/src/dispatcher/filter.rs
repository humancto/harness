//! Constraint filtering — `pin_to_node`, `pin_to_scope`, `must_be_local`,
//! `require_tags`, `exclude_tags`. Tag/local filters are stubs in 2.2;
//! wired by 3.6 when cloud capabilities ship.

use harness_core::{Constraints, NodeId};

use crate::dispatcher::live_set::LiveSet;
use crate::error::DispatchError;
use crate::index::ScopeIndex;

pub(crate) fn apply_constraints<L: LiveSet>(
    mut candidates: Vec<NodeId>,
    c: &Constraints,
    scopes: &ScopeIndex,
    live: &L,
) -> Result<Vec<NodeId>, DispatchError> {
    if let Some(pin) = c.pin_to_node {
        if !live.is_live(&pin) {
            return Err(DispatchError::PinnedNodeNotLive { node: pin });
        }
        candidates.retain(|n| *n == pin);
    }
    if let Some(scope_id) = &c.pin_to_scope {
        let owners = live.live_subset(&scopes.owners(scope_id));
        if owners.is_empty() {
            return Err(DispatchError::PinnedScopeUnowned {
                scope_id: scope_id.clone(),
            });
        }
        candidates.retain(|n| owners.contains(n));
    }
    // Tag filters (`require_tags` / `exclude_tags`) and `must_be_local`
    // operate on per-capability tag metadata that today (Phase 2.2) is not
    // routable: no built-in capability declares `local-only` or
    // `cloud_paid` yet. These stubs accept all candidates as-is so the
    // API surface is final and 3.6 can plug in without breaking callers.
    let _ = &c.require_tags;
    let _ = &c.exclude_tags;
    let _ = c.must_be_local;
    Ok(candidates)
}
