//! Per-capability JSON-Schema index for plan-time input validation.
//!
//! Schemas are compiled **once** when the index is built (eager) so the
//! per-`PlanNode` validate path pays only the validation cost, not the
//! compile cost. The compiled [`jsonschema::Validator`] is `Send + Sync`
//! so a `CapabilitySchemaIndex` can be cloned cheaply (Arc-shared) and
//! handed to every backend in a single planning invocation.
//!
//! Failure to compile a schema is non-fatal at index-build time: the
//! offending capability still appears in the manifest (so the LLM
//! knows it exists and may emit nodes referencing it) but plans
//! referencing the cap fail validation with `UnknownSchema` rather than
//! crashing the index build. Adversarial / malformed schemas cannot
//! take down the planner.

use std::collections::HashMap;
use std::sync::Arc;

use jsonschema::Validator;
use serde_json::Value as JsonValue;

/// Snapshot of every available capability's `input_schema`, compiled
/// to a [`Validator`] eagerly. Built once per `brain.plan` invocation
/// and shared across the escalation chain.
#[derive(Clone, Default)]
pub struct CapabilitySchemaIndex {
    by_id: HashMap<String, Arc<Validator>>,
}

impl std::fmt::Debug for CapabilitySchemaIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapabilitySchemaIndex")
            .field("compiled_schemas", &self.by_id.len())
            .finish_non_exhaustive()
    }
}

impl CapabilitySchemaIndex {
    /// Build the index from `(capability_id, input_schema)` pairs.
    /// Schemas that fail to compile are dropped with a `tracing::warn!`;
    /// callers see the cap in `available_capabilities` (so the planner
    /// can list it to the LLM) but plans referencing it surface
    /// `UnknownSchema` at validate time.
    #[must_use]
    pub fn from_pairs(pairs: Vec<(String, JsonValue)>) -> Self {
        let mut by_id = HashMap::with_capacity(pairs.len());
        for (id, schema) in pairs {
            match jsonschema::validator_for(&schema) {
                Ok(v) => {
                    by_id.insert(id, Arc::new(v));
                }
                Err(err) => {
                    tracing::warn!(
                        capability = %id,
                        ?err,
                        "capability input_schema failed to compile; \
                         plans referencing this capability will fail validation"
                    );
                }
            }
        }
        Self { by_id }
    }

    /// Number of compiled schemas. Useful for tests asserting that
    /// compile happens once per pair (not once per `validate` call).
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Look up the compiled validator. Returns `None` if the capability
    /// has no schema (either because it was never registered, or because
    /// its schema failed to compile at index-build time).
    #[must_use]
    pub fn get(&self, cap_id: &str) -> Option<&Validator> {
        self.by_id.get(cap_id).map(Arc::as_ref)
    }

    /// Validate `instance` against the schema for `cap_id`.
    ///
    /// Returns `Ok(())` when valid, `Err(messages)` collecting **every**
    /// `ValidationError` from `Validator::iter_errors` (multi-error
    /// reporting so operators get one round-trip of debugging, not N).
    /// Returns `Err(vec!["unknown capability schema"])` when the cap
    /// isn't in the index — caller should map to
    /// `PlanValidationError::UnknownSchema { cap, .. }`.
    pub fn validate(&self, cap_id: &str, instance: &JsonValue) -> Result<(), Vec<String>> {
        let Some(validator) = self.get(cap_id) else {
            return Err(vec!["unknown capability schema".to_string()]);
        };
        let errors: Vec<String> = validator
            .iter_errors(instance)
            .map(|e| e.to_string())
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// Compile-time guarantee that `Arc<Validator>` (the index's storage
// shape) is shareable across async tasks. `jsonschema 0.26+` provides
// `Send + Sync` on `Validator`; this `const _` fails the build if a
// future bump regresses it.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Arc<Validator>>();
    assert_send_sync::<CapabilitySchemaIndex>();
};

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use serde_json::json;

    fn shell_exec_schema() -> JsonValue {
        json!({
            "type": "object",
            "required": ["cmd"],
            "additionalProperties": false,
            "properties": {
                "cmd":  { "type": "string", "minLength": 1 },
                "args": { "type": "array", "items": { "type": "string" } },
                "timeout_ms": { "type": "integer", "minimum": 1, "maximum": 60000 },
            },
        })
    }

    fn idx() -> CapabilitySchemaIndex {
        CapabilitySchemaIndex::from_pairs(vec![("shell.exec".into(), shell_exec_schema())])
    }

    #[test]
    fn t01_happy_path() {
        let i = idx();
        let r = i.validate("shell.exec", &json!({"cmd": "ls", "args": ["-la"]}));
        assert!(r.is_ok(), "got {r:?}");
    }

    #[test]
    fn t02_missing_required_field() {
        let i = idx();
        let r = i.validate("shell.exec", &json!({"args": []}));
        let Err(errs) = r else {
            panic!("expected Err");
        };
        assert!(
            errs.iter().any(|e| e.contains("cmd")),
            "errors should name the missing field; got {errs:?}"
        );
    }

    #[test]
    fn t03_wrong_type() {
        let i = idx();
        let r = i.validate("shell.exec", &json!({"cmd": 42}));
        assert!(r.is_err(), "got {r:?}");
    }

    #[test]
    fn t04_additional_properties_rejected() {
        let i = idx();
        let r = i.validate("shell.exec", &json!({"cmd": "ls", "extra": true}));
        assert!(r.is_err(), "got {r:?}");
    }

    #[test]
    fn t05_int_out_of_range() {
        let i = idx();
        let r = i.validate("shell.exec", &json!({"cmd": "ls", "timeout_ms": 99999}));
        assert!(r.is_err(), "got {r:?}");
    }

    #[test]
    fn t05a_compile_happens_once_per_pair() {
        // Eagerness invariant: N pairs in → N compiled validators in
        // the index. The `Drop` cost is not measurable but `len()`
        // proves storage; coupled with each `validate()` not
        // re-compiling, this gives the right invariant.
        let i = CapabilitySchemaIndex::from_pairs(vec![
            ("shell.exec".into(), shell_exec_schema()),
            ("echo".into(), json!({"type": "object"})),
            ("noisy".into(), json!({"type": "string"})),
        ]);
        assert_eq!(i.len(), 3);
        // Validate four times — the fourth call still uses the compiled
        // instance, no re-compile happens. Observable only via this
        // not exploding under any compile-time bound.
        for _ in 0..4 {
            assert!(i.validate("shell.exec", &json!({"cmd": "ls"})).is_ok());
        }
    }

    #[test]
    fn t05b_malformed_schema_is_dropped() {
        // A schema whose `type` field is invalid won't compile; index
        // builds anyway, validate() of that cap returns "unknown".
        let i = CapabilitySchemaIndex::from_pairs(vec![
            ("good".into(), json!({"type": "object"})),
            ("bad".into(), json!({"type": "this is not a valid type"})),
        ]);
        assert_eq!(i.len(), 1, "only the good schema compiled");
        let r = i.validate("bad", &json!({}));
        assert!(r.is_err());
    }

    #[test]
    fn t05c_unknown_cap_returns_unknown_schema_marker() {
        let i = idx();
        let r = i.validate("never.registered", &json!({}));
        let Err(errs) = r else {
            panic!("expected Err");
        };
        assert_eq!(errs, vec!["unknown capability schema".to_string()]);
    }
}
