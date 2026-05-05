# ADR-0013 — `brain.plan` Template tier and planner runtime shape

**Status:** Accepted (2026-05-04)
**Context:** Phase 3.8 of `HARNESS_PRD_v2.md` / `ROADMAP.md`. PRD §15 defines the brain runtime; this ADR records the architectural decisions that shape the 3.8 implementation and constrain the 3.9 layering.
**Supersedes:** —
**Superseded by:** —

## 1. Why ship Template separately from LocalFast

PRD §15.1 defines a four-tier `PlannerBackend`: `LocalFast` / `LocalStrong` / `Cloud` / `Template`. Template is the lowest tier — pure pattern matching against a static prefix table, no LLM, no I/O, deterministic. Splitting Template (3.8) from LocalFast (3.9) lets the mesh ship a working `brain.plan` capability _before_ Ollama is on the critical path:

- 3.8 runs on a model-less laptop with no internet (PRD §15.6's stated goal).
- 3.9 layers LocalFast in front of Template; the trait, validation, and capability surface stay identical, only the backend lineup changes.
- Each PR is reviewable in isolation. Template + escalation framework + capability wiring is one shape; LocalFast + Ollama integration + JSON-Schema validation + cost-cap is another.

CLAUDE.md's "no half-implementations" gate applies: Template is not a stub, it is a **complete reference implementation** that emits valid plans for `run:` / `shell:` patterns and surfaces clear `MatchedButUnsupported` diagnostics for prefixes whose capabilities ship later.

## 2. `PlannerBackend` trait shape — four-state outcome

```rust
pub enum PlanOutcome {
    Confident(Box<PlanResponse>),
    NoMatch,
    MatchedButUnsupported {
        matched_pattern: &'static str,
        missing_capability: String,
    },
}
```

`Result<Option<PlanResponse>, _>` would collapse two distinct cases — "this backend cannot help" vs. "this backend matched but the capability is missing" — into a single `Ok(None)` and lose the diagnostic operators need. The four-state outcome:

- **`Confident`** short-circuits the escalation chain.
- **`NoMatch`** silently advances to the next tier.
- **`MatchedButUnsupported`** advances _with_ a diagnostic that the brain.plan executor accumulates and surfaces in the final `Failed("...; template matched 'fetch:' but http.fetch is not registered")` error if no later tier succeeds.
- **`Err(PlannerError)`** is reserved for transport failures (HTTP, decode, timeout); the executor treats those as "this tier didn't work" and advances, accumulating the error into the final diagnostic.

`PlanOutcome::Confident(Box<PlanResponse>)` boxes the response so the enum size stays bounded as `PlanResponse` grows in 3.9+.

## 3. Template confidence = 0.6, hardcoded

`pub const TEMPLATE_CONFIDENCE: f64 = 0.6;` exposed for tests and future tuning. Sits below `mesh.planning.confidence_threshold` (default 0.7 per PRD §15.2) so a 3.9 LocalFast backend returning 0.85+ wins the escalation chain when both are registered.

## 4. 3.8 has NO threshold check

The `BrainPlanCapability` executor accepts every `Confident(_)` outcome in 3.8 — no threshold gate. 3.9 introduces `mesh.planning.confidence_threshold` at the executor: a `Confident(_)` whose `confidence < threshold` is treated as `NoMatch` and escalation continues.

This means Template's 0.6 ships uncontested when LocalFast is not registered (3.8). It only becomes a "below threshold, escalate" outcome once 3.9 LocalFast lands. This is the right phasing — premature threshold enforcement in 3.8 would make Template inert.

ADR-0011 sets the precedent for "write the honest scope down."

## 5. `WeakCapabilityRegistry` for available-capabilities snapshot

`brain.plan` is a capability registered inside `CapabilityRegistry`. The capability needs to observe the registry's contents (to populate `available_capabilities` per `PlanRequest`). Strong reference would leak: registry holds the cap, cap holds the registry, neither refcount reaches zero.

`WeakCapabilityRegistry` (added to `harness-capabilities`) holds a `Weak<RwLock<HashMap<...>>>`. `CapabilityRegistry::downgrade()` produces one. The brain.plan capability captures the `Weak` in its `Arc<dyn Fn() -> Vec<CapabilityRef>>` closure. On daemon shutdown, the last `CapabilityRegistry` clone drops; the inner `Arc` reaches zero; `Weak::upgrade()` returns `None` and the closure yields `Vec::new()` — the capability surfaces a clean `Failed("no backend produced ...")` instead of leaking.

The narrow companion type (rather than promoting `CapabilityRegistry` to `Arc<CapabilityRegistry>` everywhere) keeps the existing `&CapabilityRegistry` calling convention used by every other enricher (`enrich_with_llm_local`, `enrich_with_llm_cloud_claude`).

## 6. `Unsigned<Plan>` newtype

The `Plan` envelope (`Plan::sig: Signature`) is built for signed, on-the-wire plans, and `Plan::verify_signature` rejects a zero signature. The planner runs _before_ signing — the dispatcher signs the plan it receives back from `brain.plan` before fan-out (PRD §13.5).

`Unsigned<T>` (in `harness-core::protocol::signable`) wraps any `Signable` value with a `#[must_use]` annotation that turns "treating an unsigned plan as authentic" into a compile-time warning. `Unsigned::sign(identity)` consumes the wrapper and returns the signed inner. `Unsigned::into_inner_unsigned()` projects out for serialization-only paths (the brain.plan capability serializes the inner `Plan` to JSON; the dispatcher decodes back into `Unsigned<Plan>` and then signs).

The wire shape on the brain.plan capability output is a JSON-encoded `Plan` with `sig = [0; 64]`. Downstream callers MUST decode as `Unsigned<Plan>` and sign before passing to anything that calls `verify_signature`.

## 7. `available_capabilities` as input override

PRD §15.5 explicitly describes a brain on `mac-mini` dispatching `brain.plan` to a `gpu-box` peer with the brain's own capability list rather than the peer's. The `BrainPlanCapability` input schema accepts an optional `available_capabilities` array; absent → snapshot the local registry via the closure. Shipping the schema field in 3.8 avoids a wire migration in 3.9.

## 8. `BrainPlanCapability` does NOT consult `PolicyEngine`

Planning is policy-blind by design (PRD §10.4). The executing node enforces, and the executing node may be a different node with a different policy than the brain. The capability does not hold an `Arc<PolicyEngine>` and the Template backend does not consult one. `shell.exec` policy gates at execute-time on the worker, where it must — not at plan-time on the brain.

## 9. Argv tokenization via `shlex` is NOT shell expansion

`shlex::split` handles POSIX quoting (single + double quotes, backslash escapes inside double quotes). Returns `None` on unclosed quotes — surface as `NoMatch` per the trait contract.

`$VAR`, backticks, redirections, pipes, `&&` / `;` chains — all pass through as **literal argv elements**. `shell.exec` invokes `Command::new(cmd)`, which never invokes `sh -c` and never performs shell expansion. The user explicitly asked for a command; the planner emits exactly that command.

`shlex 1.3.0` is the workspace pin (post-CVE-2024-fix release).

## 10. `PlanNode.capability_version_major` field is intentionally NOT added in 3.8

`validate_plan_well_formed` checks capability id only. 3.9 adds JSON-Schema validation against `Capability::input_schema`, which transitively pins the input shape per registered (id, version_major). Adding a `capability_version_major: Option<u16>` field to `PlanNode` is a separate plan-envelope evolution that needs its own ADR + wire-back-compat story.

## 11. `CapabilityRef` is a core protocol type, not a planner type

Lives in `harness-core::protocol::manifest` next to `Capability` because it describes "a capability available on a node" — a manifest-shaped concept with no brain-specific semantics. Same reasoning for `Unsigned<T>`, which lives in `harness-core::protocol::signable` next to the `Signable` trait it parameterizes.

This keeps `harness-capabilities::WeakCapabilityRegistry::refs()` brain-free; non-planning paths (e.g. a future `mesh.peers` introspection capability) use the same type without dragging `harness-brain` into their dep graph.

## 12. `WeakCapabilityRegistry` rather than promoting `CapabilityRegistry` to `Arc`

The latter would ripple through `LocalExecutor`, `ApiStateBuilder`, every `enrich_*` signature, and the daemon lifecycle. The narrower `WeakCapabilityRegistry` companion (downgrades the inner `Arc<RwLock<...>>`) keeps the existing `&CapabilityRegistry` calling convention and constrains the blast radius to one file. Operators get clean drop semantics: when the daemon drops the last `CapabilityRegistry` clone, the inner Arc reaches zero and the entire registry (including `BrainPlanCapability` and the `WeakCapabilityRegistry` it holds) drops.

## Consequences

- The brain.plan capability is operational on every node by default (Template does not require Ollama).
- `harness-brain` becomes the home for planner trait + types; 3.9's LocalFast lands as a sibling backend module without changing the trait.
- The capability output's `plan` field is _unsigned_; downstream consumers must decode as `Unsigned<Plan>` and sign with the dispatcher's identity before fan-out.
- `Action::Plan { goal }` policy variant — analogous to `Action::Llm { model }` — is intentionally NOT added in 3.8 because planning is policy-blind. If future versions add per-issuer plan gating (e.g. an admin-only flag for cloud escalation), that variant joins the policy enum then.
