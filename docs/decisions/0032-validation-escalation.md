# ADR-0032 — Full validation ruleset + escalation triggers (5.3)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 5.3, PRD §15.4 (five-rule validation; "retry
with stricter prompt, escalate tier, or error; invalid plans are
never executed"), §15.2 (`escalate_to_cloud_if`,
`max_replanning_attempts`, `escalation_only`), §10.4 (policy on the
executing node). Builds on 5.2's double gate (ADR-0031).

## What ships

1. **§15.4 rule 4 — `LocalityConflict`.** `validate_plan` gains a
   `cloud_caps` parameter (the snapshot's set of locally-registered
   cloud-tier capability ids: `CostHint::CloudPaid` or a `"cloud"`
   tag) and rejects any `must_be_local` plan that names one. Checked
   first — cheapest rule, clearest repair diagnostic. Foreign caps
   (§15.5 input-override) are never in the local set and are
   deliberately not flagged: we refuse only what we can prove
   conflicts, and the executing node's policy is the §10.4 backstop.
   `plan.execute`'s pre-flight validation passes an empty set (its
   constraints are `default()` — the rule is inert there; execution-
   side locality stays policy territory). With 5.2's
   `local_only_for_tags` forcing, a `medical`-tagged goal that plans
   a cloud capability now hard-fails validation on every tier — that
   is the point of §10.4, not a bug. `allow_cloud = false` plan
   CONTENT is not flagged: PRD rule 4 names only `must_be_local`;
   `allow_cloud` gates the planner tier, not plan content.

2. **Escalation triggers.** `PlanningPolicy.escalate_to_cloud_if` is
   a typed `Vec<CloudTrigger>` (`plan_validation_failed`,
   `tool_not_found`, `low_confidence`, `backend_error`) — a typo'd
   string fails policy load loudly (house rule), never a silent
   narrowing. The executor attempts the cloud tier only when an
   EARLIER local LLM tier produced a fired trigger:
   - `plan_validation_failed`: Confident plan failed `validate_plan`
     (an `UnknownCapability`/`UnknownSchema` failure ALSO fires
     `tool_not_found`);
   - `tool_not_found`: `MatchedButUnsupported`;
   - `low_confidence`: Confident below threshold;
   - `backend_error`: `Err(PlannerError)` of any kind.
   `NoMatch` is deliberately NOT a trigger — it means "this tier
   cannot help with this goal shape", not "this tier failed"
   (pinned by test). Triggers compose AFTER 5.2's double gate: a
   fired trigger invokes the backend, which still self-gates on
   `allow_cloud`/`must_be_local`. Triggers narrow, never widen.
   - **Default deviation from the PRD example:** §15.2's TOML lists
     only the first two strings, but production local tiers emit
     only `Confident` or `Err` — the two-string set would lock cloud
     out of every real local failure mode and regress 5.2's
     reachability, contradicting §15.3 ("until one returns a
     confident, validated plan"). The default is therefore ALL FOUR;
     operators narrow via the knob (the PRD pair remains
     expressible verbatim).
   - **Cloud-as-baseline (rule b):** when NO local LLM tier exists
     in the lineup, the trigger gate does not apply — there is
     nothing to escalate FROM, and a model-less mesh with cloud
     configured must still plan. This is the coherent reading of
     `[mesh.planning.cloud] escalation_only = true`: cloud never
     PREEMPTS a local tier; on a mesh without local tiers it is the
     baseline planner (still behind 5.2's policy cap + `cloud_ok`
     opt-in).

3. **Replanning (§15.4 "retry with stricter prompt").**
   `PlanRequest.repair: Option<String>`; on a validation failure the
   SAME tier is re-invoked with the error text rendered into the
   prompt (1 KiB char-safe cap — `SchemaViolation` embeds instance
   values and must not starve the capability list or burn paid
   tokens). `max_replanning_attempts` (PRD default 2) is read
   per-tier; the cloud tier is further capped at 1 retry — paid
   retries need a plausible ROI, and one repair attempt has it while
   unbounded ones do not (no spend accounting until 5.9/Phase 6).
   Low-confidence and `NoMatch` are never retried (escalation
   semantics, not repair); Template is deterministic and never
   retried.

4. **Chain planning budget (carried from 5.1/5.2 — resolved).**
   `BrainPlanConfig.chain_budget_ms: Option<u64>` (`None` =
   unbounded, the derived default, so direct-construction tests keep
   pre-5.3 behavior; daemon passes `Some(210_000)` = the 240 s CLI
   plan default minus Template + polling slack). LLM/cloud attempts
   may neither START past the budget nor RUN beyond it — each is
   wrapped in `tokio::time::timeout(budget − elapsed, …)`; expiry is
   a diagnostic, not an error, and the walk falls through. Template
   is exempt: the §15.2 floor always runs. A budget event fires no
   trigger (the same budget gates the cloud tier anyway). The budget
   is a Template floor within the DEFAULT CLI window; a caller
   passing a much smaller `--timeout-ms` can still see the task die
   mid-chain (pre-existing envelope behavior).

5. **Tier classification** is by backend id prefix (`cloud:` /
   `TEMPLATE_BACKEND_ID` / else-LLM). Stringly typed, but
   `PlannerBackend` is a trait object exposing only `id()`, and a
   `tier()` method would touch every impl for the same information.
   Unknown prefixes count as LLM tiers — conservative for trigger
   accounting, never eligible for the cloud gate.

## Not in 5.3 (deliberate)

- `max_replanning_attempts` read as per-CHAIN (the PRD is ambiguous;
  per-tier + the chain budget + the cloud cap bound the same risk
  and keep the knob's meaning local).
- Real cost-aware escalation (spend accounting): 5.8/5.9.
- `version_major` validation (`PlanNode` has no version field —
  ADR-0013 §10 / ADR-0014 §11 carry-forward).
- Execution-side locality for foreign caps: §10.4 policy on the
  executing node (unchanged).

## Rejected

- `NoMatch` as a trigger: would make every Template-shaped goal on a
  weak local model a paid cloud call.
- Free-string trigger knob with boot-time warn: violates the
  loud-fail config discipline; typed enum costs nothing.
- Per-attempt timeout changes (tier timeouts stay 30/120/60 s): the
  executor-side `timeout` wrap achieves the floor without touching
  backend internals.
