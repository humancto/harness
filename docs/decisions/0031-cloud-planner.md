# ADR-0031 — `brain.plan` Cloud backend + policy-driven escalation (5.2)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 5.2, PRD §15.1 (tier 3: Cloud), §15.2
(`local_first` order: LocalFast → LocalStrong → Cloud → Template;
"Cloud (if `cloud_ok`-tagged)"; `[mesh.planning.cloud]`), §10.4
(`allow_cloud_escalation`, `local_only_for_tags`), §10.5 (secrets).

## What ships

1. **`CloudBackend` (harness-brain, feature `cloud`).** Anthropic
   Messages API; the whole planner pipeline (prompt, JSON extraction,
   plan rewrite) is the same code as the local tiers — extracted from
   `local_fast.rs` into `llm_common.rs` (pure move). Tier knobs:
   id `cloud:<model>`, 60 s timeout (matches the `llm.cloud.claude`
   capability default), 16 KiB prompt cap (as `LocalStrong`),
   `max_tokens` 4096, temperature pinned 0.0 — planning wants
   determinism, not creativity. Errors map exactly like the local
   tiers (Timeout / Transport / Decode → executor advances with a
   diagnostic), so Template always remains reachable.

2. **Double-gated escalation ("policy-driven rules").**
   - *Policy approval is a registration cap:* the daemon registers
     the cloud tier only when `planning.allow_cloud_escalation =
     true` (default false) AND `planning.cloud_planner_model` is
     non-empty. Requests can never resurrect an unregistered tier —
     requests narrow policy, never widen it.
   - *Per-task opt-in (§15.2 "if `cloud_ok`-tagged"):* the
     `brain.plan` executor narrows `allow_cloud` to false unless the
     task carries the `cloud_ok` tag OR the request set
     `constraints.allow_cloud: true` explicitly. The explicit
     constraint is the programmatic equivalent of the tag — both are
     issuer-controlled, so treating them as interchangeable opt-ins
     adds no authority. Narrowing only: a false `allow_cloud` stays
     false. `harness plan|exec --cloud` is the CLI surface for the
     explicit constraint (Codex review P1: without it the cloud tier
     was unreachable from the primary CLI flows).
   - *In-backend gate:* `!allow_cloud || must_be_local` →
     `NoMatch` before any I/O, whatever lineup it sits in.

3. **`local_only_for_tags` — first enforcement.** Parsed since
   Phase 2, enforced nowhere. The `brain.plan` executor now forces
   `must_be_local = true` when the task's tags intersect the set
   (force-to-true only; an explicit `true` is never loosened). This
   is what makes the cloud gate policy-driven for `medical`/`legal`-
   style tagged tasks. Execution-side locality (where tasks *run*)
   is unchanged — this governs where they are *planned*.

4. **Key handling.** The API key never lives on the backend struct
   and never crosses a crate boundary as owned bytes: the daemon
   hands `CloudBackend` a closure that reads the vault
   (`secret/claude-api-key`), builds a `HeaderValue` from the
   borrowed `SecretValue::as_bytes()`, and marks it
   `set_sensitive(true)` before returning — same redaction wall as
   `llm.cloud.claude`. Missing/malformed key → an `Internal`
   diagnostic naming the TAG only; the chain degrades to Template.
   No new secret-handling *path* is introduced: the vault tag, the
   header construction, and the redaction pattern are the 3.6 ones.

5. **`cloud_planner_model` knob (harness-policy).** Default
   `claude-sonnet-5`. Deviation from the PRD's
   `[mesh.planning.cloud] default_model = "claude-sonnet-4-7"`
   example: that id does not exist in the Anthropic API, so the
   default tracks the current mid-tier model and operators override
   via policy. Empty string disables the tier without flipping the
   policy bit. No `default_provider` knob (anthropic-only in 5.2 —
   a one-value enum is noise) and no `escalation_only` knob
   (inherent in lineup position: cloud sits after both local tiers).

6. **Planning budget 180 s → 240 s** (`DEFAULT_PLAN_TIMEOUT_MS`):
   the worst-case chain is now 30 + 120 + 60 s + Template.

## Not in 5.2 (deliberate)

- **Escalation triggers** (`escalate_to_cloud_if =
  ["plan_validation_failed", "tool_not_found"]`) and per-chain
  deadline budgeting: 5.3.
- **Real cloud cost enforcement:** the cloud tier's
  `estimated_cost_usd` is LLM-self-reported (usually ~0), so the
  `validate_plan` cost cap is nominal for the one tier that costs
  money. 5.9's token-based cost tracking makes it real; until then
  the §5.8 budget work must not be considered done for cloud
  planning.
- **§15.2 planning-mode enum** (`policy = local_first | cloud_first
  | cloud_only | smart`) and the `[mesh.planning.cloud]` table
  shape: unimplemented repo-wide; the lineup hardwires
  `local_first`, the only mode the PRD elaborates. Recorded here so
  the drift is documented, not invented.
- **Other providers** (OpenAI/Gemini planner tiers): backlog; the
  provider-specific surface is ~80 lines of request/response
  mapping.
- **Streaming responses**: planning output is one JSON object;
  batch is fine at 60 s.

## Rejected

- Reusing the `llm.cloud.claude` *capability* as the planner tier:
  it is policy/batcher/vault-coupled, takes `model` as input, and
  returns capability JSON — the planner needs a config-driven model
  and `PlanOutcome`. Sharing happens one layer down (`llm_common`).
- A `harness-vault` dependency in `harness-brain`: layering leak;
  the closure inverts it.
- Gating registration on key presence: the key can arrive after
  boot (`HARNESS_SECRET_CLAUDE_API_KEY`); per-request resolution
  with a clean diagnostic degrades better.
