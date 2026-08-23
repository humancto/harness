# ADR-0030 — `brain.plan` LocalStrong backend (5.1)

**Status:** Accepted (2026-08-23)
**Context:** Roadmap 5.1, PRD §15.1 (tier 2: 32B–70B-class local
model), §15.2 (`local_first`: LocalFast → LocalStrong → Cloud →
Template; one `prefer_models` list spans the local tiers).

## What ships

1. **Shared local-LLM planner core.** `LocalFastBackend` (3.9) and the
   new `LocalStrongBackend` are thin newtypes over one private
   `LocalLlmCore` — identical Ollama plumbing, prompting, JSON
   extraction, and response mapping. The tiers differ in exactly three
   knobs: id prefix (`localfast:` / `localstrong:`), request timeout
   (30 s / `STRONG_TIMEOUT_MS` = 120 s — a 70B on consumer hardware
   streams slowly), and prompt byte cap (8 KiB / 16 KiB — strong
   models afford a fuller capability projection). `LocalFastBackend`'s
   public ctor and id scheme are byte-identical to 3.9; the whole 3.9
   wiremock suite exercises the shared core unchanged.

2. **Model-class detection.** `classify_local_model` parses the
   VERBATIM Ollama tag (quantization suffixes and all): split on `:`
   then `-`, match tokens case-insensitively as `<n>x<m>b` (MoE —
   effective n×m, so `mixtral:8x7b` = 56B → Strong; a judgment call,
   recorded here) or `<m>b` (decimals included — `qwen2:0.5b`).
   **Boundary: ≥ `STRONG_MIN_PARAMS_B` = 20** — PRD names 32B–70B as
   the class examples, not a fence; 20 keeps 22B/27B-class models
   (mistral-small, gemma2:27b) in tier 2 while 13B/14B stay tier 1.
   No size token (`phi-4`, `llama3:latest`) → Fast: conservative,
   never over-promises tier 2. Misclassification costs one escalation
   hop, never a wrong plan (validation gates every tier).

3. **Lineup partition.** `resolve_local_models` walks
   `policy.planning.prefer_local_models` against the registered
   `llm.local.*` set and takes the FIRST fast-class and FIRST
   strong-class matches → `[LocalFast?, LocalStrong?, Template]`
   (§15.2 order). **Behavior change on mixed lists**: 3.9 bound
   LocalFast to the first preferred model regardless of size — under
   the PRD default list that was the 70B, mislabeled as tier 1 with a
   30 s budget. 5.1's partition is the §15.2-correct reading
   (fast→8b, strong→70b). Fast-only meshes behave exactly as 3.9.

4. **Planning budget fix (plan review MAJOR-2).** The CLI's
   `obtain_plan` hardcoded a 60 s brain.plan budget and ignored
   `--timeout-ms`; a 120 s tier-2 attempt would have starved the
   Template fallback and turned graceful degradation into a task
   timeout. 5.1 wires `--timeout-ms` through planning and raises
   `DEFAULT_PLAN_TIMEOUT_MS` to 180 s (covers 30 s + 120 s + Template
   with slack). Per-chain deadline budgeting (subtracting elapsed time
   from later tiers) stays with 5.3's escalation-trigger work.

## Not in 5.1 (deliberate)

- **§15.5 remote planning** (brain routes its own thinking to a GPU
  peer): `brain.plan` is `Anyone`-cardinality, so TASK-level routing
  to an LLM-hosting peer already works; an in-backend remote hop is
  the 5.2 Cloud/remote surface.
- **Escalation triggers** (`plan_validation_failed`, `tool_not_found`
  → cloud) and per-chain budgets: 5.3.
- PRD §15.1's `avg_latency_ms`/`host` enum fields: already dropped by
  3.9's trait design (backends are node-local; latency tracking is a
  Phase 6 metric).

## Rejected

- Marker-type generics (`LocalLlmBackend<Fast>`): the newtype pair is
  simpler, keeps derives trivial, and the trait object erases the type
  anyway.
- A separate `prefer_strong_models` policy knob: one ordered list +
  classification is fewer knobs and matches §15.2's single
  `prefer_models`.
- Classifying by benchmark rather than tag: no local benchmark data
  exists; the tag heuristic is transparent and cheap, and the
  confidence threshold + validation absorb misclassification.
