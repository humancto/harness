# Harness PRD — v2 Addendum

> **Companion to:** `HARNESS_PRD.md` (v1)
> **Status:** Design rationale + new specifications, to be folded into v2 of the main PRD.
> **Audience:** the implementer (AI agent or human) — explains *why* v1 was insufficient and *what* v2 adds.

---

## 0. Why this exists

The v1 PRD defined a capable LAN agent harness, but three concerns surfaced after review that v1 either underspecified or treated as edge cases. They aren't edge cases — each one strikes at the product's central thesis.

| Concern | What v1 said | Why it's wrong | What v2 says |
|---|---|---|---|
| Where does planning happen? | "LLM-backed (cloud or local)" | Implicitly cloud-first. Breaks the privacy and offline thesis. | The brain *is* a local LLM by default. Cloud is escalation. |
| Who answers a search? | "Pick least-loaded eligible node." | Wrong for data-bound queries. A `fs.search` against one node is incomplete by definition. | Capability **cardinality** as a first-class field. Anyone / Owner / Federated. |
| What about brutal workloads? | Mentioned fan-out and DAGs. | Underspecified. Real intensive workloads need streaming dispatch, backpressure, checkpoints, batched local inference, real-time cost tracking. | First-class intensive-workload section. The product thesis is **N×X scaling**. |

The corrections below are not optional. They define what the product actually is.

---

## 1. Pillar One — Brain Runtime & Local-First Planning

### 1.1 The thesis correction

The brain is the part of the mesh that decides what to do. If the brain is a cloud LLM, then every user prompt goes to a third party — and the privacy story collapses. If the brain only works online, then the offline-resilience story collapses. If the brain costs money to think, then the cost story collapses.

**The brain must be local by default. Cloud LLMs are a tool the brain *uses* when needed, not the brain itself.**

This is technically reasonable: planning workloads (decompose a goal, pick capabilities, route inputs, merge results) are well within reach of a 7–14B local model. It's structured decomposition, not frontier reasoning. The hard parts that *do* benefit from frontier models (deep code generation, complex synthesis, novel reasoning) get dispatched as sub-tasks to a `llm.cloud.*` capability — which the brain calls the same way it'd call any other tool.

### 1.2 Planner backend tiers

The mesh advertises planner backends in a strict precedence order. The brain picks the highest-tier backend that's available and within policy.

```rust
enum PlannerBackend {
    LocalFast {                              // tier 1
        // 7B–14B class, low latency, lives on the brain node or nearby
        model: String,                        // "llama3.1:8b", "qwen2.5:7b", "phi-4"
        host: NodeId,
        avg_latency_ms: u32,
    },
    LocalStrong {                            // tier 2
        // 32B–70B class, on a tagged GPU node
        model: String,                        // "llama3.1:70b", "qwen2.5-coder:32b"
        host: NodeId,
        avg_latency_ms: u32,
    },
    Cloud {                                  // tier 3
        provider: CloudProvider,              // Anthropic | OpenAI | Google
        model: String,
        cost_per_1k_tokens: (f64, f64),       // (in, out)
    },
    Template,                                // tier 4 fallback
    // Hardcoded plans for trivial known patterns. Used when no LLM is reachable.
}
```

### 1.3 Planning policy (mesh-wide config)

```toml
[mesh.planning]
policy = "local_first"   # local_first | local_only | cloud_first | cloud_only | smart

# What "local_first" means:
# 1. If LocalFast can produce a confident plan (parsed + validated against capabilities),
#    use it.
# 2. Else if LocalStrong is available, use it.
# 3. Else if Cloud is available AND user request is tagged cloud_ok, use it.
# 4. Else fall back to Template.
# 5. Else: refuse the request with a clear "no planner available" error.

# "smart" routes per-request based on prompt complexity heuristics.
# "local_only" never escalates to cloud — useful for strict privacy modes.

confidence_threshold = 0.7   # LocalFast falls through to LocalStrong below this
max_replanning_attempts = 2
escalate_to_cloud_if = ["plan_validation_failed", "tool_not_found"]
```

A user can override per-request: `harness exec --planner cloud "..."` or `--planner local_only`.

### 1.4 The `brain.plan` capability

A built-in, every-node capability with multiple backends:

```rust
struct PlanRequest {
    goal: String,                            // natural-language user goal
    available_capabilities: Vec<CapabilityRef>, // injected by brain
    constraints: PlanConstraints,            // budget, deadline, privacy tier
    context: Option<Value>,                  // recent results, user prefs, etc.
}

struct PlanResponse {
    plan: Plan,                              // DAG (see §13 of main PRD)
    confidence: f64,                         // 0.0–1.0
    rationale: String,                       // human-readable explanation
    estimated_cost_usd: f64,
    estimated_duration_ms: u64,
    fallback_plan: Option<Plan>,             // if primary plan fails
}
```

The capability has multiple implementations registered in priority order; the brain calls them in tier sequence until one returns a confident, validated plan.

**Validation rules** (run after every planner response):

- Every task references a capability that exists in `available_capabilities`.
- Every task's input matches the referenced capability's input schema.
- DAG is acyclic.
- No task's `must_be_local` constraint conflicts with a `cloud`-tagged dependency.
- Estimated cost ≤ `constraints.max_cost_usd`.

If validation fails, the brain either retries with a stricter prompt, escalates a tier, or returns an error. It does **not** execute an invalid plan.

### 1.5 Brain election: weighted by planning capability

v1 used "highest node_id wins." That's fine for tie-breaking but ignores that some nodes are *much* better suited to be the brain.

**v2 weighted election**:

```
brain_score(node) =
    base_score(node_id)              // small deterministic tiebreak
  + 100  * has_local_fast_planner    // "I can plan without help"
  + 200  * has_local_strong_planner  // "I can plan complex things without help"
  + 50   * has_cloud_planner_keys    // "I can escalate"
  + 30   * cpu_cores
  + 50   * (ram_gb / 16)
  - 100  * is_battery_powered_unplugged   // laptops on battery should not lead
  - 200  * was_brain_recently_demoted     // anti-flap
  - 1000 * recent_planner_error_rate      // penalize unreliable brains
```

Highest score wins. Election still converges in ~2s on disagreement. The election is announced in heartbeats; ties broken by node_id.

**Anti-flap**: a node that was demoted in the last 60s gets a -200 penalty to prevent oscillation when two nodes are close in score.

**Battery awareness**: a laptop on battery (detected via `IOPMCopyPSStateInfo` on macOS, `/sys/class/power_supply/` on Linux) is automatically deprioritized. When plugged in, the penalty disappears and election may transfer back.

### 1.6 Routing the brain's *own* thinking

Crucially, the brain node ≠ the LLM-host node. A brain on a small Mac mini can route its planning thoughts to a 4090-equipped peer:

```
User goal
    ↓
Brain (mac-mini) receives request
    ↓
Brain dispatches `brain.plan` task to gpu-box (which has llama3.1:70b loaded)
    ↓
gpu-box returns Plan
    ↓
Brain validates Plan
    ↓
Brain dispatches Plan's tasks across the mesh
```

The brain is the *coordinator* of planning, not necessarily the *executor* of planning. This decouples leadership from raw compute, which is the whole point of having a mesh.

### 1.7 Configuration & secrets

Brain-specific config (`~/.harness/brain.toml`):

```toml
[planning]
preferred_backend = "local_strong"
fallback_chain = ["local_strong", "local_fast", "cloud", "template"]
max_plan_size = 50         # nodes in a single DAG
max_replan_depth = 3       # how many times we'll regenerate a sub-plan

[planning.cloud]
default_provider = "anthropic"
default_model = "claude-sonnet-4-7"
escalation_only = true     # never use cloud unless escalated to

[planning.local]
prefer_models = ["llama3.1:70b", "qwen2.5:32b", "llama3.1:8b"]
batch_concurrent = true    # combine concurrent planning calls if possible
```

### 1.8 What this implies for the implementer

- Ship with `llm.ollama.*` capabilities auto-detected at startup.
- Ship with a default planner prompt template optimized for ~8B models that can't handle long, fuzzy instructions. Be prescriptive in the system prompt.
- Ship a `brain.plan.template` fallback that handles ~10 well-known patterns (file search, web research, batch summarize, etc.) without any LLM at all. This makes the mesh *useful* even on a model-less laptop with no internet.
- Document a recommended "minimum viable brain" hardware spec so users know what to expect.

---

## 2. Pillar Two — Capability Cardinality & Federated Execution

### 2.1 The thesis correction

v1 routed every task as if any eligible worker would do. That's correct for stateless capabilities (`llm.claude`, `http.fetch`) but **wrong** for data-bound capabilities. Some examples of how v1 fails:

- `fs.search "term sheet"` dispatched to one node returns *that node's* hits. The user thinks they searched everywhere; they didn't.
- `git.log --since=monday` against a repo that lives on one machine returns empty from any other machine.
- `photos.recent` dispatched to a random node returns nothing useful.

v2 introduces **capability cardinality** as a first-class field. Every capability declares whether it's stateless, owner-bound, or federated.

### 2.2 The `Cardinality` enum

```rust
enum Cardinality {
    /// Any eligible node can answer; result is independent of which node ran it.
    /// Examples: llm.claude, http.fetch, text.translate.
    /// Routing: pick the least-loaded eligible node.
    Anyone,

    /// Only nodes that own the relevant data scope can answer.
    /// The capability declares which input field carries the scope.
    /// Each node manifests which scopes it owns.
    /// Examples: fs.search (scope: directory), git.log (scope: repo path).
    /// Routing: dispatch to the specific node(s) owning the named scope.
    Owner {
        scope_field: String,          // which input key carries the scope
    },

    /// Every relevant node executes; results are merged.
    /// No single node has the complete answer.
    /// Examples: mesh.search, mesh.grep, federated.embed_lookup.
    /// Routing: fan out to all eligible nodes, merge results.
    Federated {
        merge: MergeStrategy,
        on_node_failure: PartialPolicy,
    },
}
```

This field lives in the `Capability` struct (which is part of the node manifest). The dispatcher reads it and routes accordingly. **No more guessing.**

### 2.3 Scope ownership

Each node advertises *what it owns* alongside *what it can do*. v1's manifest gets a new field:

```rust
struct NodeManifest {
    // ... existing fields ...
    capabilities: Vec<Capability>,
    scopes: Vec<Scope>,               // NEW
}

struct Scope {
    kind: String,                     // "directory", "repo", "photo_library", "mailbox"
    id: String,                       // canonical identifier (path, URL, etc.)
    label: String,                    // human-readable
    indexed: bool,
    last_indexed: Option<Timestamp>,
}
```

Examples:

```toml
[[scope]]
kind = "directory"
id = "/Users/archy/Documents"
label = "macbook-archy:Documents"
indexed = true

[[scope]]
kind = "repo"
id = "/Users/archy/dev/atom-tickets/showtimes-spa"
label = "showtimes-spa @ macbook-archy"
indexed = true
```

When the brain receives `fs.search { scope: "macbook-archy:Documents", query: "term sheet" }`, it routes to `macbook-archy` and only `macbook-archy`. When it receives `fs.search { query: "term sheet" }` (no scope), it treats this as a *federated* search and fans out.

### 2.4 Merge strategies

For `Cardinality::Federated`, results from multiple nodes need to be combined. The capability declares its preferred strategy; the caller can override.

```rust
enum MergeStrategy {
    /// Concatenate result arrays. Trivial.
    Concat,

    /// Concatenate then dedupe by a key field.
    Dedupe { key: String },

    /// Sort by a numeric score field (descending) and take top K.
    TopK { k: usize, score_field: String },

    /// Run a reranker capability over the union (typically an LLM).
    Rerank { reranker_capability: String, top_k: usize },

    /// Numeric aggregation (sum, avg, min, max) for metric-style results.
    Aggregate { op: AggregateOp, field: String },

    /// Caller provides a custom merger as a follow-on capability call.
    Custom { capability: String },
}
```

For search: typically `Rerank` with a small local LLM, falling back to `TopK` by relevance score.
For grep: `Concat` — every line is a result.
For metrics: `Aggregate`.
For dedupe-able lists: `Dedupe { key: "file_hash" }`.

### 2.5 Federated task lifecycle

```
SUBMITTED → DISCOVERED → DISPATCHED → STREAMING → MERGING → DONE
                              ↓                       ↓
                       (per-node fan-out)     (partial results
                                                 may stream to
                                                 caller as they
                                                 arrive)
```

Concretely:

1. Brain receives federated task.
2. Brain queries the capability index: which nodes advertise this capability + the scope?
3. Brain dispatches sub-tasks to all eligible nodes, in parallel, with `lease_ms` and `timeout_per_node`.
4. As each node returns, the brain emits a partial result envelope with `progress: { complete: M, total: N }` to the caller — UX win, results visible immediately.
5. When all nodes return or `global_timeout` fires (whichever first), the brain runs the merge strategy.
6. Final merged result is signed and broadcast.
7. Per-node successes/failures are recorded in the result envelope's `provenance` field.

```rust
struct FederatedResult {
    task_id: TaskId,
    output: Value,                    // merged result
    provenance: Vec<NodeContribution>,
    completed_nodes: u16,
    failed_nodes: u16,
    timed_out_nodes: u16,
    merge_strategy: MergeStrategy,
}

struct NodeContribution {
    node_id: NodeId,
    status: NodeStatus,                // Ok | Err | Timeout
    duration_ms: u64,
    item_count: usize,
}
```

The user sees this in the UI: "Searched 3 nodes, 12 results from macbook-archy, 5 from thinkpad-archy, nas timed out after 5s."

### 2.6 The natural-language routing problem

The hardest part: when a user types "find that contract from Tuesday," how does the planner decide between `fs.search` (Owner, requires scope) and `mesh.search` (Federated)?

**Heuristic** (encoded in the planner system prompt):

1. If the user names a specific machine ("on my work laptop", "in the NAS"), use Owner with that scope.
2. If the user names a specific scope ("in my Documents folder", "in the showtimes repo"), use Owner with that scope.
3. Otherwise, default to Federated with `Rerank` merge.

The user can always force: `harness search --on macbook-archy --scope ~/Documents "term sheet"`.

### 2.7 Built-in `mesh.*` capabilities

v2 adds a family of explicitly-federated capabilities so users (and planners) have a clear default for "ask everyone":

| Capability | Cardinality | Notes |
|---|---|---|
| `mesh.search` | Federated, Rerank | Free-text search across all indexed scopes |
| `mesh.grep` | Federated, Concat | Pattern match across all indexed text |
| `mesh.find_file` | Federated, Concat | Locate a file by name/glob across all nodes |
| `mesh.embed_lookup` | Federated, TopK | Vector similarity across all embedding stores |
| `mesh.stat` | Federated, Aggregate | Resource stats (disk, memory, GPU usage) |
| `mesh.exec` | Federated, Concat | Same as `shell.exec --all` but typed |

These are thin wrappers over their scoped counterparts (`fs.search`, `fs.grep`, etc.) that set Cardinality to Federated and choose a sensible merge.

### 2.8 What this implies for the implementer

- Add `Cardinality` field to `Capability` struct.
- Add `scopes: Vec<Scope>` to `NodeManifest`.
- Routing in the scheduler branches three ways based on cardinality.
- Implement merge strategies as a small library (`harness-merge` crate).
- Streaming partial results requires the result envelope to support a "partial" variant.
- The `mesh.search` family of capabilities is built on top of the scoped ones — implement scoped first, federated wrappers second.

---

## 3. Pillar Three — Intensive Workload Support (N×X Scaling)

### 3.1 The thesis correction

v1 acknowledged fan-out and DAGs. v2 makes the **N×X scaling thesis** an explicit product goal: a mesh's compute capability for embarrassingly parallel work should approach Σ(per-node capability), and the framework should actively shift work toward that form whenever possible.

> Mesh capability ≈ N × X
>   where N = number of healthy nodes
>         X = average per-node capability for the workload class

This is the line that should appear on the README. It's what the harness *gives* you that you don't have today.

For this to be real and not aspirational, six specific capabilities must exist.

### 3.2 Streaming chunk dispatch

**Problem:** a user submits 100k items for processing. Materializing 100k tasks at submission time blows memory, slows brain election, and makes failure recovery expensive.

**Solution:** dispatch in waves. Keep ~`2 × N` tasks in flight per worker; refill as workers complete.

```rust
struct FanoutController {
    inputs: Box<dyn Stream<Item = TaskInput>>,    // input source (file, DB, API)
    capability: String,
    in_flight_target_per_node: usize,             // default 2
    on_completion: Box<dyn Fn(TaskResult)>,       // streaming callback
    on_progress: Box<dyn Fn(Progress)>,
    checkpoint: Option<CheckpointConfig>,
}
```

The controller maintains a sliding window: when a node completes a task, it gets the next input from the stream. Memory usage stays O(N × in_flight_target_per_node), not O(total inputs).

### 3.3 Result streaming, not buffering

**Problem:** for a 100k-item job, the caller doesn't want the result returned as a single 500MB JSON blob.

**Solution:** the result of a fan-out is a `Stream<TaskResult>` from the caller's perspective. The brain emits results as they complete; the caller consumes them incrementally.

API surface:

```rust
// CLI
harness fanout --capability doc.summarize --input ./pdfs/*.pdf \
    --output ./summaries/{input_basename}.txt \
    --concurrency-per-node 4 \
    --progress

// Library
let mut stream = mesh.fanout(spec).await?;
while let Some(result) = stream.next().await {
    // write each result as it arrives
    match result {
        Ok(r) => write_summary(r)?,
        Err(e) => log_error(e),
    }
}
```

The UI consumes the same stream over WebSocket, updating progress bars and partial result tables in real time.

### 3.4 Resource-aware scheduling

**Problem:** v1's scheduler scored on queue depth. That's wrong for mixed workloads. A node running one CPU-pinned ML inference is full; a node running 30 network-bound HTTP fetches isn't.

**Solution:** tasks declare resource hints; nodes track multidimensional load.

```rust
struct ResourceHints {
    cpu_class: CpuClass,            // Light | Heavy | Pinned
    memory_mb: Option<u32>,
    gpu_required: bool,
    gpu_memory_mb: Option<u32>,
    network_class: NetworkClass,    // None | Light | Heavy
    disk_io_class: DiskIoClass,     // None | Light | Heavy
    estimated_duration_ms: Option<u32>,
}

struct NodeLoad {
    cpu_busy_pct: u8,
    cpu_pinned_count: u8,
    ram_used_mb: u32,
    ram_total_mb: u32,
    gpu_used_mb: u32,
    gpu_total_mb: u32,
    network_in_flight: u16,
    disk_io_pending: u16,
}
```

Scheduler scoring becomes multidimensional:

```
fit_score(node, task) =
    can_fit_cpu(node, task)        // hard bool
  * can_fit_memory(node, task)     // hard bool
  * can_fit_gpu(node, task)        // hard bool
  * (1 - cpu_pressure_after)
  * (1 - memory_pressure_after)
  * (1 - gpu_pressure_after)
```

### 3.5 Backpressure across the pipeline

**Problem:** producer faster than consumer → unbounded queues → OOM.

**Solution:** every internal channel is bounded. When a downstream stage is full, upstream producers block (or, for fan-out, pause polling new inputs).

Specifically:

- Per-node task queue: bounded (default 64). When full, the node broadcasts `paused = true` in heartbeats; brain stops dispatching.
- Result aggregation channel: bounded. Brain's merger blocks if downstream consumer (caller stream) is slow.
- Fan-out controller: respects worker queue depth before pulling next input.
- Log streams: bounded ring buffer; old lines dropped if reader is slow (with a "logs lossy" warning).

This eliminates the most common distributed-systems failure mode: cascading OOM under load.

### 3.6 Checkpoint and resume

**Problem:** a 100k-item job that fails at item 67k should resume from 67k, not restart.

**Solution:** plan-level checkpointing using deterministic input hashes.

```rust
struct CheckpointConfig {
    enabled: bool,
    interval_items: usize,          // checkpoint every N completed
    storage: CheckpointStorage,     // Sqlite | File | None
    input_hash_fn: HashFn,          // default blake3 of canonical-encoded input
}
```

On crash/restart:

1. Brain (or replacement brain) loads the plan's checkpoint.
2. For every input in the source stream, compute its hash.
3. If hash exists in the result table, skip (already done).
4. Otherwise, dispatch.

This makes intensive jobs **safe to interrupt**. Close your laptop, the mesh keeps going. Power cycle the brain, the new brain resumes where the old one left off.

### 3.7 Local-LLM batching

**Problem:** a node hosting Ollama or vLLM serializes requests by default. A 4090 is *much* more efficient at `batch_size=8` than `batch_size=1`.

**Solution:** the `llm.local.*` capability has a built-in micro-batcher:

- Incoming requests for the same model wait up to `batch_window_ms` (default 50ms) for siblings.
- When the window closes or `max_batch_size` (default 16) is reached, requests are batched and sent to the inference engine in one call.
- Per-request latency increases by ~50ms in exchange for ~3–8× throughput on batched-friendly hardware.

Configurable per-capability:

```toml
[[capability]]
id = "llm.local.llama70b"
batch_window_ms = 100
max_batch_size = 8
disable_batching_for = ["interactive"]   # tag
```

The brain knows to mark interactive planning calls with `tag: interactive` so they aren't batched.

### 3.8 Real-time cost tracking

**Problem:** a mixed local/cloud workload can quietly spend $40 in tokens before the user notices.

**Solution:** running cost dashboard with budget enforcement.

- Every task carries an `estimated_cost` and reports `actual_cost` on completion.
- The brain maintains a running total per plan, per user, per day.
- A `budget` field on the plan: `max_cost_usd`. Exceeded → plan paused, user notified, "continue?" prompt.
- UI dashboard in the Runs page: live cost graph, projected total, big red stop button.

```rust
struct Budget {
    max_cost_usd: Option<f64>,
    soft_limit_usd: Option<f64>,    // warn at this; pause at hard limit
    on_exceed: BudgetAction,        // Pause | Cancel | Notify
}
```

Per-mesh defaults can be set: "no plan may exceed $5 without explicit approval."

### 3.9 Brutal-mode use cases (concrete demos)

Use cases that *only* make sense with the harness's intensive-workload features:

1. **Embed 100k documents.** Single laptop: 8h. 4 laptops with batched embeddings: <2h.
2. **Grade 5,000 LLM outputs against a rubric.** Single: 3h serial Claude calls. 4 laptops sharing the API key with rate-limit awareness: ~45 min.
3. **Run Llama 70B over 1000 prompts** for offline analysis. One GPU box serial: hours. Two GPU boxes batched: minutes.
4. **Triage 50 GitHub issues** with parallel agent loops, one agent per issue. Single: 50× serial agent time. Mesh: ~constant time.
5. **Compile + test a monorepo** across multiple targets. Single: 30 min. Distributed: 8 min.
6. **Process a 10TB photo library** — face detection, OCR, EXIF analysis. Pipeline parallelism with GPU on one node, CPU encoding on another, NAS storage on a third.
7. **Background nightly batch jobs** — ingest receipts, summarize emails, generate weekly reports. While you sleep.

Each of these is a single command at the harness level:

```bash
# Embed 100k docs
harness fanout --capability embed.batch --input ./corpus/**/*.txt \
    --output embeddings.sqlite --concurrency-per-node 4 --checkpoint

# Grade outputs
harness fanout --capability llm.grade --input outputs.jsonl \
    --rubric rubric.md --max-cost 50.00

# Triage issues
harness exec "Triage all open issues in atom-tickets/showtimes-spa, label by type and priority."
```

### 3.10 Benchmark targets

To make the N×X thesis concrete, v2 declares performance targets:

| Workload class | Single-node baseline | 2-node target | 4-node target | Notes |
|---|---|---|---|---|
| Embarrassingly parallel (CPU) | 1.0× | ≥ 1.85× | ≥ 3.5× | Linear-ish |
| Embarrassingly parallel (Cloud LLM) | 1.0× | ≥ 1.9× | ≥ 3.7× | API rate limits permitting |
| Embarrassingly parallel (Local LLM) | 1.0× | ≥ 1.95× | ≥ 3.8× | If both nodes have GPU |
| Mixed pipeline (DAG with deps) | 1.0× | ≥ 1.4× | ≥ 2.2× | Bottlenecked by serial sections |
| Single sequential job | 1.0× | 1.0× | 1.0× | No speedup possible |

If we can't hit these on the canonical demos, the product thesis isn't real. The CI suite must include these benchmarks.

### 3.11 What this implies for the implementer

- Build streaming dispatch from day one — never materialize all sub-tasks.
- Every task spec includes ResourceHints; every node tracks multidimensional load.
- Bounded channels everywhere; backpressure tested.
- Checkpoint storage is a first-class component, not an afterthought.
- LLM micro-batching is built into `llm.local.*`, not an optimization.
- Cost tracking is a first-class data type.
- Benchmark suite runs in CI; regressions fail the build.

---

## 4. Inline edits to v1 PRD

The following sections of `HARNESS_PRD.md` v1 need updates. Listed as a diff log.

### §4.1 Goals — add

- **N×X scaling for embarrassingly parallel workloads.** Mesh throughput approaches Σ(per-node capability) for parallel-friendly tasks.
- **Local-first planning.** Brain functions without internet by default; cloud LLMs are escalation, not baseline.
- **Federated capability execution.** Searches and scans across all data-holding nodes are first-class.

### §6 Use cases — append

Append the seven brutal-mode use cases from §3.9 above as a sub-section "Intensive workloads (N×X scaling)."

### §7 Glossary — add

- **Cardinality** — a capability's routing class: Anyone, Owner, or Federated.
- **Scope** — a node's claim to ownership of a data domain (directory, repo, mailbox, etc.).
- **Federated task** — a task that runs on every eligible node and merges results.
- **Brain runtime** — the local-LLM stack the brain uses for planning.
- **Planner backend** — a specific implementation of `brain.plan` (LocalFast, LocalStrong, Cloud, Template).

### §12 Leader Election — replace algorithm

Replace v1's "highest node_id wins" with the weighted brain election from §1.5 above. Anti-flap and battery-awareness are mandatory.

### §13 Protocol — additions

- Add `cardinality: Cardinality` to `Capability`.
- Add `scopes: Vec<Scope>` to `NodeManifest`.
- Add `resource_hints: ResourceHints` to `Task`.
- Add `budget: Option<Budget>` to plans.
- Add `partial: bool` and `progress: Progress` to result envelope.

### §14 Task Execution Model — additions

- New §14.6: **Federated execution** (full lifecycle from §2.5 above).
- New §14.7: **Streaming dispatch and result streams** (from §3.2–3.3).
- New §14.8: **Resource-aware scheduling** (from §3.4).
- New §14.9: **Backpressure and bounded channels** (from §3.5).
- New §14.10: **Checkpoint and resume** (from §3.6).

### §15 Built-in Capabilities — additions

Add the `mesh.*` family from §2.7. Add `brain.plan` with multi-backend implementation. Update `llm.local.*` to include built-in batching.

### §15 → renumber and add new §16

Insert a new section between Built-in Capabilities and Web UI:

**§16 Brain Runtime & Planner** — full content of §1 above.

(Old §16 Web UI becomes §17, etc.)

### §17 Web UI — additions

- Mesh page: show planner backend status per node (which models are loaded, batch queue depth).
- Submit page: planner-backend selector ("auto / local-fast / local-strong / cloud").
- Runs page: live cost graph + budget remaining, per-node contribution breakdown for federated tasks.
- Settings page: planning policy config, budget defaults, scope management.

### §21 Build phases — updates

- **Phase 2** adds: cardinality field on capabilities; scopes in manifest.
- **Phase 3** adds: `brain.plan` with at least Template + LocalFast backends; `llm.local.*` batching; mesh.search + mesh.grep.
- **Phase 4** adds: streaming dispatch; result streaming; resource-aware scheduling; basic backpressure.
- **Phase 5** adds: checkpoint/resume; budget enforcement; federated lifecycle with partial progress.
- **Phase 6** adds: weighted brain election; battery awareness; benchmark suite.

### §25 Open questions — add

- How does the planner discover capability schemas at scale (50+ capabilities)? Inline in the prompt, retrieval, or fine-tuned router model?
- For Federated tasks with very many eligible nodes (10+), do we sample or always dispatch to all?
- Cross-mesh federated search (v2 product feature): how to handle scope namespace collisions between meshes?

---

## 5. Migration notes for the implementer

If implementation has already begun against v1:

1. **No protocol breakage required** — the new fields (`cardinality`, `scopes`, `resource_hints`, etc.) can be added as optional with sensible defaults (`Cardinality::Anyone`, empty scopes, default ResourceHints). Older nodes treat unknown fields as opaque.
2. **The brain election change is non-breaking** — old nodes always get `base_score` only, so they participate fairly until upgraded. Once everyone is on v2, weighted scoring takes effect.
3. **The `brain.plan` capability replaces v1's monolithic planner.** If the v1 planner exists as code, refactor it as the `LocalFast` backend.
4. **Streaming dispatch replaces v1's batch fan-out.** This is a behavior change visible to users; document it as a feature, not a regression.
5. **Phase ordering remains unchanged.** v2 additions slot into existing phases as listed above; no phase needs to be re-done.

---

## 6. Single-paragraph summary for the README

> Harness is a local-first agent mesh for the machines you already own. A small Rust binary on each laptop, desktop, and server auto-discovers its peers, elects a brain (powered by your local LLM, with cloud as escalation), and turns your idle compute into a private agent fleet. Tasks are typed, capability-routed, and parallelized — searches federate across every node that has data, while stateless work runs wherever there's headroom. Intensive workloads (embed 100k docs, grade 5k LLM outputs, triage 50 issues) scale linearly with node count: N machines × X each. Nothing leaves your network unless you say so. Install it on two laptops, click pair, and your mesh is alive.

That paragraph is the product. Everything in the PRD exists to make it true.

---

*End of v2 addendum. Fold into `HARNESS_PRD.md` for v2 release.*
