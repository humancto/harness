# Harness — A LAN-Native Agent Mesh (v2)

> **Working codename: `harness`** — final naming open. ("Open Claw" is taken.)
> **Document type:** Product Requirements Document + Architecture Specification
> **Audience:** an autonomous coding agent (Claude Code) implementing the system end-to-end, plus humans reviewing the design.
> **Version:** v2 — consolidates v1 + v2 addendum into a single artifact.

---

## CHANGELOG (v1 → v2)

v2 makes three structural changes that v1 underspecified:

1. **Brain runtime is local-first.** The planner defaults to a local LLM. Cloud is escalation, not baseline. (§15)
2. **Capability cardinality is first-class.** Every capability declares `Anyone | Owner | Federated`. (§14.6)
3. **N×X scaling is an explicit thesis.** Streaming dispatch, result streams, resource-aware scheduling, backpressure, checkpoint/resume, batched local inference, real-time cost tracking. (§17)

Inline edits flow through Goals (§4), Protocol (§13), Execution Model (§14), Capabilities (§16), UI (§18), and Build Phases (§23).

---

## 1. Executive Summary

**Harness is a single Rust binary you install on every laptop, desktop, and server you own. It auto-discovers its peers on the LAN, forms a self-healing mesh, and turns your idle machines into a private agent swarm — with a *local* LLM as the brain by default.**

Any node can submit work. Any node can execute it. One node is automatically elected as the *brain* (the planner/dispatcher); leadership re-elects in seconds if the brain goes offline. The brain plans using a local LLM (Llama, Qwen, etc.); cloud LLMs are an escalation path, not the baseline. External clients (mobile apps, WhatsApp webhooks, iOS Shortcuts, browsers) talk to whichever node answers — the mesh routes internally. Tasks are typed, capability-routed, parallelized across nodes, and auditable through an Airflow-style web UI on `:19198`.

Searches federate: ask once, every relevant machine contributes, results merge. Intensive workloads scale: N machines × X capability each.

The product solves four problems no existing tool solves together:

1. **Idle compute is wasted.** Every household and small team has 3–10 capable machines doing nothing most of the time.
2. **Agent workloads are I/O-bound and parallelism-starved.** Single-machine agent loops are slow when they don't have to be.
3. **Privacy-sensitive agent work has no good story.** Cloud agent platforms see everything; LAN-only fleets currently require ops effort.
4. **Federated retrieval over personal data is unsolved.** Search across your machines is harder than search across the public web.

Harness collapses the operational cost of running a private agent fleet to "install a binary on each machine, click pair." After that, your machines work as one.

---

## 2. Problem Statement

### 2.1 The user's reality

A typical technical user has 2–5 personal computers all on the same LAN, all with similar software stacks, only one in active use at a time, and a growing set of agentic workloads they'd like to run.

### 2.2 What goes wrong today

- Long-running agent jobs pin the laptop the user is currently using.
- Cloud agents can't see local files or run local tools at scale.
- Local agents only see one machine.
- Distributed frameworks (Ray, Celery, Airflow) are heavy, ops-shaped, and not designed for personal multi-laptop use.
- A search across "all my machines" requires SSH-and-grep on every one.

### 2.3 What "good" looks like

- One install per machine, no further per-machine setup.
- One UI from any node or mobile.
- Tasks go to *the mesh*, not to a specific machine.
- Wall-clock for parallel-friendly workloads drops linearly with node count.
- Searches cover all data-holding nodes by default.
- The brain works without internet.
- No data leaves the LAN unless the user explicitly opts in.

---

## 3. Vision & Thesis

> Harness is **MCP at the network layer**, with a **local-LLM brain** and **federated capabilities** — a substrate that turns your existing machines into a coherent agent fleet.

Two-sentence thesis:

1. **The harness, not the agent, is the missing layer in personal AI infrastructure.**
2. **Mesh capability ≈ N × X.** For embarrassingly parallel work, throughput approaches Σ(per-node capability).

---

## 4. Goals & Non-Goals

### 4.1 Goals

- **Zero-config onboarding** — install + pair + done.
- **Self-healing mesh** with automatic leader election.
- **Local-first planning** — brain functions without internet by default.
- **Identity-first security** — every node has Ed25519 keys; every message signed.
- **Capability-typed routing with cardinality** — Anyone / Owner / Federated routing is automatic.
- **N×X scaling** for parallel workloads.
- **Single-binary deploy** — no broker, no DB server, no Kubernetes.
- **Privacy by default** — `local-only` tag honored.
- **Observable** — trace ID per task, audit log, live UI.
- **Useful on a single machine** — multi-node is an upgrade, not a prerequisite.

### 4.2 Non-Goals (v1)

- WAN/internet-scale routing (LAN-bound; cross-LAN is v2 via Tailscale/iroh).
- Multi-tenant SaaS hosting.
- Replacing Kubernetes/Ray/Celery for cloud workloads.
- Strong consistency beyond task lifecycle.
- Native mobile apps (web UI is mobile-responsive in v1).

---

## 5. Target Users

| Persona | Setup | Primary use |
|---|---|---|
| **Solo developer** | 2 laptops + Mac mini | Distributed code agents, parallel test runs |
| **AI tinkerer** | Gaming PC (4090) + MacBook + NAS | Hybrid local/cloud LLM workflows, intensive batch |
| **Privacy-sensitive professional** | Work + personal laptop, no cloud | Federated search/summarization over local files |
| **Small team / family** | 3–6 personal devices | Shared capabilities, household automation |
| **SMB owner** | Office of 5–20 machines | Internal RAG over local data, cost-controlled gateway |

---

## 6. Use Cases

### 6.1 Foundational

1. **Parallel research** — "Research these 10 companies" → 10 parallel sub-tasks
2. **Bulk transformation** — "Summarize these 200 PDFs" → fan-out
3. **Mixed local/cloud pipelines** — local Llama filters → Claude analyzes top results (~95% cost cut)
4. **Background fleet work** — submit from phone, picks up overnight
5. **Long-running refactors** — dispatch to non-active laptop
6. **Fleet exec** — "Update Ollama on every node"
7. **External trigger** — WhatsApp → mesh → text reply
8. **Specialization routing** — "Run on the GPU box"
9. **Graceful degradation** — node closes mid-task, auto-requeue

### 6.2 Federated retrieval

10. **Mesh-wide file search** — federated to all nodes with `fs.search`
11. **Mesh-wide grep** — across all repos on all machines
12. **Federated semantic lookup** — vector similarity across all embedding stores
13. **Mesh-wide stats** — federated aggregate

### 6.3 Intensive workloads (N×X scaling)

14. **Embed 100k documents** — 4 laptops with batched embeddings: <2h vs 8h single
15. **Grade 5,000 LLM outputs** — 4 laptops sharing API key: ~45 min vs 3h
16. **Run Llama 70B over 1000 prompts** — two GPU boxes batched
17. **Triage 50 GitHub issues** — parallel agent loops, near-constant time
18. **Compile + test monorepo** across multiple targets in parallel
19. **10TB photo library processing** — pipeline parallelism across CPU/GPU/storage
20. **Background nightly batch jobs** — receipts, emails, weekly reports

### 6.4 (v2) Cross-mesh

21. **Cross-mesh federation** — two homes' meshes via Tailscale; family shares capabilities

---

## 7. Glossary

- **Node** — a machine running the `harness` daemon
- **Mesh** — the set of mutually-trusted nodes
- **Brain** — the elected leader (planner/dispatcher)
- **Brain runtime** — the local-LLM stack the brain uses
- **Planner backend** — implementation of `brain.plan` (LocalFast / LocalStrong / Cloud / Template)
- **Capability** — a typed unit of work
- **Cardinality** — routing class: Anyone / Owner / Federated
- **Scope** — a node's claim to ownership of a data domain
- **Task** — instance of a capability invocation
- **Federated task** — runs on every eligible node, results merged
- **Plan / DAG** — directed acyclic graph of tasks
- **Manifest** — node's signed declaration of identity, capabilities, scopes, resources
- **Heartbeat** — periodic signed liveness packet
- **Pairing** — one-time human-mediated mesh-join
- **Worker** — role any node assumes when executing
- **Gateway** — capability tag for nodes that bridge to paid cloud services
- **Resource hints** — task's declaration of CPU/GPU/memory/network needs

---

## 8. System Architecture

### 8.1 Topology

```
                 ┌──────────────────────┐
   external      │   External clients    │
   ────────►     │  (mobile, WhatsApp,   │
                 │   browser, CLI)       │
                 └──────────┬────────────┘
                            │ HTTPS :19198
                            ▼
   ┌────────────────────────────────────────────────┐
   │                LAN MESH (mDNS + QUIC)          │
   │   ┌──────────┐    ┌──────────┐   ┌──────────┐  │
   │   │  Node A  │◄──►│  Node B  │◄─►│  Node C  │  │
   │   │ (brain)  │    │  worker  │   │  worker  │  │
   │   │ +llm 8B  │    │ +llm 70B │   │ +data    │  │
   │   └──────────┘    └──────────┘   └──────────┘  │
   │       ▲                ▲              ▲        │
   │       │  signed gossip │  signed gossip│       │
   │       └────────────────┴───────────────┘       │
   └────────────────────────────────────────────────┘
```

The brain may use peer LLMs for hard reasoning. Brain location ≠ LLM location.

### 8.2 Layered architecture (per-node)

```
┌─────────────────────────────────────────────────────────┐
│                    Web UI (embedded)                    │
├─────────────────────────────────────────────────────────┤
│  HTTP API (axum)         │     CLI (clap)               │
├──────────────────────────┴──────────────────────────────┤
│                    Orchestration Layer                  │
│  Brain runtime · Planner · DAG executor · Scheduler ·   │
│  Federation merger · Policy engine · Cost tracker       │
├─────────────────────────────────────────────────────────┤
│                    Capability Layer                     │
│  shell · llm.{local,cloud} · mcp · fs · http · mesh.* · │
│  brain.plan · schedule · embed · ...                    │
├─────────────────────────────────────────────────────────┤
│                       Mesh Layer                        │
│  Identity · Discovery (mDNS) · Transport (QUIC) ·       │
│  Heartbeats · Weighted election · Gossip · Replication  │
├─────────────────────────────────────────────────────────┤
│                    Persistence Layer                    │
│  SQLite (tasks, audit, capability index, traces, costs, │
│           checkpoints)                                  │
└─────────────────────────────────────────────────────────┘
```

### 8.3 Roles

Every node is simultaneously: discovery participant, worker, state replica, front door.
Exactly one is also: brain.
Optionally: planner host (has local LLM), data host (owns scopes), cloud gateway (holds API keys).

---

## 9. Onboarding & Installation

> **The onboarding *is* the product. If a user ever needs to SSH into a node, the design has failed.**

### 9.1 Install (per machine, once)

```bash
curl -sSL https://get.harness.sh | sh
```

The installer detects OS/arch, drops a signed binary at `/usr/local/bin/harness`, creates `~/.harness/` (mode 0700), generates an Ed25519 keypair, installs a launchd/systemd unit for auto-start with restart-on-crash, starts the daemon, and prints the next command.

Alt paths: `brew install harness`, `cargo install harness-cli`, `.deb`/`.rpm`, single-binary download.

### 9.2 First-node bootstrap

```
$ harness init
✓ Identity created (node_id: 7f3a9c…)
✓ Hostname:           macbook-archy
✓ Mesh created:       "archy-home"
✓ Admin password:     k7Qm-9Rxz-Wp3a
✓ Web UI:             http://192.168.1.42:19198
✓ Pairing code:       4271-9384  (valid 10 min)

Detecting capabilities...
✓ shell, git, python detected
✓ Ollama running with: llama3.1:8b, qwen2.5:7b
✓ Planner backend: LocalFast (llama3.1:8b on this node)

Mesh is live. Open the UI to add more nodes.
```

### 9.3 Joining additional nodes

```
$ harness join
🔍 Scanning LAN…
   Found mesh "archy-home" advertised by macbook-archy

? Pairing code: 4271-9384
✓ Submitted pairing request
⏳ Waiting for approval on macbook-archy…
✓ Approved as "thinkpad-archy"
✓ Capabilities auto-detected: shell, ollama, git, rust, python, gpu
✓ Ollama: llama3.1:70b loaded → registered as Planner backend (LocalStrong)
✓ Brain election will reweight (this node has stronger LLM)
✓ Web UI: http://192.168.1.78:19198
```

### 9.4 Auto-detection at startup

- Binaries on PATH → shell capabilities
- `nvidia-smi` exit 0 → `gpu` tag
- Ollama running → `llm.local.<model>` per `ollama list`
- API keys in env or `~/.harness/secrets` → `llm.cloud.*`
- MCP configs in `~/.harness/mcp/` → `mcp.<server>.<tool>`
- Common dirs (`~/Documents`, `~/dev/*`, `~/Photos`) suggested as scopes

User reviews and approves the proposed manifest in the UI before publish.

### 9.5 Day-2 ops

`harness peers`, `harness status`, `harness logs -f`, `harness scopes add`, `harness leave`, `harness update` (rolling).

---

## 10. Identity, Trust & Security

### 10.1 Identity
Each node generates Ed25519 keypair at install. `node_id = blake3(pubkey)[..16]`. Stored in `~/.harness/identity.key` mode 0600.

### 10.2 Trust model
Trust file `~/.harness/peers.toml` lists peer pubkeys + tier (`trusted` / `default` / `guest`). Joining = pairing-code-approved exchange of pubkeys, gossiped to all members. Unsigned/untrusted messages dropped at transport layer.

### 10.3 Wire security
QUIC with Noise/TLS, one connection per peer pair, multiplexed streams. Every app-level message Ed25519-signed over canonical CBOR encoding. Heartbeats include monotonic seq for replay protection.

### 10.4 Policy engine
Per-node `~/.harness/policy.toml` declares what's executable remotely:

```toml
[shell]
allow = [
  { cmd = "ls", any_args = true },
  { cmd = "git", subcmds = ["status", "log", "diff", "pull", "fetch"] },
  { cmd = "ollama", any_args = true },
  { cmd = "cargo", subcmds = ["build", "test", "check"] },
]
deny = [
  { pattern = "rm -rf /" },
  { cmd = "sudo", any_args = true },
]

[shell.from]
"macbook-archy" = "trusted"
"*"             = "default"

[capability]
default_local_only = false
require_2fa_for    = ["shell.exec", "fs.write"]

[planning]
allow_cloud_escalation = true
local_only_for_tags    = ["medical", "legal"]
```

**Policy is evaluated on the executing node, not the dispatcher.** Brains cannot override worker policy.

### 10.5 Secrets
Stored in `~/.harness/secrets.enc`, encrypted with key derived from identity + admin password. Replicated encrypted across mesh. Tagged (`secret/claude-api-key`, etc.). Capabilities reference by tag; raw values never on the wire.

### 10.6 Audit log
Every privileged action (dispatch, shell exec, secret access, peer approval, policy change, cloud escalation) → append-only audit log replicated to every node. Tamper-evident via hash chain. Viewable in History tab.

---

## 11. Discovery & Networking

### 11.1 Discovery
mDNS service `_harness._tcp.local`, port 19198 for HTTP, separate port for QUIC. TXT record advertises mesh_name, node_id, pubkey_fingerprint, version. Static peer list fallback. (v2) iroh DHT for cross-LAN.

### 11.2 Transport
QUIC (quinn) for node-to-node. HTTP/HTTPS on `:19198` for UI and external clients. One persistent QUIC connection per peer pair, bidirectional streams per active task.

### 11.3 Why QUIC
Multiplexed streams without head-of-line blocking; built-in encryption; 0-RTT reconnect after WiFi blips (laptops sleep/wake constantly); UDP-based, easier in NAT/Tailscale scenarios.

---

## 12. Leader Election (Brain Selection)

### 12.1 Algorithm
Weighted bully-style protocol. Every heartbeat carries `leader_belief` + `brain_score`. Highest score wins (ties broken by node_id). Convergence ~2–3s on startup or leader timeout (>6s missed heartbeats).

### 12.2 Brain scoring

```
brain_score(node) =
    base_score(node_id)              // small deterministic tiebreak
  + 100  * has_local_fast_planner
  + 200  * has_local_strong_planner
  + 50   * has_cloud_planner_keys
  + 30   * cpu_cores
  + 50   * (ram_gb / 16)
  - 100  * is_battery_powered_unplugged
  - 200  * was_brain_recently_demoted   // 60s anti-flap
  - 1000 * recent_planner_error_rate
```

Properties: planner-aware, power-aware, anti-flap, deterministic, no split-brain on connected LAN. On partition: each side elects own brain; reconciles via vector-clock-latest-wins per task.

### 12.3 What the brain owns
Planner runtime, dispatch decisions, result aggregation, in-flight task state (replicated), cost tracker.

### 12.4 What the brain does NOT own
Local task policy (workers retain authority), secrets storage (replicated), audit log (append-only), LLM hosting (separate role).

### 12.5 Brain handover
New brain claims in-flight from local replica, gossips its URL, UI updates within ~2s, external clients see no break. Checkpointed plans resume on new brain (§17.6).

---

## 13. Protocol Specification

All messages CBOR-encoded, Ed25519-signed.

### 13.1 Heartbeat (broadcast every 2s, ~280 bytes)

```rust
struct Heartbeat {
    node_id: NodeId,                 // [u8; 16]
    seq: u64,
    timestamp: u64,
    queue_depth: u16,
    cpu_busy_pct: u8,
    cpu_pinned_count: u8,
    ram_used_mb: u32,
    ram_total_mb: u32,
    gpu_used_mb: u32,
    gpu_total_mb: u32,
    capabilities_hash: [u8; 16],
    in_flight: Vec<TaskId>,
    leader_belief: NodeId,
    brain_score: i32,
    on_battery: bool,
    paused: bool,                    // backpressure signal
    version: SemVer,
    sig: [u8; 64],
}
```

### 13.2 Capability manifest (gossiped on change)

```rust
struct NodeManifest {
    node_id: NodeId,
    hostname: String,
    pubkey: [u8; 32],
    capabilities: Vec<Capability>,
    scopes: Vec<Scope>,
    resources: Resources,
    online_since: u64,
    version: SemVer,
    sig: [u8; 64],
}

struct Capability {
    id: String,
    version: SemVer,
    cardinality: Cardinality,
    input_schema: JsonSchema,
    output_schema: JsonSchema,
    cost_hint: CostHint,
    tags: Vec<String>,
    rate_limit: Option<RateLimit>,
    resource_hints: ResourceHints,
}

enum Cardinality {
    Anyone,
    Owner { scope_field: String },
    Federated { merge: MergeStrategy, on_node_failure: PartialPolicy },
}

struct Scope {
    kind: String,                    // "directory", "repo", "photo_library", "mailbox"
    id: String,
    label: String,
    indexed: bool,
    last_indexed: Option<u64>,
}

struct ResourceHints {
    cpu_class: CpuClass,             // Light | Heavy | Pinned
    memory_mb: Option<u32>,
    gpu_required: bool,
    gpu_memory_mb: Option<u32>,
    network_class: NetworkClass,     // None | Light | Heavy
    disk_io_class: DiskIoClass,
    estimated_duration_ms: Option<u32>,
}
```

### 13.3 Task envelope

```rust
struct Task {
    id: TaskId,                      // Uuid v7 (sortable)
    parent: Option<TaskId>,
    plan_id: Option<PlanId>,
    capability: String,
    input: serde_json::Value,
    constraints: Constraints,
    retry: RetryPolicy,
    execution: ExecutionPolicy,
    resource_hints: ResourceHints,
    trace_ctx: TraceContext,         // OpenTelemetry W3C
    issued_by: NodeId,
    issued_at: u64,
    sig: [u8; 64],
}

struct Constraints {
    deadline: Option<u64>,
    max_cost_usd: Option<f64>,
    must_be_local: bool,
    require_tags: Vec<String>,
    exclude_tags: Vec<String>,
    pin_to_node: Option<NodeId>,
    pin_to_scope: Option<String>,
}

struct ExecutionPolicy {
    redundancy: u8,                  // 1 normal, 2 speculative
    timeout_ms: u32,
    on_partial: PartialPolicy,       // FailFast | ReturnPartial | Wait
    lease_ms: u32,
}
```

### 13.4 Result envelope

```rust
enum TaskResult {
    Final(FinalResult),
    Partial(PartialResult),           // for federated/streaming
}

struct FinalResult {
    task_id: TaskId,
    node_id: NodeId,
    started_at: u64,
    finished_at: u64,
    status: Status,
    output: serde_json::Value,
    cost: Cost,
    logs: Vec<LogLine>,
    provenance: Vec<NodeContribution>,
    sig: [u8; 64],
}

struct NodeContribution {
    node_id: NodeId,
    status: NodeStatus,
    duration_ms: u64,
    item_count: usize,
}

struct Cost {
    tokens_in: u64,
    tokens_out: u64,
    usd: f64,
    wall_ms: u64,
    node_id: NodeId,
}
```

### 13.5 Plan envelope

```rust
struct Plan {
    id: PlanId,
    name: String,
    tasks: HashMap<TaskId, PlanNode>,
    edges: Vec<(TaskId, TaskId)>,
    budget: Option<Budget>,
    checkpoint: Option<CheckpointConfig>,
    issued_by: NodeId,
    sig: [u8; 64],
}

struct Budget {
    max_cost_usd: Option<f64>,
    soft_limit_usd: Option<f64>,
    on_exceed: BudgetAction,         // Pause | Cancel | Notify
}
```

### 13.6 Logical channels

```
harness.announce              # new node manifests
harness.heartbeat.<node_id>   # liveness
harness.task.offer            # available task
harness.task.bid.<task_id>    # workers respond
harness.task.assign.<task_id> # brain picks one
harness.task.lease.<task_id>  # worker extends lease
harness.task.result.<task_id> # final
harness.task.partial.<task_id># partial / streaming
harness.task.log.<task_id>    # streaming logs
harness.gossip.state          # CRDT diffs
harness.audit                 # append-only
harness.cost                  # running cost updates
```

---

## 14. Task Execution Model

### 14.1 Lifecycle

```
SUBMITTED → PLANNED → DISPATCHED → CLAIMED → RUNNING → DONE
                                       │         │
                                       └► EXPIRED └► FAILED → (retry?)
```

### 14.2 Cardinality-driven routing

- **Anyone** — least-loaded eligible node by score.
- **Owner { scope_field }** — read scope from input; route to nodes owning that scope.
- **Federated { merge, on_node_failure }** — fan out to all eligible; merge.

### 14.3 Scheduler scoring

```
fit_score(worker, task) =
    can_fit_cpu(worker, task)      // hard
  * can_fit_memory(worker, task)   // hard
  * can_fit_gpu(worker, task)      // hard
  * (1 - cpu_pressure_after)
  * (1 - memory_pressure_after)
  * (1 - gpu_pressure_after)
  * success_rate_recent
  / cost_weight
```

### 14.4 Distribution patterns

Fan-out (data parallel), DAG (topological), work-stealing (workers pull), Federated (§14.6).

### 14.5 Failure handling

Lease expiry → re-dispatch. Retry policy. Idempotency via task_id. Speculative execution (`redundancy=2`) → first wins. Circuit breaker (5 consecutive fails → 60s bench). Partial results respected.

### 14.6 Federated execution lifecycle

```
SUBMITTED → DISCOVERED → DISPATCHED → STREAMING → MERGING → DONE
```

1. Brain queries capability index for nodes with capability + relevant scope.
2. Dispatches sub-tasks to all eligible in parallel.
3. As each returns, brain emits `PartialResult` with `progress` to caller.
4. When all return or `global_timeout` fires, runs merge strategy.
5. Final merged result signed and broadcast.
6. Per-node successes/failures in `provenance`.

**Merge strategies:**

```rust
enum MergeStrategy {
    Concat,                                            // grep-style
    Dedupe { key: String },                            // by hash, by id
    TopK { k: usize, score_field: String },
    Rerank { reranker_capability: String, top_k: usize },
    Aggregate { op: AggregateOp, field: String },
    Custom { capability: String },
}
```

Defaults: `mesh.search` → Rerank; `mesh.grep` → Concat; `mesh.find_file` → Dedupe by blake3; `mesh.embed_lookup` → TopK; `mesh.stat` → Aggregate.

### 14.7 Streaming chunk dispatch

For fan-out: never materialize all sub-tasks. `FanoutController` keeps `2 × N_workers` in flight; refill on completion. Memory O(window), not O(total).

### 14.8 Result streaming

Caller sees `Stream<TaskResult>`:

```rust
let mut stream = mesh.fanout(spec).await?;
while let Some(result) = stream.next().await {
    match result {
        Ok(r) => write_summary(r)?,
        Err(e) => log_error(e),
    }
}
```

UI consumes same stream over WebSocket.

### 14.9 Resource-aware scheduling

Tasks declare `ResourceHints`; nodes track multidimensional load (CPU/RAM/GPU/network/disk). A node with one CPU-pinned ML inference is "full" for further pinned tasks but not for network-bound ones.

### 14.10 Backpressure & bounded channels

Per-node task queue bounded (default 64); when full, broadcast `paused = true`. Result aggregation channels bounded. Fan-out controller respects worker queue depth. Log streams ring-buffered. **Prevents cascading OOM.**

### 14.11 Checkpoint & resume

```rust
struct CheckpointConfig {
    enabled: bool,
    interval_items: usize,
    storage: CheckpointStorage,      // Sqlite | File | None
    input_hash_fn: HashFn,           // default blake3 of canonical-encoded input
}
```

On crash/restart: load checkpoint, hash each input, skip if hash exists in result table, dispatch otherwise. **A 100k-item job is safe to interrupt.**

---

## 15. Brain Runtime & Planner

The brain is the part of the mesh that decides what to do. v2 makes it **local-first**: planning runs on a local LLM by default; cloud is escalation.

### 15.1 Planner backend tiers

```rust
enum PlannerBackend {
    LocalFast {                              // tier 1
        model: String,                        // "llama3.1:8b", "qwen2.5:7b", "phi-4"
        host: NodeId,
        avg_latency_ms: u32,
    },
    LocalStrong {                            // tier 2
        model: String,                        // "llama3.1:70b", "qwen2.5-coder:32b"
        host: NodeId,
        avg_latency_ms: u32,
    },
    Cloud {                                  // tier 3
        provider: CloudProvider,
        model: String,
        cost_per_1k_tokens: (f64, f64),
    },
    Template,                                // tier 4 fallback
}
```

### 15.2 Planning policy

```toml
[mesh.planning]
policy = "local_first"   # local_first | local_only | cloud_first | cloud_only | smart
confidence_threshold = 0.7
max_replanning_attempts = 2
escalate_to_cloud_if = ["plan_validation_failed", "tool_not_found"]

[mesh.planning.cloud]
default_provider = "anthropic"
default_model = "claude-sonnet-4-7"
escalation_only = true

[mesh.planning.local]
prefer_models = ["llama3.1:70b", "qwen2.5:32b", "llama3.1:8b"]
batch_concurrent = true
```

`local_first`: try LocalFast → LocalStrong → Cloud (if `cloud_ok`-tagged) → Template → refuse. User overrides per-request: `harness exec --planner cloud "..."`.

### 15.3 The `brain.plan` capability

```rust
struct PlanRequest {
    goal: String,
    available_capabilities: Vec<CapabilityRef>,
    constraints: PlanConstraints,
    context: Option<Value>,
}

struct PlanResponse {
    plan: Plan,
    confidence: f64,
    rationale: String,
    estimated_cost_usd: f64,
    estimated_duration_ms: u64,
    fallback_plan: Option<Plan>,
}
```

Multiple implementations registered in priority order; brain calls them in tier sequence until one returns a confident, validated plan.

### 15.4 Plan validation

Run after every planner response:
- Every task references a capability in `available_capabilities`.
- Every input matches the referenced capability's schema.
- DAG acyclic.
- No `must_be_local` constraint conflicts with cloud-tagged dependency.
- Estimated cost ≤ `constraints.max_cost_usd`.

Validation failures → retry with stricter prompt, escalate tier, or error. **Invalid plans are never executed.**

### 15.5 Routing the brain's own thinking

Brain node ≠ LLM-host node. Brain on a small Mac mini can route planning to a 4090-equipped peer:

```
User goal → Brain (mac-mini) receives → Brain dispatches `brain.plan`
to gpu-box (llama3.1:70b loaded) → gpu-box returns Plan → Brain validates
→ Brain dispatches Plan's tasks across mesh
```

The brain *coordinates* planning; doesn't have to *execute* it.

### 15.6 Template fallback

For trivial known patterns, ship hardcoded plan templates that work without any LLM:
- File search → `mesh.search { query: <user words> }`
- Web fetch → `http.fetch { url: <extracted URL> }`
- Batch summarize → `fanout(doc.summarize, inputs: <files>)`
- Run command → `shell.exec { cmd: <quoted command> }`
- Schedule → `schedule.cron { spec: <parsed cron> }`

Template fallback makes the mesh useful **on a model-less laptop with no internet.**

### 15.7 Planner prompt requirements

For ~8B models: prescriptive system prompt with exact JSON schema, 3–5 worked examples, output constrained to JSON, capability list inline (or retrieval if >50), explicit `confidence` and `rationale` fields.

---

## 16. Built-in Capabilities

### 16.1 `shell.exec` — fleet exec

The flagship built-in.

**Cardinality:** `Anyone` by default; `--all` selector makes it federated in practice.

```rust
struct ShellExec {
    cmd: Vec<String>,
    cwd: Option<PathBuf>,
    env: HashMap<String, String>,
    stdin: Option<Bytes>,
    timeout_ms: u32,
    capture: CaptureMode,
}
```

Selectors: `--all`, `--on <node>`, `--where '<expr>'` (e.g. `tag:gpu`, `os:linux`, `cap:llm.claude`).

Sub-features: streaming output (line-frames on QUIC, `[node-name]` prefixes interleaved), stdin piping, file staging (`--upload`), persistent workspaces (`harness ws`), detached jobs (`--detach`), result piping across nodes.

Strictly governed by `policy.toml`. Default deny.

### 16.2 LLM capabilities

**Local:** `llm.local.<model>` auto-registered per Ollama model. Built-in **micro-batcher**: requests for same model wait up to `batch_window_ms` (default 50) for siblings, dispatch in batched call. ~3–8× throughput on batched-friendly hardware. Configurable; `tag:interactive` requests opt out of batching.

**Cloud:** `llm.cloud.claude`, `llm.cloud.openai`, `llm.cloud.gemini`.

**Embeddings:** `llm.embed.<model>` for both local + cloud.

Unified `LlmRequest` / `LlmResponse` shape.

### 16.3 `brain.plan`

Multi-backend planner per §15.

### 16.4 MCP proxy

`mcp.proxy` connects to MCP servers (subprocess or remote) and exposes their tools as harness capabilities. Auto-registered from `~/.harness/mcp/*.toml`. Each tool becomes `mcp.<server>.<tool>`.

### 16.5 Filesystem

**Scoped (Owner):**
- `fs.list { scope, path }`
- `fs.read { scope, path }`
- `fs.search { scope, query }`
- `fs.write { scope, path, content }` — `trusted` tier required
- `fs.index { scope }` — Tantivy/Sqlite-FTS index

**Federated wrappers:**
- `mesh.search { query }` → fan-out + Rerank
- `mesh.grep { pattern }` → fan-out + Concat
- `mesh.find_file { name_glob }` → fan-out + Dedupe by hash

### 16.6 HTTP

`http.fetch`, `http.post` (rate-limited, audited), `http.webhook` (receives external triggers).

### 16.7 Schedule

`schedule.cron` — recurring task scheduler. Stored in replicated state; brain triggers.

### 16.8 Mesh meta-capabilities

| Capability | Cardinality | Notes |
|---|---|---|
| `mesh.search` | Federated, Rerank | Free-text across all indexed scopes |
| `mesh.grep` | Federated, Concat | Pattern match across all indexed text |
| `mesh.find_file` | Federated, Dedupe | Locate file by name/glob |
| `mesh.embed_lookup` | Federated, TopK | Vector similarity across embedding stores |
| `mesh.stat` | Federated, Aggregate | Resource stats |
| `mesh.exec` | Federated, Concat | Same as `shell.exec --all` but typed |

### 16.9 Plan composition

`plan.compile`, `plan.execute`.

---

## 17. Intensive Workload Support (N×X Scaling)

### 17.1 Thesis

```
Mesh capability ≈ N × X
   N = number of healthy nodes
   X = average per-node capability for the workload class
```

This is the README headline. Everything in this section exists to make it true.

### 17.2 Streaming chunk dispatch — see §14.7
### 17.3 Result streaming — see §14.8
### 17.4 Resource-aware scheduling — see §14.9
### 17.5 Backpressure — see §14.10
### 17.6 Checkpoint & resume — see §14.11
### 17.7 Local-LLM batching — see §16.2

### 17.8 Real-time cost tracking

- Every task carries `estimated_cost`; reports `actual_cost`.
- Brain maintains running total per plan / user / day.
- `Budget` field on plan; on exceed: pause / cancel / notify.
- UI Costs page: live spend graph, projected total, big red stop button.
- Per-mesh defaults: "no plan may exceed $5 without explicit approval."

### 17.9 Benchmark targets

| Workload class | Single-node | 2-node | 4-node | Notes |
|---|---|---|---|---|
| Embarrassingly parallel (CPU) | 1.0× | ≥ 1.85× | ≥ 3.5× | Linear-ish |
| Embarrassingly parallel (Cloud LLM) | 1.0× | ≥ 1.9× | ≥ 3.7× | Rate limits permitting |
| Embarrassingly parallel (Local LLM) | 1.0× | ≥ 1.95× | ≥ 3.8× | If both have GPU |
| Mixed pipeline (DAG with deps) | 1.0× | ≥ 1.4× | ≥ 2.2× | Bottlenecked by serial sections |
| Single sequential job | 1.0× | 1.0× | 1.0× | No speedup possible |

CI suite includes these. Regressions fail the build.

### 17.10 Implementer checklist

- Streaming dispatch from day one — never materialize all sub-tasks
- `ResourceHints` on every task spec
- Bounded channels everywhere; backpressure tested
- Checkpoint storage is first-class
- LLM micro-batching built into `llm.local.*`
- Cost tracking is a first-class data type
- Benchmark suite runs in CI

---

## 18. Web UI

Served on `:19198` from every node, single-page app embedded in the binary.

### 18.1 Login
Username `admin` (single-user v1), password set at `harness init` (replicated salted hash). Cookie + WebSocket after login. (v2) OAuth, SSO, multi-user.

### 18.2 Page 1 — Mesh
Live topology. Card per node: hostname, status (green/yellow/red), CPU%, RAM, GPU, queue depth, capabilities count, scopes count, latency to brain, role badge, **planner backend status** (which models loaded, batch queue depth). Brain badge visible. Heartbeat pulse animation. Hover/click → drilldown. "Add node" → fresh pairing code with QR.

### 18.3 Page 2 — Submit
Three modes:
1. **Natural language** — textarea + planner-backend selector ("auto / local-fast / local-strong / cloud / template"). Shows proposed DAG; user previews and confirms.
2. **Capability picker** — dropdown of all capabilities; auto-renders form from JSON Schema. Cardinality clearly shown.
3. **Remote shell** — multiline command, target selector (checkboxes/chips), streaming merged output below.

### 18.4 Page 3 — Runs
Airflow-style. Columns: ID, plan name, status, started, duration, nodes used, cost, retries. Filters. Click row → run detail with DAG viz, per-task panels, live logs, federated per-node breakdown, fan-out progress bars, cancel/retry/clone buttons. Live updates via WebSocket.

### 18.5 Page 4 — Costs
Dedicated cost dashboard: live spend graph (per plan/day/provider), token breakdown, projected total, budget bars, big red stop button, per-node contribution.

### 18.6 Page 5 — History / Audit
Append-only event log. Filters by actor, action, time, node. Cloud escalations highlighted. Export JSON/CSV.

### 18.7 Page 6 — Settings
Tabs: Peers, Capabilities, Scopes, Secrets, Policy, Mesh, Schedules.

### 18.8 Front-door behavior
UI on non-brain node connects to brain via local WebSocket relay. All API calls proxied. Brain change mid-session → transparent reconnect.

---

## 19. CLI Specification

```
harness init                              # bootstrap a new mesh
harness join                              # join an existing mesh
harness leave                             # gracefully exit
harness status                            # role, queue, recent tasks
harness peers                             # list mesh members
harness logs [-f] [--node <name>]         # tail logs

harness submit <capability> [--input <json>|@file] [flags]
harness fanout --capability <cap> --inputs <glob> [--concurrency N] [--checkpoint]
harness plan "<goal>"                     # show DAG without running
harness exec "<goal>" [--planner <tier>]  # plan + execute

harness run [--all|--on <node>|--where <expr>] -- <cmd...>
harness ws create|sync|run|destroy        # persistent workspaces

harness search "<query>"                  # mesh.search
harness grep "<pattern>"                  # mesh.grep
harness find "<glob>"                     # mesh.find_file

harness caps [--node <name>]
harness scopes add|rm|list|reindex
harness secrets add|rm|list
harness policy show|edit|apply
harness costs                             # show running spend

harness update                            # rolling self-update
harness backup                            # snapshot ~/.harness
```

`--json` for machine output.

---

## 20. External Integrations

### 20.1 Mobile / web
UI mobile-responsive; `harness.local:19198` from a phone on same Wi-Fi works. Save to home screen for "app." (v2) Native wrapper + push notifications.

### 20.2 Webhooks
HTTP POST to `/webhook/<integration>` on any node forwarded to brain.

Built-in adapters: WhatsApp (Twilio/Meta signature), SMS (Twilio), iOS Shortcuts (signed JSON token), Slack (slash commands + events), Telegram bot, Email-to-task (IMAP poller).

Each validates provider signature and converts to `Task`/`Plan` submission.

### 20.3 mDNS convenience name
Binary advertises `harness.local` resolving to whichever node is brain (round-robin among healthy acceptable). Clients use stable name.

---

## 21. Tech Stack

### 21.1 Language & runtime
Rust (latest stable), `tokio` async, MSRV declared.

### 21.2 Networking
`quinn` (QUIC), `mdns-sd` (or `iroh` for v1.5 NAT), `axum` (HTTP + WebSocket), `hyper`.

### 21.3 Cryptography
`ed25519-dalek`, `blake3`, `age` or `chacha20poly1305`, `argon2`.

### 21.4 Serialization
`serde` + `ciborium` (CBOR) on wire, `serde_json` for HTTP, `schemars` for JSON Schema generation.

### 21.5 Persistence
`rusqlite` (or `sqlx` sqlite). WAL mode, single-writer per node. **Checkpoint store** as separate SQLite tables. (v2) embedded vector store via `hnsw_rs` or `qdrant-client`.

### 21.6 LLM clients
`reqwest` + thin wrappers for Claude, OpenAI, Gemini, Ollama. **Micro-batcher** crate for `llm.local.*` request coalescing. (v2) `mistral.rs` or `candle` for in-process inference.

### 21.7 MCP
`rmcp` (Rust MCP SDK).

### 21.8 CRDT / state replication
`automerge` for shared state where eventual consistency is fine. Custom lightweight gossip layer for high-frequency state.

### 21.9 Observability
`tracing` + `tracing-subscriber`, `opentelemetry` (W3C trace context propagated in task envelope), local trace viewer in UI.

### 21.10 Web UI
**SvelteKit** built static, embedded via `rust-embed` (alt: **Leptos** for full-Rust). Tailwind CSS. Charts: Chart.js / Recharts. DAG viz: Cytoscape.js / React Flow.

### 21.11 Build & distribution
`cargo` workspace, `cross` for cross-compile (linux x86_64/aarch64, darwin x86_64/aarch64, windows x86_64), `cargo-dist` for release artifacts, GitHub Actions CI, signed releases via `cosign` or `minisign`. Single-binary outputs ~20–40 MB stripped.

### 21.12 Why Rust
Single binary, no runtime. First-class async networking. Strong typing for protocol correctness. Good cross-compile. Memory-safe daemon you can trust on a personal LAN. Performance comparable to Go, much better than Python — matters for high-frequency gossip and large fan-outs.

---

## 22. Repository Structure

```
harness/
├── Cargo.toml                  # workspace
├── README.md
├── HARNESS_PRD.md              # this document
├── CLAUDE.md                   # Claude Code operating instructions
├── STATE.md                    # current phase tracker
├── CHANGELOG.md
├── LICENSE                     # MIT or Apache-2.0
├── crates/
│   ├── harness-core/           # protocol types, traits, glue
│   ├── harness-mesh/           # discovery, transport, gossip, weighted election
│   ├── harness-store/          # SQLite + replication + checkpoints
│   ├── harness-policy/         # policy engine
│   ├── harness-merge/          # federated merge strategies
│   ├── harness-cost/           # cost tracking
│   ├── harness-brain/          # planner runtime, backend tiers
│   ├── harness-capabilities/
│   │   ├── shell/
│   │   ├── llm-local/          # with micro-batcher
│   │   ├── llm-cloud/
│   │   ├── mcp/
│   │   ├── fs/
│   │   ├── http/
│   │   ├── mesh-meta/          # mesh.search, mesh.grep, etc.
│   │   └── schedule/
│   ├── harness-orchestrator/   # planner glue, scheduler, DAG executor, fanout controller
│   ├── harness-api/            # HTTP/WebSocket API (axum)
│   ├── harness-cli/            # clap-based CLI
│   ├── harness-ui/             # embedded UI assets
│   └── harness-daemon/         # the `harness` binary
├── ui/                         # SvelteKit (or Leptos) source
├── installers/
│   ├── get.sh                  # the curl|sh installer
│   ├── homebrew/
│   ├── deb/
│   ├── rpm/
│   └── launchd/
├── docs/
│   ├── architecture.md
│   ├── protocol.md
│   ├── policy.md
│   ├── decisions/              # ADRs
│   └── tutorials/
├── tests/
│   ├── integration/            # multi-node tests via tokio + mock LAN
│   └── e2e/                    # spawn N daemons, run scenarios
├── benchmarks/                 # N×X scaling benchmarks (CI-gated)
└── .github/workflows/
```

---

## 23. Build Phases / Roadmap

Each phase ends with a working, useful artifact. Do not skip ahead.

### Phase 0 — Project setup (1–2 days)
Cargo workspace scaffolded, CI green, README, `harness --version` works.

### Phase 1 — Mesh skeleton (week 1)
- Identity generation, `~/.harness/` layout
- mDNS discovery; QUIC with Noise/TLS
- Signed heartbeats; **weighted brain election** with battery + LLM awareness
- `harness init` / `harness join` (pairing codes)
- `harness peers` / `harness status`
- Web UI Mesh page (read-only)

**Demo:** install on two laptops, watch them discover each other, elect a brain, brain reweighting when a stronger node joins.

### Phase 2 — Tasks flow (week 2)
- Task envelope, result envelope, signing
- Single-task lifecycle: submit → dispatch → execute → result
- **Cardinality field on capabilities** (Anyone, Owner with scope, Federated)
- **Scopes in node manifest**
- Round-robin dispatch (no scoring yet)
- SQLite task DB with replication via gossip
- HTTP submit API; CLI `harness submit`
- Web UI Submit page (capability picker) and Runs page
- Built-in `echo` capability for testing

**Demo:** submit `echo "hello"` from any node, see it execute on another, view in UI.

### Phase 3 — Fleet exec, brain runtime, built-ins (week 3)
- `shell.exec` with policy engine + streaming output
- `harness run --all|--on|--where` CLI; UI Remote Shell mode
- `llm.local.*`, `llm.cloud.*`, `mcp.proxy` capabilities
- **`brain.plan`** with Template + LocalFast backends
- **`llm.local.*` micro-batching**
- `mesh.search` and `mesh.grep` built on top of `fs.search` / `fs.grep`

**Demo:** `harness run --all -- uname -a`; `harness search "term sheet"` federates across nodes.

### Phase 4 — Distribution patterns (week 4)
- **Streaming chunk dispatch** for fan-out
- **Result streams** (`Stream<TaskResult>` for callers, WebSocket for UI)
- DAG executor with topological dispatch
- **Resource-aware scheduling** (multidimensional)
- **Federated execution lifecycle** with partial progress
- Lease-based claiming, retry, partial results
- Web UI DAG visualization + progress bars

**Demo:** "summarize 50 PDFs across 2 laptops" — fan-out completes in ~half the time. Federated `mesh.search` shows per-node contribution.

### Phase 5 — Planner intelligence + cost + external (week 5)
- **`brain.plan` LocalStrong backend** + Cloud backend with escalation rules
- **Planner validation** (rejects invalid plans before execution)
- Natural-language submit → planner DAG
- WhatsApp/SMS/iOS Shortcuts webhook adapters
- **`Budget` enforcement and Cost dashboard**
- **Checkpoint and resume**
- Audit log + History UI

**Demo:** WhatsApp message → mesh executes multi-step plan → text reply with cost summary. Crash brain mid-plan; new brain resumes from checkpoint.

### Phase 6 — Hardening & polish (week 6)
- Self-updater (rolling, version-negotiated)
- Speculative execution + circuit breakers
- Schedules (cron)
- Secrets management (encrypted, replicated)
- Policy UI
- Mobile-responsive UI pass
- One-line installer + Homebrew tap + .deb/.rpm
- **Benchmark suite in CI** (N×X scaling targets)

**Demo:** end-to-end story from §9–§20 working without manual intervention. Benchmarks pass.

### Phase 7 (v2 backlog)
- Cross-LAN federation via Tailscale or iroh
- WASM sandboxed third-party capabilities
- Multi-user UI with role-based access
- Embedded inference (mistral.rs / candle)
- Capability marketplace (signed third-party plugins)
- Cross-mesh sync for traveling users

---

## 24. Testing Strategy

- **Unit** — every protocol type round-trips through CBOR + signature; every scoring/policy decision is deterministic and tested.
- **Integration** — spawn N daemons in-process, simulate LAN, run multi-node scenarios in CI.
- **End-to-end** — Docker Compose with 3 daemons + webhook poster + UI tester (Playwright).
- **Chaos** — kill brain mid-task; partition mesh; corrupt DB; verify recovery.
- **Property tests** — `proptest` on protocol invariants (election converges; tasks never lost; replication eventually consistent; checkpoints idempotent).
- **Fuzzing** — `cargo-fuzz` on wire decoders.
- **Benchmarks** — heartbeat throughput, gossip convergence, task dispatch latency, **N×X scaling on canonical workloads from §17.9**. Regressions fail CI.

---

## 25. Operational Concerns

### 25.1 Resource limits
Daemon RAM ceiling (default 512 MB). Per-capability concurrency caps. Backpressure: queue depth at limit → broadcast `paused`; brain stops dispatching.

### 25.2 Sleep/wake
macOS sleep → graceful pause + lease release. Wake → fast-resume reconnect. In-flight tasks → auto-redispatched via lease expiry.

### 25.3 Logging
Structured JSON to `~/.harness/logs/` with rotation. `harness logs -f` tails locally; UI tails any node's logs over the mesh. Log levels per module via env.

### 25.4 Backup
`harness backup` snapshots `~/.harness/` (excluding identity key by default). Restore on a new machine continues mesh membership iff identity key is included.

### 25.5 Version compatibility
Protocol version negotiated per connection; minor versions backward-compatible, majors gated. Self-updater respects mesh consensus: rolling, one node at a time, automatic rollback on health failure.

### 25.6 Clock skew
Heartbeat timestamps advisory; ordering uses monotonic seq + vector clocks. NTP recommended but not required.

---

## 26. Success Metrics

For personal-use phase:
- **Onboarding time** — install to first cross-node task: target < 5 min for two laptops
- **Wall-clock speedup** — see §17.9 benchmark targets
- **Crash-free days** — daemon uptime ≥ 99% over 30 days (sleep/wake included)
- **Brain failover** — brain offline → new brain serving requests: target < 3s p95
- **Local-first usage** — % of plans completed without cloud LLM: target > 60% on default config
- **Daily active use** — does the author run something through the harness every day?

For open-source release:
- **Time to first task on a fresh machine** by a new user: target < 10 min
- **Star/issue/PR ratio** — qualitative health
- **Number of contributed capabilities** — measures whether the capability layer is well-designed

---

## 27. Open Questions

Resolve and document each in `docs/decisions/NNNN-title.md`:

1. **Discovery resilience.** mDNS is flaky on some routers. Static fallback only, or iroh DHT in v1?
2. **Web UI framework.** SvelteKit (faster) vs Leptos (pure-Rust).
3. **CRDT vs. Raft.** Automerge for everything, or small Raft for task ownership?
4. **Multi-user.** v1 single-admin. When multi-user lands, how are pairing codes scoped?
5. **Capability versioning.** SemVer declared, but how do we handle worker-v1 vs brain-dispatching-v2?
6. **Mobile-native.** Responsive web UI vs PWA + push notifications for v1.
7. **Secrets blast radius.** Replicated encrypted secrets are convenient but every node is a credential target. Per-secret access tiers?
8. **Planner capability discovery at scale.** With 50+ capabilities, inline schemas in the prompt, retrieve, or fine-tune a router?
9. **Federated fan-out cap.** With 10+ eligible nodes, sample or always dispatch to all?
10. **Cross-mesh scope namespace** (v2): how to handle collisions?

---

## 28. Appendix A — Wire Schemas

(See §13. The implementer copies these types verbatim into `crates/harness-core/src/protocol.rs`.)

---

## 29. Appendix B — `CLAUDE.md`

A separate `CLAUDE.md` at the repo root:

```
# Claude Code Operating Instructions

You are implementing the Harness project specified in HARNESS_PRD.md.

## Operating principles

1. Never skip phases. Complete Phase N's demo before starting Phase N+1.
2. Every PR includes tests and updates CHANGELOG.md.
3. When the spec is silent, propose a resolution in `docs/decisions/NNNN-title.md`
   (ADR format) and proceed.
4. Maintain `STATE.md` at the repo root tracking current phase, what's done,
   what's next, what's blocked.
5. Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test` before
   any commit. Run `cargo bench --bench scaling` before completing Phase 6.
6. Prefer simple, small modules over clever ones.
7. Always implement streaming dispatch (never materialize all sub-tasks).
8. Always include `Cardinality` on new capabilities; default to Anyone if
   unsure but document the choice.
9. Never bypass plan validation, even for "trusted" planner backends.
10. Bounded channels everywhere.

## Stopping conditions

Stop and ask if:
- A protocol-level change would break backwards compatibility.
- A new external dependency is required.
- A security/privacy implication arises that isn't covered by the PRD.
- Brain election or planner validation would be weakened.
```

---

## 30. Appendix C — End-to-End Scenario

User on phone, away from home. Two laptops at home (`mac` and `linux-box`, the latter has a 4090 + Llama 70B). User WhatsApps the harness bot:

> "Find every PDF in my documents folder mentioning 'Series A term sheet', summarize each, and email me a comparison table."

What happens:

1. WhatsApp webhook hits home router's port forward → `mac:19198/webhook/whatsapp`.
2. `mac` validates Twilio signature, forwards to brain (currently `linux-box` because of weighted election: it has LocalStrong planner).
3. `linux-box` brain calls `brain.plan` locally (its own llama3.1:70b is the LocalStrong backend). Plan compiled:
   - `t1: mesh.search { query: "Series A term sheet", scope_type: "documents" }` → federated to all nodes with `fs.search` and a `documents` scope (just `mac`)
   - `t2: doc.extract_text` — fan-out across results (streaming dispatch)
   - `t3: llm.summarize` — fan-out (uses `llm.local.llama70b` on `linux-box` and `llm.local.llama3.1:8b` on `mac` based on resource hints; batched on `linux-box`)
   - `t4: llm.compose_table` — single call to `llm.local.llama70b`
   - `t5: email.send` — to user's address (uses `secret/smtp-credentials`)
4. Plan validated against capability index. Confidence 0.84 — proceed.
5. Brain dispatches with streaming results.
6. Each task streams logs back; UI Runs page shows live DAG progress on the user's phone.
7. Total tracked cost: $0.00 (all local). Wall-clock: 1m 14s.
8. Result email arrives. WhatsApp gets reply: "Done — emailed comparison of 7 term sheets. 1m 14s, $0.00 (all local), 22 tasks across 2 nodes."

User never knew which node ran what. Never used SSH. Never opened a laptop. Nothing left the LAN.

---

## 31. Single-paragraph README pitch

> Harness is a local-first agent mesh for the machines you already own. A small Rust binary on each laptop, desktop, and server auto-discovers its peers, elects a brain (powered by your local LLM, with cloud as escalation), and turns your idle compute into a private agent fleet. Tasks are typed, capability-routed, and parallelized — searches federate across every node that has data, while stateless work runs wherever there's headroom. Intensive workloads (embed 100k docs, grade 5k LLM outputs, triage 50 issues) scale linearly with node count: N machines × X each. Nothing leaves your network unless you say so. Install it on two laptops, click pair, and your mesh is alive.

That paragraph is the product. Everything in this PRD exists to make it true.

---

*End of v2 PRD. Hand to Claude Code, set phase to 0, begin.*
