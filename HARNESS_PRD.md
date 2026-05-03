# Harness — A LAN-Native Agent Mesh

> **Working codename: `harness`** (the project name is a placeholder — final naming is open; "Open Claw" is taken by an existing agent system).
>
> **Document type:** Product Requirements Document + Architecture Specification
> **Audience:** an autonomous coding agent (Claude Code or equivalent) implementing the system end-to-end, plus humans reviewing the design.
> **Status:** v0 — intended for direct handoff to implementation.

---

## 1. Executive Summary

**Harness is a single Rust binary you install on every laptop, desktop, and server you own. It auto-discovers its peers on the LAN, forms a self-healing mesh, and turns your idle machines into a private agent swarm.**

Any node can submit work. Any node can execute it. One node is automatically elected as the *brain* (the planner/dispatcher); leadership re-elects in seconds if the brain goes offline. External clients (mobile apps, WhatsApp webhooks, iOS Shortcuts, browsers) talk to whichever node answers — the mesh routes internally. Tasks are typed, capability-routed, parallelized across nodes, and auditable through an Airflow-style web UI served on `:19198` from every node.

The product solves three problems no existing tool solves together:

1. **Idle compute is wasted.** Every household and small team has 3–10 capable machines doing nothing most of the time.
2. **Agent workloads are I/O-bound and parallelism-starved.** Single-machine agent loops are slow when they don't have to be.
3. **Privacy-sensitive agent work has no good story.** Cloud agent platforms see everything; LAN-only fleets currently require ops effort no normal person will exert.

Harness collapses the operational cost of running a private agent fleet to "install a binary on each machine, click pair." After that, your machines work as one.

---

## 2. Problem Statement

### 2.1 The user's reality

A typical technical user has:

- 2–5 personal computers (laptops, desktops, mini-PCs, NAS, occasionally a GPU box)
- All on the same Wi-Fi / LAN
- All with internet access (most of the time)
- Identical or near-identical software stacks (Ollama, Python, Node, Claude API key, etc.)
- Only one machine in active use at any given moment
- A growing set of agentic workloads they'd like to run (refactors, research, embedding, summarization, scheduled jobs)

### 2.2 What goes wrong today

- Long-running agent jobs pin the laptop the user is currently using.
- Switching machines means losing context, re-installing tools, re-syncing data.
- Cloud agents (Claude.ai, ChatGPT, etc.) can't see local files or run local tools at scale.
- Local agents (Claude Code, Aider, Continue) only see one machine.
- "Distributed" frameworks (Ray, Celery, Airflow) are heavy, ops-shaped, and not designed for personal multi-laptop use.
- SSH-into-each-machine is the de-facto answer, and it doesn't scale past three commands.

### 2.3 What "good" looks like

- One install per machine, no further per-machine setup.
- One UI, accessible from any node or from mobile.
- One mental model: "my mesh." Tasks go to the mesh, not to a specific machine.
- Wall-clock for parallel-friendly workloads drops linearly with node count.
- No data leaves the LAN unless the user explicitly opts in.
- The system stays useful when the internet is down.

---

## 3. Vision & Thesis

> Harness is **MCP at the network layer** — a federation protocol for tools, models, and capabilities across the machines a person already owns, plus a scheduler that routes typed tasks across that fleet.

The thesis in one sentence: **the harness, not the agent, is the missing layer in personal AI infrastructure.** Agents are commodity. The substrate that lets them act as a fleet is scarce. Whoever builds the open-source Rust implementation of that substrate becomes the default.

---

## 4. Goals & Non-Goals

### 4.1 Goals

- **Zero-config onboarding.** Install command + pairing code + done.
- **Self-healing mesh.** Nodes join and leave freely; leader re-elects automatically.
- **Identity-first security.** Every node has an Ed25519 keypair; every message is signed.
- **Capability-typed routing.** Tasks declare what they need; the dispatcher matches by capability + load + constraints.
- **Single-binary deploy.** No external broker, no database server, no Kubernetes.
- **Privacy by default.** Tasks tagged `local-only` cannot be routed to nodes flagged `cloud`.
- **Observable.** Every task has a trace ID; every action is auditable; the UI shows live state.
- **Useful from day one on a single machine** — multi-node is an upgrade, not a prerequisite.

### 4.2 Non-Goals (v1)

- WAN/internet-scale routing (mesh is LAN-bound; cross-LAN is a v2 feature via Tailscale or similar).
- Multi-tenant SaaS hosting.
- Replacing Kubernetes/Ray/Celery for production cloud workloads.
- Strong consistency guarantees beyond what's needed for task lifecycle (eventual consistency for everything else).
- Mobile-native apps (mobile is a web client of the local UI in v1).

---

## 5. Target Users

| Persona | Setup | Primary use |
|---|---|---|
| **Solo developer** | 2 laptops + Mac mini | Distributed code agents, parallel test runs, background bulk work |
| **AI tinkerer** | Gaming PC (4090) + MacBook + NAS | Hybrid local-LLM + cloud-LLM workflows |
| **Privacy-sensitive professional** | Work + personal laptop, no cloud allowed | Federated search and summarization over local files |
| **Small team / family** | 3–6 personal devices | Shared agent capabilities, household automation, coordination |
| **SMB owner** | Office of 5–20 machines | Internal RAG over local data, cost-controlled cloud-LLM gateway |

---

## 6. Use Cases

The system is justified by the following concrete user stories. Each one must work end-to-end in v1 unless marked otherwise.

1. **Parallel research.** "Research these 10 companies, draft outreach for each." Brain decomposes into 10 parallel sub-tasks, dispatches across nodes, aggregates results.
2. **Bulk transformation.** "Summarize these 200 PDFs." Fan-out across nodes; work-stealing handles imbalance.
3. **Mixed local/cloud pipelines.** Local Llama filters 1,000 candidates → Claude analyzes top 50. 95% cost reduction.
4. **Background fleet work.** Submit from phone: "Process these receipts overnight." Whichever node is online at 2am picks them up.
5. **Long-running refactors.** Dispatch a Claude Code-style multi-hour refactor to a non-active laptop; primary machine stays free.
6. **Fleet exec.** "Update Ollama on every node." One command, parallel execution, merged output.
7. **External trigger.** WhatsApp message → webhook → mesh executes → result texted back.
8. **Specialization routing.** "Run this on the GPU box." Capability tag `gpu` matches one node.
9. **Graceful degradation.** A node closes mid-task; the task auto-requeues to a surviving node within seconds.
10. **(v2) Cross-mesh.** Two homes' meshes federate via Tailscale; family members share capabilities.

---

## 7. Core Concepts (Glossary)

- **Node** — a machine running the `harness` daemon.
- **Mesh** — the set of mutually-trusted nodes forming a logical cluster.
- **Brain** — the currently-elected leader node responsible for planning and dispatch.
- **Capability** — a typed unit of work a node can perform (e.g. `shell.exec`, `llm.claude`, `fs.search`).
- **Task** — an instance of a capability invocation with concrete inputs.
- **Plan / DAG** — a directed acyclic graph of tasks emitted by the planner from a user goal.
- **Manifest** — a node's signed declaration of its identity, capabilities, and resources.
- **Heartbeat** — periodic signed liveness packet broadcast by every node.
- **Pairing** — the one-time human-mediated process by which a new node joins the mesh.
- **Worker** — role assumed by any node when executing a task. (Every node is a worker.)
- **Gateway** — capability tag for nodes that bridge to external paid services (Claude, OpenAI, etc.).

---

## 8. System Architecture

### 8.1 High-level topology

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
   │                                                │
   │   ┌──────────┐    ┌──────────┐   ┌──────────┐  │
   │   │  Node A  │◄──►│  Node B  │◄─►│  Node C  │  │
   │   │ (brain)  │    │  worker  │   │  worker  │  │
   │   └──────────┘    └──────────┘   └──────────┘  │
   │       ▲                ▲              ▲        │
   │       │  signed gossip │  signed gossip│       │
   │       └────────────────┴───────────────┘       │
   └────────────────────────────────────────────────┘
```

Every node runs the same binary. Every node has the same code paths. The brain role is soft: an elected pointer that any node fulfills if needed.

### 8.2 Layered architecture (per-node)

```
┌─────────────────────────────────────────────────────────┐
│                    Web UI (embedded)                    │
│        Login · Mesh · Submit · Runs · History · Settings│
├─────────────────────────────────────────────────────────┤
│  HTTP API (axum)         │     CLI (clap)               │
├──────────────────────────┴──────────────────────────────┤
│                    Orchestration Layer                  │
│   Planner · DAG executor · Scheduler · Policy engine    │
├─────────────────────────────────────────────────────────┤
│                    Capability Layer                     │
│   shell.exec · llm.* · mcp.proxy · fs.* · http.* · etc. │
├─────────────────────────────────────────────────────────┤
│                       Mesh Layer                        │
│  Identity · Discovery (mDNS) · Transport (QUIC) ·       │
│  Heartbeats · Leader election · Gossip · Replication    │
├─────────────────────────────────────────────────────────┤
│                    Persistence Layer                    │
│   SQLite (tasks, audit, capability index, traces)       │
└─────────────────────────────────────────────────────────┘
```

### 8.3 Roles

Every node simultaneously is:

- **Discovery participant** — mDNS broadcast + listen
- **Worker** — executes assigned tasks
- **State replica** — holds full task DB; ready to become brain
- **Front door** — accepts client requests over HTTP, forwards to brain if needed

Exactly one node is also:

- **Brain** — runs planner, dispatch, aggregation. Re-elected on failure.

---

## 9. Onboarding & Installation

> **The onboarding *is* the product. If a user ever needs to SSH into a node, the design has failed.**

### 9.1 Install (per machine, once)

```bash
curl -sSL https://get.harness.sh | sh
```

This installer:

1. Detects OS/arch.
2. Downloads the signed `harness` binary to `/usr/local/bin/harness`.
3. Creates `~/.harness/` with mode `0700`.
4. Generates an Ed25519 identity keypair.
5. Installs a launchd plist (macOS) or systemd user unit (Linux) for auto-start on boot with restart-on-crash.
6. Starts the daemon.
7. Prints the next command to run.

Alternative install paths:
- `brew install harness` (macOS)
- `cargo install harness-cli` (developers)
- Pre-built `.deb`/`.rpm` for Linux servers
- Single-binary download for air-gapped installs

### 9.2 First-node bootstrap

```bash
$ harness init
✓ Identity created (node_id: 7f3a9c…)
✓ Hostname:           macbook-archy
✓ Mesh created:       "archy-home"
✓ Admin password:     k7Qm-9Rxz-Wp3a   (saved to ~/.harness/admin)
✓ Web UI:             http://192.168.1.42:19198
✓ Pairing code:       4271-9384  (valid 10 minutes)

Mesh is live. Open the UI to add more nodes.
```

### 9.3 Joining additional nodes

```bash
$ harness join
🔍 Scanning LAN…
   Found mesh "archy-home" advertised by macbook-archy

? Pairing code: 4271-9384
✓ Submitted pairing request
⏳ Waiting for approval on macbook-archy…
✓ Approved as "thinkpad-archy"
✓ Capabilities auto-detected: shell, ollama, git, rust, python
✓ Web UI: http://192.168.1.78:19198
```

The receiving node displays a notification in its UI:

> **New node wants to join: thinkpad-archy** (192.168.1.78)
> Capabilities: shell, ollama, git, rust, python
> [Approve] [Reject]

After approval, the new node's pubkey is added to every existing node's trust list via gossip.

### 9.4 Capability auto-detection

On first start, the daemon probes the local environment:

- Binaries on PATH → corresponding shell capabilities
- `nvidia-smi` exit 0 → `gpu` tag
- Ollama running → `llm.ollama.*` capabilities (one per `ollama list` model)
- API keys in env or `~/.harness/secrets` → `llm.claude`, `llm.openai`, etc.
- MCP server configs in `~/.harness/mcp/` → corresponding `mcp.*` capabilities

The user reviews and approves the proposed manifest in the UI before it's published.

### 9.5 Day-2 operations are also one-line

- `harness peers` — list mesh members and status
- `harness status` — show this node's role, queue, recent tasks
- `harness logs -f` — tail this node's logs
- `harness leave` — gracefully exit the mesh
- `harness update` — self-update (rolling, mesh stays up)

---

## 10. Identity, Trust & Security

### 10.1 Identity

- Each node generates an Ed25519 keypair at install.
- `node_id = blake3(pubkey)[..16]` (16-byte stable ID).
- Identity persists across restarts; stored in `~/.harness/identity.key` mode `0600`.

### 10.2 Trust model

- A trust file `~/.harness/peers.toml` lists each trusted peer's pubkey and tier.
- Tiers: `trusted` (full access), `default` (whitelist only), `guest` (read-only).
- Joining a mesh = pairing-code-approved exchange of pubkeys, gossiped to all members.
- Unsigned or untrusted messages are dropped at the transport layer.

### 10.3 Wire security

- All inter-node transport over **QUIC with Noise/TLS** (one connection per peer pair, multiplexed streams).
- Every application-level message is **signed** by the sender (Ed25519 over the canonical CBOR encoding).
- Heartbeats and gossip messages include monotonic sequence numbers to prevent replay.

### 10.4 Policy engine

Every node has `~/.harness/policy.toml` declaring what it will execute remotely:

```toml
[shell]
allow = [
  { cmd = "ls", any_args = true },
  { cmd = "git", subcmds = ["status", "log", "diff", "pull", "fetch"] },
  { cmd = "ollama", any_args = true },
  { cmd = "cargo", subcmds = ["build", "test", "check"] },
  { pattern = "^pdftotext .* -$" },
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
```

Policy is evaluated **on the executing node**, not the dispatcher. Brains cannot override worker policy.

### 10.5 Secrets

- Stored in `~/.harness/secrets.enc`, encrypted with a key derived from the node's identity + admin password.
- Replicated across the mesh (encrypted at rest on every node) so any node can become brain.
- Tagged: `secret/claude-api-key`, `secret/openai-api-key`, etc.
- Capabilities reference secrets by tag; raw values are never on the wire.

### 10.6 Audit log

Every privileged action (task dispatch, shell exec, secret access, peer approval, policy change) is written to an **append-only audit log** replicated to every node. Tamper-evident via hash chain. Viewable in the UI's History tab.

---

## 11. Discovery & Networking

### 11.1 Discovery

- mDNS service: `_harness._tcp.local`, port 19198 for HTTP, separate port for QUIC.
- TXT record advertises: `mesh_name`, `node_id`, `pubkey_fingerprint`, `version`.
- Optional fallback: static peer list in `~/.harness/peers.toml`.
- (v2) DHT-based discovery for cross-LAN via iroh.

### 11.2 Transport

- **QUIC** (via `quinn`) for all node-to-node traffic.
- **HTTP/HTTPS** on `:19198` for the web UI and external clients.
- One QUIC connection per peer pair, persistent, with bidirectional streams per active task.

### 11.3 Why QUIC

- Multiplexed streams (one per task) without head-of-line blocking
- Built-in encryption
- 0-RTT reconnect after WiFi blips (laptops sleep/wake constantly)
- UDP-based, easier in NAT/Tailscale scenarios later

---

## 12. Leader Election (Brain Selection)

### 12.1 Algorithm

For 2–10 nodes (the expected range), full Raft is overkill. Use a **simplified bully-style protocol**:

1. Every heartbeat contains the sender's `leader_belief`.
2. A node considers itself the leader iff it has the highest `node_id` among nodes whose heartbeats it has seen in the last 6 seconds.
3. On startup, a node broadcasts heartbeats marking `leader_belief = self`. After one election window (~3s), it converges.
4. On leader timeout (>6s missed heartbeats), surviving nodes recompute. Convergence within ~2s.

Properties:

- Deterministic: highest node_id always wins.
- No split-brain on a connected LAN (everyone sees the same set).
- On partition: each side elects its own brain, reconciles when partition heals (latest task DB wins per task by vector clock).

### 12.2 What the brain owns

- The planner agent (LLM-backed, optional fallback to template plans)
- The dispatch decision for every new task
- Result aggregation for fan-out and DAG plans
- The single source of truth for in-flight task state (replicated to followers via gossip)

### 12.3 What the brain does **not** own

- Local task execution policy (workers retain authority)
- Secrets storage (replicated, brain has no special read access)
- Audit log (append-only, replicated)

### 12.4 Brain handover

When the elected brain changes:

1. New brain claims in-flight tasks from local replica state.
2. New brain's URL is gossiped; UI badges update within ~2s.
3. External clients see no break; their next request is forwarded by whichever node receives it.
4. Previous brain (if still alive but demoted) becomes a worker.

---

## 13. Protocol Specification

All messages are **CBOR-encoded** (compact, schema-flexible, fast in Rust) and **Ed25519-signed**.

### 13.1 Heartbeat (broadcast every 2s)

```rust
struct Heartbeat {
    node_id: NodeId,             // [u8; 16]
    seq: u64,                    // monotonic per-node
    timestamp: u64,              // unix millis
    queue_depth: u16,
    cpu_pct: u8,
    ram_free_mb: u32,
    capabilities_hash: [u8; 16], // changed? request full manifest
    in_flight: Vec<TaskId>,
    leader_belief: NodeId,
    version: SemVer,
    sig: [u8; 64],
}
```

Approximate size: 250 bytes. Multicast on the mesh.

### 13.2 Capability manifest (gossiped on change)

```rust
struct NodeManifest {
    node_id: NodeId,
    hostname: String,
    pubkey: [u8; 32],
    capabilities: Vec<Capability>,
    resources: Resources,
    online_since: u64,
    version: SemVer,
    sig: [u8; 64],
}

struct Capability {
    id: String,                  // "shell.exec", "llm.claude", "fs.search"
    version: SemVer,
    input_schema: JsonSchema,
    output_schema: JsonSchema,
    cost_hint: CostHint,         // LocalFast | LocalSlow | Gpu | CloudPaid
    tags: Vec<String>,           // "private", "gpu", "needs_internet"
    rate_limit: Option<RateLimit>,
}

struct Resources {
    ram_total_mb: u32,
    cpu_cores: u8,
    gpu: Option<GpuInfo>,
    os: String,
    arch: String,
}

enum CostHint { LocalFast, LocalSlow, Gpu, CloudPaid }
```

### 13.3 Task envelope

```rust
struct Task {
    id: TaskId,                  // Uuid v7 (sortable by time)
    parent: Option<TaskId>,      // for sub-tasks in a DAG
    plan_id: Option<PlanId>,     // for DAG-grouped tasks
    capability: String,
    input: serde_json::Value,    // matches capability.input_schema
    constraints: Constraints,
    retry: RetryPolicy,
    execution: ExecutionPolicy,
    trace_ctx: TraceContext,     // OpenTelemetry W3C trace context
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
}

struct RetryPolicy {
    max_attempts: u8,            // default 3
    backoff: Backoff,            // Constant | Linear | Exponential
    retryable_errors: Vec<ErrorClass>,
}

struct ExecutionPolicy {
    redundancy: u8,              // 1 = normal, 2 = speculative
    timeout_ms: u32,
    on_partial: PartialPolicy,   // FailFast | ReturnPartial | Wait
    lease_ms: u32,               // worker must complete-or-extend within
}
```

### 13.4 Result envelope

```rust
struct TaskResult {
    task_id: TaskId,
    node_id: NodeId,             // which node executed
    started_at: u64,
    finished_at: u64,
    status: Status,              // Ok | Err(ErrorClass) | Timeout | Cancelled
    output: serde_json::Value,
    cost: Cost,                  // tokens, dollars, etc.
    logs: Vec<LogLine>,          // stdout/stderr/structured
    sig: [u8; 64],
}
```

### 13.5 Subjects (logical channels)

Whether implemented as gossipsub topics, NATS subjects, or QUIC stream classifiers, the logical namespace is:

```
harness.announce                    # new node manifests
harness.heartbeat.<node_id>         # liveness
harness.task.offer                  # brain broadcasts available task
harness.task.bid.<task_id>          # workers respond with eligibility + ETA
harness.task.assign.<task_id>       # brain picks one
harness.task.lease.<task_id>        # worker extends lease
harness.task.result.<task_id>       # final result
harness.task.log.<task_id>          # streaming logs
harness.gossip.state                # CRDT state diffs
harness.audit                       # append-only audit entries
```

---

## 14. Task Execution Model

### 14.1 Single task lifecycle

```
SUBMITTED → PLANNED → DISPATCHED → CLAIMED → RUNNING → DONE
                                       │         │
                                       └─► EXPIRED └─► FAILED → (retry?)
```

1. **SUBMITTED** — client posts to any node's HTTP API.
2. **PLANNED** — if natural language, brain's planner emits a DAG; else direct.
3. **DISPATCHED** — brain's scheduler scores eligible workers, sends offer.
4. **CLAIMED** — worker accepts, takes a lease, ACKs.
5. **RUNNING** — worker streams logs; lease auto-extends if work continues.
6. **DONE** — result signed and broadcast.
7. **EXPIRED** — lease lapsed; brain re-dispatches.
8. **FAILED** — error returned; retry policy consulted.

### 14.2 Scheduler scoring

```
score(worker, task) =
    (capability_match  ? 1 : drop)
  * (constraints_met   ? 1 : drop)
  / (1 + queue_depth)
  / (1 + avg_latency_ms / 1000)
  * success_rate_recent
  / cost_weight
```

Hard filters first, then score the survivors, pick top-1 (top-k for redundancy > 1).

### 14.3 Distribution patterns (all three are first-class)

**Fan-out (data parallelism)**

```rust
mesh.fanout(
    capability: "doc.summarize",
    inputs: [...500 docs...],
    concurrency_per_node: 4,
).await
```

Brain creates 500 sibling tasks under one plan, work-stealing queue, results stream back as they complete.

**DAG (task dependency)**

```rust
let plan = Plan::new()
    .add("t1", "fs.search", { query: "..." })
    .add("t2", "doc.summarize", { input: "$t1.results[*]" })
    .add("t3", "llm.compose", { docs: "$t2.outputs" })
    .edge("t1", "t2")
    .edge("t2", "t3");
mesh.execute(plan).await
```

Topological execution: brain maintains a ready-set, dispatches in parallel, advances on each completion.

**Work-stealing (worker pull)**

Workers with idle capacity poll for tasks matching their capabilities. Used for background queues and bulk processing.

### 14.4 Failure handling

- **Lease expiry** → re-dispatch
- **Retry policy** → exponential backoff, max attempts
- **Idempotency keys** → task ID is the key; double-execution prevented
- **Speculative execution** (`redundancy=2`) → first result wins, others cancelled
- **Circuit breaker** → 5 consecutive failures benches a worker for 60s for that capability
- **Partial results** → for fan-out, return successes + warning rather than failing whole plan if `on_partial = ReturnPartial`

### 14.5 Cost accounting

Every result carries a `Cost` field:

```rust
struct Cost {
    tokens_in: u64,
    tokens_out: u64,
    usd: f64,
    wall_ms: u64,
    node_id: NodeId,
}
```

Aggregated per task / per plan / per node / per day in the UI. Budget caps enforced at dispatch time (`Constraints.max_cost_usd`).

---

## 15. Built-in Capabilities

Shipped in the binary; available on every node by default unless disabled in policy.

### 15.1 `shell.exec` — fleet exec

The flagship built-in. Subsumes SSH-into-each-machine.

**Input**

```rust
struct ShellExec {
    cmd: Vec<String>,            // ["bash", "-lc", "your command"]
    cwd: Option<PathBuf>,
    env: HashMap<String, String>,
    stdin: Option<Bytes>,
    timeout_ms: u32,
    capture: CaptureMode,        // Lines | Raw | Streaming
}
```

**Output**

```rust
struct ShellResult {
    exit_code: i32,
    stdout: Bytes,
    stderr: Bytes,
    duration_ms: u64,
}
```

**Selectors** (CLI surface)

- `harness run --all -- <cmd>` — every node
- `harness run --on <node> -- <cmd>` — by name
- `harness run --where '<expr>' -- <cmd>` — by tag/capability expression (`tag:gpu`, `os:linux`, `cap:llm.claude`)

**Sub-features**

- Streaming output: stdout/stderr line-frames on QUIC stream, interleaved with `[node-name]` prefixes
- Stdin piping: local stdin streams into the remote process
- File staging: `--upload <path>` syncs a file to a temp workspace before exec
- Persistent workspaces: `harness ws create/sync/run` for iterating against a kept directory
- Detached jobs: `--detach` returns immediately with a job ID
- Result piping: `harness run … | harness run …` chains across nodes

**Policy**

Strictly governed by `policy.toml`'s `[shell]` block on the executing node. Default deny.

### 15.2 LLM capabilities

- `llm.claude` — Claude API (requires `secret/claude-api-key`)
- `llm.openai` — OpenAI API
- `llm.ollama.<model>` — auto-registered per locally-installed model
- `llm.embed.<model>` — embedding endpoints

All conform to a unified `LlmRequest` / `LlmResponse` shape so the planner can swap backends by cost/availability.

### 15.3 MCP proxy

- `mcp.proxy` — connects to a configured MCP server (subprocess or remote) and exposes its tools as harness capabilities.
- Auto-registered from `~/.harness/mcp/*.toml` configs.
- Each MCP tool becomes a callable capability (`mcp.<server>.<tool>`).

### 15.4 Filesystem capabilities

- `fs.list`, `fs.read`, `fs.search`, `fs.write` (write requires `trusted` peer tier)
- `fs.index` — maintains a local Tantivy/Sqlite-FTS index for `fs.search`

### 15.5 HTTP capabilities

- `http.fetch`, `http.post` — for fetching public URLs (rate-limited, auditable)
- `http.webhook` — receives external triggers (WhatsApp, etc.)

### 15.6 Schedule capabilities

- `schedule.cron` — cron-like recurring task scheduler. Stored in replicated state; any node can hold the schedule, brain triggers.

### 15.7 Plan composition

- `plan.compile` — calls the planner (LLM) with a user goal, returns a DAG
- `plan.execute` — runs a DAG (used internally; surfaced for advanced users)

---

## 16. Web UI Specification

Served on `:19198` from every node, single-page app embedded in the binary. Login on first visit; session cookie thereafter.

### 16.1 Login

- Username: `admin` (single-user v1)
- Password: set at `harness init`, replicated as a salted hash to every node.
- After login: cookie + WebSocket connection for live updates.
- (v2) OAuth, SSO, multi-user.

### 16.2 Page 1 — Mesh

Live topology visualization.

- One card per node showing: hostname, status (green/yellow/red), CPU%, RAM, queue depth, capabilities count, latency to brain, role badge.
- Brain badge clearly visible on the elected node.
- Pulse animation per heartbeat received.
- Hover/click → drilldown: full manifest, recent tasks on this node, logs button.
- "Add node" button → reveals a fresh pairing code with QR.

### 16.3 Page 2 — Submit

Three modes:

1. **Natural language** — single textarea ("What do you want done?"). Goes to the planner. Shows the proposed DAG before execution; user confirms.
2. **Capability picker** — dropdown of all capabilities in the mesh; auto-renders a form from the input JSON Schema.
3. **Remote shell** — multiline command, target selector (checkboxes / chips), run button, streaming merged output below.

### 16.4 Page 3 — Runs

Airflow-style table.

- Columns: ID, plan name, status, started, duration, nodes used, cost, retries.
- Filters: status, node, capability, date range, free-text.
- Click a row → run detail page:
  - DAG visualization (nodes colored by status, edges show data flow)
  - Per-task panel: assigned node, logs (live tail), inputs, outputs
  - Buttons: cancel, retry, retry-from-failed, clone
- Live updating via WebSocket — no refresh needed.

### 16.5 Page 4 — History / Audit

Append-only event log.

- Filters: actor, action type, time, node.
- Shows: who submitted what, from where (UI? CLI? webhook?), what it touched, what it cost.
- Export to JSON/CSV.

### 16.6 Page 5 — Settings

Tabs:

- **Peers** — approve/revoke, pubkeys, trust tiers, last seen
- **Capabilities** — toggle per-node, edit tags, rate limits
- **Secrets** — add/rotate (write-only UI; values never re-displayed)
- **Policy** — view/edit `policy.toml` per node
- **Mesh** — name, default LLM, cost ceilings, retry defaults
- **Schedules** — recurring tasks (cron)

### 16.7 Front-door behavior

When the UI on a non-brain node loads:

- It connects to the brain over the local WebSocket relay.
- All API calls are proxied through. The user perceives no difference.
- If the brain changes mid-session, the UI reconnects transparently.

---

## 17. CLI Specification

```
harness init                              # bootstrap a new mesh
harness join                              # join an existing mesh (interactive)
harness leave                             # gracefully exit the mesh
harness status                            # this node's role, queue, recent tasks
harness peers                             # list mesh members
harness logs [-f] [--node <name>]         # tail logs

harness submit <capability> [--input <json>|@file] [flags]
harness fanout --capability <cap> --inputs <glob> [--concurrency N]
harness plan "<natural language goal>"    # show proposed DAG without running
harness exec "<natural language goal>"    # plan + execute

harness run [--all|--on <node>|--where <expr>] -- <cmd...>
harness ws create|sync|run|destroy        # persistent workspaces

harness caps [--node <name>]              # list capabilities
harness secrets add|rm|list               # manage secrets
harness policy show|edit|apply

harness update                            # rolling self-update
harness backup                            # snapshot ~/.harness
```

Output is human-readable by default, `--json` for machine.

---

## 18. External Integrations

### 18.1 Mobile / web

The UI is mobile-responsive; tapping `harness.local:19198` from a phone on the same Wi-Fi works. Save to home screen for an "app."

(v2) Native iOS/Android wrapper using the same web UI plus push notifications.

### 18.2 Webhooks

Any HTTP POST to `/webhook/<integration>` on any node is forwarded to the brain.

Built-in adapters (configurable in Settings):

- **WhatsApp** — Twilio/Meta webhook signature validation; reply via API
- **SMS** — Twilio
- **iOS Shortcuts** — generic JSON POST + signed token
- **Slack** — slash commands and event subscriptions
- **Telegram bot**
- **Email-to-task** — IMAP poller; subject becomes natural-language goal

Each adapter validates its provider signature and converts the payload into a `Task` submission.

### 18.3 mDNS convenience name

The binary also advertises a single mDNS name `harness.local` that resolves to whichever node is currently brain (or any healthy node — round-robin acceptable). Clients use this stable name without caring about IPs.

---

## 19. Tech Stack

### 19.1 Language & runtime

- **Rust 2024 edition** (or stable at time of build)
- **`tokio`** async runtime
- **MSRV** declared in `Cargo.toml`

### 19.2 Networking

- **`quinn`** for QUIC transport
- **`mdns-sd`** for service discovery (or `iroh` if we want NAT traversal in v1.5)
- **`axum`** for HTTP API + WebSocket
- **`hyper`** under the hood

### 19.3 Cryptography

- **`ed25519-dalek`** for signatures
- **`blake3`** for hashing
- **`age`** or **`chacha20poly1305`** for at-rest secret encryption
- **`argon2`** for admin password hashing

### 19.4 Serialization

- **`serde`** with **`ciborium`** (CBOR) on the wire
- **`serde_json`** for HTTP API and human-facing surfaces
- **`schemars`** for JSON Schema generation from Rust types

### 19.5 Persistence

- **`rusqlite`** (or **`sqlx`** with sqlite feature) for the per-node DB
- WAL mode, single-writer per node
- (v2) Embedded vector store via **`hnsw_rs`** or **`qdrant-client`** for capability semantic search

### 19.6 LLM clients

- **`reqwest`** + custom thin wrappers for Claude, OpenAI, Ollama
- (Optional v2) **`mistral.rs`** or **`candle`** for in-process inference on Rust-friendly hardware

### 19.7 MCP

- **`rmcp`** (Rust MCP SDK) for proxying MCP servers as capabilities

### 19.8 CRDT / state replication

- **`automerge`** for shared state where strong consistency isn't needed
- Lightweight custom gossip layer for high-frequency state (heartbeats, queues)

### 19.9 Observability

- **`tracing`** + **`tracing-subscriber`**
- **`opentelemetry`** for distributed traces (W3C trace context propagated in task envelope)
- Local trace viewer in the UI

### 19.10 Web UI

- **Frontend:** **SvelteKit** built to static files, embedded via `rust-embed`
  - (Alternative: **Leptos** for full-Rust stack — choose based on team preference)
- **Charts:** **Chart.js** or **Recharts**
- **DAG viz:** **Cytoscape.js** or **React Flow** (if React/Leptos chosen)
- **Styling:** **Tailwind CSS**

### 19.11 Build & distribution

- **`cargo`** workspace with multiple crates
- **`cross`** for cross-compilation (linux-x86_64, linux-aarch64, darwin-x86_64, darwin-aarch64, windows-x86_64)
- **`cargo-dist`** for release artifacts and installers
- **GitHub Actions** for CI; signed releases via **`cosign`** or **`minisign`**
- **Single-binary outputs**, ~20–40 MB stripped

### 19.12 Why Rust (justify the choice)

- Single binary, no runtime, easy to install on any machine
- First-class async networking (`tokio` + `quinn`)
- Strong typing for protocol correctness
- Good cross-compile story
- Memory-safe daemon you can trust on a personal LAN
- Performance comparable to Go, much better than Python; matters for high-frequency gossip and large fan-outs

---

## 20. Repository Structure

```
harness/
├── Cargo.toml                  # workspace
├── README.md
├── HARNESS_PRD.md              # this document
├── CLAUDE.md                   # instructions for Claude Code agent flow
├── CHANGELOG.md
├── LICENSE                     # MIT or Apache-2.0
├── crates/
│   ├── harness-core/           # protocol types, traits, glue
│   ├── harness-mesh/           # discovery, transport, gossip, election
│   ├── harness-store/          # SQLite + replication
│   ├── harness-policy/         # policy engine
│   ├── harness-capabilities/   # built-in capability impls
│   │   ├── shell/
│   │   ├── llm/
│   │   ├── mcp/
│   │   ├── fs/
│   │   ├── http/
│   │   └── schedule/
│   ├── harness-orchestrator/   # planner, scheduler, DAG executor
│   ├── harness-api/            # HTTP/WebSocket API (axum)
│   ├── harness-cli/            # clap-based CLI binary
│   ├── harness-ui/             # embedded web UI assets (build script copies dist/)
│   └── harness-daemon/         # the actual `harness` binary that wires it all
├── ui/                         # SvelteKit (or Leptos) source
│   ├── src/
│   ├── package.json
│   └── ...
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
│   └── tutorials/
├── tests/
│   ├── integration/            # multi-node tests via tokio + mock LAN
│   └── e2e/                    # spawn N daemons, run scenarios
├── benchmarks/
└── .github/workflows/
```

---

## 21. Build Phases / Roadmap

Each phase is independently demoable. **Each phase ends with a working, useful artifact**, even if the rest of the system isn't built. Do not skip ahead.

### Phase 0 — Project setup (1–2 days)

- Cargo workspace scaffolded
- CI green
- README with project pitch
- `harness --version` works

### Phase 1 — Mesh skeleton (week 1)

- Identity generation, `~/.harness/` layout
- mDNS discovery, two nodes find each other
- QUIC connection establishment with Noise/TLS
- Signed heartbeats with leader election
- `harness init` / `harness join` (with pairing codes)
- `harness peers` / `harness status`
- Web UI Mesh page (read-only)

**Demo:** install on two laptops, watch them discover each other and elect a brain.

### Phase 2 — Tasks flow (week 2)

- Task envelope, result envelope, signing
- Single-task lifecycle: submit → dispatch → execute → result
- Round-robin dispatch (no scoring yet)
- SQLite task DB with replication via gossip
- HTTP submit API; CLI `harness submit`
- Web UI Submit page (capability picker mode) and Runs page (basic table + live logs)
- Built-in `echo` capability for testing

**Demo:** submit `echo "hello"` from any node, see it execute on another, view in UI.

### Phase 3 — Fleet exec & built-ins (week 3)

- `shell.exec` capability with policy engine
- Streaming stdout/stderr over QUIC
- `harness run --all|--on|--where` CLI
- Web UI Remote Shell mode
- `llm.claude`, `llm.ollama.*`, `mcp.proxy` capabilities
- Auto-detection at startup; capability advertisement

**Demo:** `harness run --all -- uname -a` and `harness run --on gpu-box -- ollama list` working end-to-end.

### Phase 4 — Distribution patterns (week 4)

- Fan-out (`harness fanout`)
- Work-stealing queue
- DAG executor with topological dispatch
- Scheduler scoring (load + latency + success rate)
- Lease-based claiming, retry, partial results
- Web UI DAG visualization

**Demo:** "summarize 50 PDFs across 2 laptops" — fan-out completes in ~half the time of single-node.

### Phase 5 — Planner & external clients (week 5)

- Natural-language submit → LLM-backed planner emits DAG
- WhatsApp/SMS/iOS Shortcuts webhook adapters
- mDNS `harness.local` convenience name
- Cost tracking and budget caps
- Audit log + History UI

**Demo:** WhatsApp message → mesh executes multi-step plan → text reply with cost summary.

### Phase 6 — Hardening & polish (week 6)

- Self-updater (rolling)
- Speculative execution + circuit breakers
- Schedules (cron)
- Secrets management (encrypted, replicated)
- Policy UI
- Mobile-responsive UI pass
- One-line installer (`get.harness.sh`) + Homebrew tap + .deb/.rpm

**Demo:** end-to-end story from §9–§18 working without manual intervention.

### Phase 7 (v2 backlog)

- Cross-LAN federation via Tailscale or iroh
- WASM sandboxed third-party capabilities
- Multi-user UI with role-based access
- Embedded inference (mistral.rs / candle)
- Capability marketplace (signed third-party plugins)
- Cross-mesh sync for traveling users

---

## 22. Testing Strategy

- **Unit** — every protocol type round-trips through CBOR + signature; every scoring/policy decision is deterministic and tested.
- **Integration** — spawn N daemons in-process on different ports, simulate LAN, run multi-node scenarios in CI.
- **End-to-end** — Docker Compose with 3 daemons + a webhook poster + a UI tester (Playwright) running through the canonical demos.
- **Chaos** — kill the brain mid-task; partition the mesh; corrupt the DB; verify recovery.
- **Property tests** — `proptest` on protocol invariants (election always converges; tasks never lost; replication eventually consistent).
- **Fuzzing** — `cargo-fuzz` on wire decoders.
- **Benchmarks** — heartbeat throughput, gossip convergence time, task dispatch latency, fan-out scaling curves.

---

## 23. Operational Concerns

### 23.1 Resource limits

- Daemon process limited to a configurable RAM ceiling (default 512 MB).
- Per-capability concurrency caps to prevent a single node from being saturated.
- Backpressure: when queue depth hits limit, broadcast pause; brain stops dispatching to that node.

### 23.2 Sleep/wake handling

- macOS sleep events trigger graceful pause + lease release.
- On wake, fast-resume reconnect.
- Tasks in flight when a node sleeps are auto-redispatched via lease expiry.

### 23.3 Logging

- Structured JSON logs to file (`~/.harness/logs/`) with rotation.
- `harness logs -f` tails locally; UI tails any node's logs over the mesh.
- Log levels per module via env (`HARNESS_LOG=debug`).

### 23.4 Backup

- `harness backup` snapshots `~/.harness/` (excluding identity key by default).
- Restore on a new machine continues mesh membership iff identity key is included.

### 23.5 Version compatibility

- Protocol version negotiated per connection; minor versions backward-compatible, majors gated.
- Self-updater respects mesh consensus: rolling, one node at a time, automatic rollback on health failure.

### 23.6 Clock skew

- Heartbeat timestamps are advisory; ordering uses monotonic seq + vector clocks.
- NTP recommended but not required.

---

## 24. Success Metrics

For the personal-use phase:

- **Onboarding time** — install to first successful cross-node task: target < 5 minutes for two laptops.
- **Wall-clock speedup** — fan-out scaling factor across N nodes: target ≥ 0.7N for embarrassingly parallel workloads.
- **Crash-free days** — daemon uptime: target ≥ 99% over 30 days on consumer hardware (sleep/wake included).
- **Brain failover** — time from brain offline to new brain serving requests: target < 3s p95.
- **Daily active use** — does the author run something through the harness every day? If not, the design has failed.

For the open-source release phase:

- **Time to first task on a fresh machine** by a new user: target < 10 minutes.
- **Star/issue/PR ratio** — qualitative health.
- **Number of contributed capabilities** — measures whether the capability layer is well-designed.

---

## 25. Open Questions

The implementer should explicitly resolve these and document the resolution in `CHANGELOG.md`:

1. **Discovery resilience.** mDNS is flaky on some routers. Acceptable to require static peer list as fallback, or do we add iroh/DHT in v1?
2. **Web UI framework choice.** SvelteKit is faster to iterate on; Leptos keeps the stack pure-Rust. Decide.
3. **CRDT vs. Raft for state.** Automerge for everything is simpler; a small Raft module for task ownership would be more robust. Pick a default.
4. **Multi-user.** v1 is single-admin. When multi-user is added, how are pairing codes scoped (per user vs per mesh)?
5. **Capability versioning.** SemVer, but how do we handle a worker running v1 of a capability while the brain dispatches v2 input? Negotiate at task time vs reject?
6. **Mobile-native.** Is the responsive web UI enough for v1, or do we need PWA + push notifications for the "WhatsApp-like" experience to feel right?
7. **Secrets blast radius.** Replicating encrypted secrets to every node is convenient but every node becomes a credential target. Worth a per-secret access tier?

---

## 26. Appendix A — Wire Schemas (canonical)

(Included as Rust source so the implementer can `cargo add` and use directly.)

```rust
// crates/harness-core/src/protocol.rs

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type NodeId = [u8; 16];
pub type TaskId = Uuid;
pub type PlanId = Uuid;
pub type Signature = [u8; 64];

// (Full type definitions from §13 included here verbatim.)
```

## 27. Appendix B — `CLAUDE.md` (agent flow)

A separate `CLAUDE.md` will sit at the repo root for Claude Code's agentic flow:

```
# Claude Code Operating Instructions

You are implementing the Harness project specified in HARNESS_PRD.md.

## Operating principles

1. Never skip phases. Complete Phase N's demo before starting Phase N+1.
2. Every PR must include tests and update CHANGELOG.md.
3. When in doubt, refer to HARNESS_PRD.md. If the spec is silent, propose a
   resolution in `docs/decisions/NNNN-title.md` (ADR format) and proceed.
4. Maintain a running `STATE.md` at the repo root tracking current phase, what's
   done, what's next, what's blocked.
5. Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test` before any commit.
6. Prefer simple, small modules over clever ones.

## Stopping conditions

Stop and ask if:
- A protocol-level change would break backwards compatibility.
- A new external dependency is required.
- A security/privacy implication arises that isn't covered by the PRD.
```

---

## 28. Appendix C — Example end-to-end scenario

User on phone, away from home. Two laptops at home are on (`mac` and `linux-box`). User opens WhatsApp, messages the harness bot:

> "Find every PDF in my documents folder mentioning 'Series A term sheet', summarize each, and email me a comparison table."

What happens:

1. WhatsApp webhook hits the home router's port forward → `mac:19198/webhook/whatsapp`.
2. `mac` validates the Twilio signature, forwards the request to the brain (currently `linux-box`).
3. `linux-box`'s planner calls `llm.claude` to compile the goal into a DAG:
   - `t1: fs.search` — PDFs containing "Series A term sheet" (capability tag: `private`)
   - `t2: doc.extract_text` — fan-out across results
   - `t3: llm.summarize` — fan-out, each summary
   - `t4: llm.compose_table` — single Claude call with all summaries
   - `t5: email.send` — to user's address
4. Brain dispatches:
   - `t1` runs on `mac` (the only node with `fs.index` over `~/Documents`)
   - `t2`, `t3` fan-out across both nodes (work-stealing)
   - `t4` runs on whichever node is least loaded
   - `t5` runs on the node with `secret/smtp-credentials`
5. Each task streams logs back; total trace visible in the Runs page on `harness.local:19198`.
6. Result email arrives. WhatsApp gets a reply: "Done — emailed comparison of 7 term sheets. 1m 34s, ~$0.12 in tokens."

User never knew which node ran what. Never used SSH. Never opened a laptop.

---

*End of document. Hand to Claude Code, set phase to 0, begin.*
