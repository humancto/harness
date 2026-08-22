// Wire types — must match crates/harness-api/src/dto.rs.
// If you change one side, change the other; the integration tests
// snapshot the JSON shape so drift is caught.

export interface ResourcesDto {
  cpu_load_avg: number | null;
  cpu_count: number | null;
  ram_total_bytes: number | null;
  ram_avail_bytes: number | null;
  battery_percent: number | null;
  on_ac: boolean | null;
  has_gpu: boolean | null;
}

export interface PeerDto {
  node_id: string;
  pubkey_fp: string;
  mesh_name: string;
  brain_score: number;
  leader_belief: string | null;
  seq: number;
  last_seen_ms_ago: number;
  resources: ResourcesDto | null;
  capabilities_summary: string[];
  // 3.3-fanout: from the announced manifest; absent until it lands.
  node_name?: string | null;
  os?: string | null;
}

export interface LocalDto extends PeerDto {
  is_local: true;
}

export interface PeersSnapshot {
  // Local node first, then peers sorted by node_id.
  local: LocalDto;
  peers: PeerDto[];
  leader_belief: string | null;
  fetched_at_ms: number;
}

export interface StatusDto {
  node_id: string;
  node_name?: string;
  pubkey_fp: string;
  mesh_name: string;
  brain_score: number;
  leader_belief: string | null;
  started_at_ms: number;
  ui_version: string;
}

// --- Tasks (crates/harness-api/src/routes/tasks.rs) ---

// Mirrors harness_core::Constraints as serialized by `harness run`
// (crates/harness-cli/src/run.rs submit_task).
export interface ConstraintsDto {
  deadline: number | null;
  max_cost_usd: number | null;
  must_be_local: boolean;
  require_tags: string[];
  exclude_tags: string[];
  /** NodeId as raw bytes (16), e.g. from hexToBytes(node_id). */
  pin_to_node: number[] | null;
  pin_to_scope: string | null;
}

// Mirrors harness_core::ExecutionPolicy.
export interface ExecutionPolicyDto {
  redundancy: number;
  timeout_ms: number;
  on_partial: "fail_fast" | "best_effort";
  lease_ms: number;
}

// Body for POST /api/v1/tasks (SubmitRequest in tasks.rs).
export interface SubmitTaskRequest {
  capability: string;
  input: unknown;
  constraints?: ConstraintsDto;
  execution?: ExecutionPolicyDto;
  tags?: string[];
}

// 201 response of POST /api/v1/tasks (SubmitResponse in tasks.rs).
export interface SubmitTaskResponse {
  task_id: string;
  state: string;
}

export type TaskState =
  | "submitted"
  | "planned"
  | "dispatched"
  | "assigned"
  | "claimed"
  | "running"
  | "done"
  | "failed"
  | "expired"
  | "cancelled";

// GET /api/v1/tasks/{id} — output/error/completed_at_ms are omitted
// (not null) until the task is terminal.
export interface TaskDetailDto {
  id: string;
  capability: string;
  state: TaskState | string;
  input: unknown;
  issued_at_ms: number;
  completed_at_ms?: number;
  output?: unknown;
  error?: string;
}

// One frame of `WS /api/v1/runs/<task_id>` — must match
// crates/harness-api/src/routes/runs.rs `RunStreamEvent`. `output` /
// `error` are omitted (not null) until the terminal frame; the server
// closes the socket after sending it.
export interface RunStreamEvent {
  state: TaskState | string;
  output?: unknown;
  error?: string;
}

// shell.exec `output` payload (harness-capabilities shell executor).
export interface ShellExecOutput {
  stdout: string;
  stderr: string;
  exit_code: number | null;
  timed_out: boolean;
  stdout_truncated_bytes?: number;
  stderr_truncated_bytes?: number;
}

export type MeshEvent =
  | { type: "peer_added"; peer: PeerDto }
  | { type: "peer_updated"; peer: PeerDto }
  | { type: "peer_evicted"; node_id: string }
  | { type: "leader_changed"; leader: string | null; brain_score: number };
