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
  pubkey_fp: string;
  mesh_name: string;
  brain_score: number;
  leader_belief: string | null;
  started_at_ms: number;
  ui_version: string;
}

export type MeshEvent =
  | { type: "peer_added"; peer: PeerDto }
  | { type: "peer_updated"; peer: PeerDto }
  | { type: "peer_evicted"; node_id: string }
  | { type: "leader_changed"; leader: string | null; brain_score: number };
