// Pure DAG layout for the run-detail visualization (roadmap 4.8).
//
// Input: the `plan` object from a plan.execute task's input (node map
// + edges, where `[from, to]` means "from DEPENDS ON to" — `to` runs
// first). Output: layered coordinates plus edge paths drawn from each
// dependency to its dependent (arrow points at the LATER step),
// mirroring the orientation lock in
// crates/harness-core/src/protocol/plan.rs
// (`plan_edges_express_from_depends_on_to`).
//
// No graph library (single-binary rust-embed budget): Kahn layering is
// O(V+E) and MAX_PLAN_STEPS is 64 server-side, so a scrollable SVG is
// plenty. Cycles cannot reach us from the executor (it validates), but
// the layout is defensive: a cycle yields `{ cycle: true }` instead of
// a hang or a lie.

export interface DagNodeBox {
  /** Plan-node id (uuid string — the step-frame `step.id` key). */
  id: string;
  capability: string;
  /** Topological layer, 0 = no dependencies. */
  layer: number;
  /** Lane within the layer (stable: sorted by id). */
  lane: number;
  x: number;
  y: number;
}

export interface DagEdgePath {
  /** Dependency (runs first) — the arrow TAIL. */
  from: string;
  /** Dependent (runs after) — the arrow HEAD. */
  to: string;
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}

export interface DagLayout {
  cycle: boolean;
  nodes: DagNodeBox[];
  edges: DagEdgePath[];
  width: number;
  height: number;
}

export const NODE_W = 148;
export const NODE_H = 44;
const GAP_X = 36;
const GAP_Y = 56;
const PAD = 16;

/**
 * Layer the DAG with Kahn's algorithm and assign SVG coordinates.
 * `edges` entries are `[dependent, dependency]` (= "from depends on
 * to"), exactly as serialized in `Plan.edges`.
 */
export function layoutDag(
  taskIds: string[],
  capabilities: Record<string, string>,
  edges: [string, string][],
): DagLayout {
  const ids = [...taskIds].sort();
  const inSet = new Set(ids);
  // deps[dependent] = its dependencies; out[dependency] = dependents.
  const depCount = new Map<string, number>();
  const dependents = new Map<string, string[]>();
  for (const id of ids) {
    depCount.set(id, 0);
    dependents.set(id, []);
  }
  for (const [from, to] of edges) {
    if (!inSet.has(from) || !inSet.has(to)) continue; // defensive
    depCount.set(from, (depCount.get(from) ?? 0) + 1);
    dependents.get(to)?.push(from);
  }

  const layerOf = new Map<string, number>();
  let frontier = ids.filter((id) => (depCount.get(id) ?? 0) === 0);
  let layer = 0;
  let placed = 0;
  const remaining = new Map(depCount);
  while (frontier.length > 0) {
    const next: string[] = [];
    for (const id of frontier.sort()) {
      layerOf.set(id, layer);
      placed += 1;
      for (const dep of dependents.get(id) ?? []) {
        const left = (remaining.get(dep) ?? 0) - 1;
        remaining.set(dep, left);
        if (left === 0) next.push(dep);
      }
    }
    frontier = next;
    layer += 1;
  }
  if (placed !== ids.length) {
    return { cycle: true, nodes: [], edges: [], width: 0, height: 0 };
  }

  // Lanes: stable order (sorted ids) within each layer.
  const lanes = new Map<number, number>();
  const nodes: DagNodeBox[] = ids.map((id) => {
    const l = layerOf.get(id) ?? 0;
    const lane = lanes.get(l) ?? 0;
    lanes.set(l, lane + 1);
    return {
      id,
      capability: capabilities[id] ?? "?",
      layer: l,
      lane,
      x: PAD + lane * (NODE_W + GAP_X),
      y: PAD + l * (NODE_H + GAP_Y),
    };
  });
  const byId = new Map(nodes.map((n) => [n.id, n]));

  // Arrows run dependency → dependent (execution order).
  const edgePaths: DagEdgePath[] = [];
  for (const [dependent, dependency] of edges) {
    const tail = byId.get(dependency);
    const head = byId.get(dependent);
    if (!tail || !head) continue;
    edgePaths.push({
      from: dependency,
      to: dependent,
      x1: tail.x + NODE_W / 2,
      y1: tail.y + NODE_H,
      x2: head.x + NODE_W / 2,
      y2: head.y,
    });
  }

  const maxLane = Math.max(0, ...nodes.map((n) => n.lane));
  const maxLayer = Math.max(0, ...nodes.map((n) => n.layer));
  return {
    cycle: false,
    nodes,
    edges: edgePaths,
    width: PAD * 2 + (maxLane + 1) * NODE_W + maxLane * GAP_X,
    height: PAD * 2 + (maxLayer + 1) * NODE_H + maxLayer * GAP_Y,
  };
}
