// Pure reducers turning `progress`-stream partial frames into UI
// state (roadmap 4.8): a per-task completion fraction and the live
// per-step plan states that drive the DAG node fills.
//
// Frame families (each a JSON object in a PartialFrame `line`):
// - `{"step": ...}` / `{"plan_summary": ...}` — plan.execute
//   (crates/harness-capabilities/src/plan_exec.rs). `in_flight` frames
//   fire at submit time; settle frames are terminal per step;
//   `plan_summary` is terminal-only, so the LIVE fraction is
//   settled-steps ÷ plan size.
// - `{"target": ..., "completed": n, "total": m}` and
//   `{"summary": {...}}` — mesh.* scope wrappers
//   (crates/harness-capabilities/src/mesh_meta.rs).
// - `{"federated": {stage, completed?, total?, ...}}` — the federated
//   coordinator (crates/harness-daemon/src/federated.rs).
//
// Malformed lines are ignored — partials are best-effort telemetry
// (ADR-0020); the terminal result stays authoritative.

export interface StepView {
  capability: string;
  state: string;
  taskId?: string;
  error?: string;
}

export interface RunProgress {
  /** 0..=1 when derivable, null → indeterminate. */
  fraction: number | null;
  /**
   * Plan-node id → live view (plan.execute parents only). A plain
   * object ON PURPOSE (diff review BLOCKER-1): Svelte 5's `$state`
   * proxies deep-track plain objects/arrays but never `Map`, so a
   * Map here renders once and then silently stops updating the DAG.
   */
  steps: Record<string, StepView>;
  /** Federated per-node settle lines, in arrival order. */
  federated: { node_name: string; outcome: string }[];
  /** True once a terminal summary frame arrived. */
  summarized: boolean;
}

export function emptyProgress(): RunProgress {
  return { fraction: null, steps: {}, federated: [], summarized: false };
}

const TERMINAL_STEP_STATES = new Set(["done", "failed", "timed_out", "skipped"]);

/**
 * Fold one `progress` frame line into the running view. `planSize` is
 * `Object.keys(input.plan.tasks).length` for plan parents (0 = not a
 * plan). Mutates and returns the same object — under a `$state` proxy
 * the property writes themselves are what fire reactivity.
 */
export function applyProgressLine(
  view: RunProgress,
  line: string,
  planSize: number,
): RunProgress {
  let chunk: unknown;
  try {
    chunk = JSON.parse(line);
  } catch {
    return view; // best-effort telemetry — never break the page
  }
  if (typeof chunk !== "object" || chunk === null) return view;
  const c = chunk as Record<string, unknown>;

  const step = c["step"] as
    | { id?: string; capability?: string; state?: string; task_id?: string; error?: string }
    | undefined;
  if (step && typeof step.id === "string") {
    const prev = view.steps[step.id];
    // A settle frame never regresses to in_flight (frames can arrive
    // ring-batched out of order across ticks).
    const next = step.state ?? "waiting";
    if (!prev || !TERMINAL_STEP_STATES.has(prev.state) || TERMINAL_STEP_STATES.has(next)) {
      view.steps[step.id] = {
        capability: step.capability ?? prev?.capability ?? "?",
        state: next,
        taskId: step.task_id ?? prev?.taskId,
        error: step.error ?? prev?.error,
      };
    }
    if (planSize > 0) {
      let settled = 0;
      for (const s of Object.values(view.steps)) {
        if (TERMINAL_STEP_STATES.has(s.state)) settled += 1;
      }
      view.fraction = Math.min(1, settled / planSize);
    }
    return view;
  }

  if (c["plan_summary"]) {
    view.fraction = 1;
    view.summarized = true;
    return view;
  }

  // mesh.* wrappers: per-target completions carry completed/total.
  const completed = c["completed"];
  const total = c["total"];
  if (typeof completed === "number" && typeof total === "number" && total > 0) {
    view.fraction = Math.min(1, completed / total);
    return view;
  }
  if (c["summary"]) {
    view.fraction = 1;
    view.summarized = true;
    return view;
  }

  const fed = c["federated"] as
    | {
        stage?: string;
        node_name?: string;
        outcome?: string;
        completed?: number;
        total?: number;
      }
    | undefined;
  if (fed) {
    if (
      fed.stage === "streaming" &&
      typeof fed.completed === "number" &&
      typeof fed.total === "number" &&
      fed.total > 0
    ) {
      view.fraction = Math.min(1, fed.completed / fed.total);
      if (fed.node_name && fed.outcome) {
        view.federated.push({ node_name: fed.node_name, outcome: fed.outcome });
      }
    } else if (fed.stage === "fanout_settled" || fed.stage === "merging") {
      view.fraction = 1;
    }
    return view;
  }

  return view;
}
