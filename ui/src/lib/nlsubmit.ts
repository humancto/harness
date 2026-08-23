// Natural-language Submit core (roadmap 5.4).
//
// Pure, node-env-testable plumbing behind the Submit page's NL mode:
// request-body construction (byte-parity with the CLI's `harness
// plan`/`exec` — crates/harness-cli/src/plan.rs), brain.plan outcome
// parsing, and a deterministic task poller with injected fetch.
//
// The planning phase POLLS rather than opening `WS /runs/:id`:
// brain.plan is a single task that emits no step frames, so polling
// is the right primitive; the EXECUTION phase gets the live WS page
// by navigating to /runs/<task id> after confirm.

import type { SubmitTaskRequest, TaskDetailDto } from "./types";

// CLI parity constants (crates/harness-cli/src/plan.rs). The
// execution ENVELOPE must be sent explicitly: a bare submit gets
// `ExecutionPolicy::default()` = 30 s, which would kill the 210 s
// planner chain before LocalStrong ever answered (plan review
// BLOCKER-1).
export const PLAN_TIMEOUT_MS = 240_000;
export const EXEC_TIMEOUT_MS = 120_000;
export const CLI_SLACK_MS = 5_000;

function envelope(timeoutMs: number): SubmitTaskRequest["execution"] {
  return {
    redundancy: 1,
    timeout_ms: timeoutMs + CLI_SLACK_MS,
    on_partial: "fail_fast",
    lease_ms: 10_000,
  };
}

/** The `brain.plan` submit body. `allowCloud` is the per-task cloud
 * opt-in (5.2 ADR-0031, the `--cloud` flag's equivalent) — the mesh
 * policy must ALSO approve escalation or the constraint is inert. */
export function planSubmitBody(goal: string, allowCloud: boolean): SubmitTaskRequest {
  const input: Record<string, unknown> = allowCloud
    ? { goal, constraints: { allow_cloud: true } }
    : { goal };
  return {
    capability: "brain.plan",
    input,
    execution: envelope(PLAN_TIMEOUT_MS),
  };
}

/** The `plan.execute` submit body. The plan JSON from the preview is
 * resubmitted VERBATIM — the server re-validates it (5.3 ruleset) at
 * execute time, so a stale/tampered payload cannot bypass anything. */
export function execSubmitBody(plan: unknown, keepGoing: boolean): SubmitTaskRequest {
  const input: Record<string, unknown> = { plan, timeout_ms: EXEC_TIMEOUT_MS };
  if (keepGoing) {
    input.on_failure = "continue";
  }
  return {
    capability: "plan.execute",
    input,
    execution: envelope(EXEC_TIMEOUT_MS),
  };
}

export type PlanOutcome =
  | {
      kind: "planned";
      plan: unknown;
      confidence: number;
      rationale: string;
      costUsd: number;
      durationMs: number;
    }
  | { kind: "failed"; error: string }
  | { kind: "pending" };

/** Interpret a polled brain.plan task. Tolerant of missing fields —
 * never throws on malformed output (a failed decode reads as a
 * failure with whatever the server said). */
export function parsePlanOutcome(task: TaskDetailDto): PlanOutcome {
  const state = String(task.state);
  if (state === "failed" || state === "cancelled" || state === "expired") {
    return { kind: "failed", error: task.error ?? `planning ${state}` };
  }
  if (state !== "done") {
    return { kind: "pending" };
  }
  const out = (task.output ?? {}) as Record<string, unknown>;
  const plan = out.plan;
  if (typeof plan !== "object" || plan === null) {
    return { kind: "failed", error: "planner returned no plan object" };
  }
  return {
    kind: "planned",
    plan,
    confidence: typeof out.confidence === "number" ? out.confidence : 0,
    rationale: typeof out.rationale === "string" ? out.rationale : "",
    costUsd: typeof out.estimated_cost_usd === "number" ? out.estimated_cost_usd : 0,
    durationMs:
      typeof out.estimated_duration_ms === "number" ? out.estimated_duration_ms : 0,
  };
}

export type PollResult =
  | { kind: "terminal"; task: TaskDetailDto }
  | { kind: "timeout" }
  | { kind: "aborted" }
  | { kind: "unauthorized" }
  | { kind: "http_error"; status: number; body: string };

const TERMINAL_STATES = new Set(["done", "failed", "cancelled", "expired"]);

export interface PollOptions {
  /** Between polls. Default 1000. */
  intervalMs?: number;
  /** Overall budget. Default envelope + 2×slack (CLI parity). */
  budgetMs?: number;
  signal?: AbortSignal;
  /** Injected clock/sleep for tests. Defaults to real setTimeout. */
  sleep?: (ms: number) => Promise<void>;
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Poll `GET /api/v1/tasks/:id` until terminal. Typed non-OK outcomes
 * (plan review MAJOR-2): 401 → `unauthorized` so the page can flip
 * its AuthGate mid-poll; any other non-OK → `http_error` immediately
 * (a persistent 500 must not spin for the whole budget). The signal
 * aborts between polls; the caller ties it to component teardown so
 * navigation never leaves a background loop writing stale state
 * (plan review MAJOR-3).
 */
export async function pollTask(
  fetchFn: typeof fetch,
  taskId: string,
  opts: PollOptions = {},
): Promise<PollResult> {
  const interval = opts.intervalMs ?? 1000;
  const budget = opts.budgetMs ?? PLAN_TIMEOUT_MS + 2 * CLI_SLACK_MS;
  const sleep = opts.sleep ?? defaultSleep;
  let elapsed = 0;

  for (;;) {
    if (opts.signal?.aborted) {
      return { kind: "aborted" };
    }
    let res: Response;
    try {
      res = await fetchFn(`/api/v1/tasks/${taskId}`, {
        headers: { Accept: "application/json" },
        signal: opts.signal,
      });
    } catch (err) {
      if (opts.signal?.aborted) {
        return { kind: "aborted" };
      }
      throw err;
    }
    if (res.status === 401) {
      return { kind: "unauthorized" };
    }
    if (!res.ok) {
      return { kind: "http_error", status: res.status, body: await res.text() };
    }
    const task = (await res.json()) as TaskDetailDto;
    if (TERMINAL_STATES.has(String(task.state))) {
      return { kind: "terminal", task };
    }
    if (elapsed >= budget) {
      return { kind: "timeout" };
    }
    await sleep(interval);
    elapsed += interval;
  }
}

/** Char-bounded one-line preview of a step input (CLI parity:
 * `truncate_chars(s, 60)` in plan.rs — JS slice is code-unit-safe,
 * no boundary panic analog, but the length + ellipsis match). */
export function inputPreview(input: unknown, max = 60): string {
  let s: string;
  try {
    s = JSON.stringify(input) ?? "null";
  } catch {
    s = String(input);
  }
  return s.length > max ? `${s.slice(0, max)}…` : s;
}
