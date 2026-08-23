// @vitest-environment node
// 5.4 — NL Submit core: CLI-parity bodies, outcome parsing, poller.

import { describe, expect, it } from "vitest";

import {
  CLI_SLACK_MS,
  EXEC_TIMEOUT_MS,
  PLAN_TIMEOUT_MS,
  execSubmitBody,
  inputPreview,
  parsePlanOutcome,
  planSubmitBody,
  pollTask,
} from "./nlsubmit";
import type { TaskDetailDto } from "./types";

function task(state: string, extra: Partial<TaskDetailDto> = {}): TaskDetailDto {
  return {
    id: "t-1",
    capability: "brain.plan",
    state,
    input: {},
    issued_at_ms: 0,
    ...extra,
  };
}

describe("planSubmitBody", () => {
  it("matches the CLI plan_input shape, constraint only when opted in", () => {
    // Parity fixture: crates/harness-cli/src/plan.rs plan_input +
    // submit_and_poll's execution envelope (review BLOCKER-1 — a
    // bare submit gets the 30s ExecutionPolicy default).
    expect(planSubmitBody("run: ls", false)).toEqual({
      capability: "brain.plan",
      input: { goal: "run: ls" },
      execution: {
        redundancy: 1,
        timeout_ms: PLAN_TIMEOUT_MS + CLI_SLACK_MS,
        on_partial: "fail_fast",
        lease_ms: 10_000,
      },
    });
    expect(planSubmitBody("run: ls", true).input).toEqual({
      goal: "run: ls",
      constraints: { allow_cloud: true },
    });
  });
});

describe("execSubmitBody", () => {
  it("wraps the plan verbatim with the exec envelope", () => {
    const plan = { id: "p", tasks: {}, edges: [], sig: "s" };
    const body = execSubmitBody(plan, false);
    expect(body.capability).toBe("plan.execute");
    expect(body.input).toEqual({ plan, timeout_ms: EXEC_TIMEOUT_MS });
    expect(body.execution?.timeout_ms).toBe(EXEC_TIMEOUT_MS + CLI_SLACK_MS);
  });

  it("maps keep-going to on_failure: continue (CLI --keep-going)", () => {
    const body = execSubmitBody({}, true);
    expect((body.input as Record<string, unknown>).on_failure).toBe("continue");
  });
});

describe("parsePlanOutcome", () => {
  it("maps done output onto the preview fields", () => {
    const out = parsePlanOutcome(
      task("done", {
        output: {
          plan: { tasks: {}, edges: [] },
          confidence: 0.85,
          rationale: "one step",
          estimated_cost_usd: 0.01,
          estimated_duration_ms: 40,
        },
      }),
    );
    expect(out).toEqual({
      kind: "planned",
      plan: { tasks: {}, edges: [] },
      confidence: 0.85,
      rationale: "one step",
      costUsd: 0.01,
      durationMs: 40,
    });
  });

  it("failed task surfaces the 5.3 diagnostics verbatim", () => {
    const out = parsePlanOutcome(
      task("failed", { error: "no backend produced a confident plan: …" }),
    );
    expect(out).toEqual({
      kind: "failed",
      error: "no backend produced a confident plan: …",
    });
  });

  it("non-terminal states are pending; malformed output never throws", () => {
    expect(parsePlanOutcome(task("running")).kind).toBe("pending");
    expect(parsePlanOutcome(task("done", { output: "not an object" }))).toEqual({
      kind: "failed",
      error: "planner returned no plan object",
    });
    expect(parsePlanOutcome(task("done", { output: { plan: 7 } })).kind).toBe(
      "failed",
    );
  });
});

type FetchStep = { status: number; json?: unknown; body?: string };

function fetchScript(steps: FetchStep[]): {
  fetchFn: typeof fetch;
  calls: () => number;
} {
  let i = 0;
  const fetchFn = (async () => {
    const step = steps[Math.min(i, steps.length - 1)];
    i += 1;
    return {
      ok: step.status >= 200 && step.status < 300,
      status: step.status,
      json: async () => step.json,
      text: async () => step.body ?? "",
    } as unknown as Response;
  }) as typeof fetch;
  return { fetchFn, calls: () => i };
}

const instant = () => Promise.resolve();

describe("pollTask", () => {
  it("returns terminal immediately on a done task", async () => {
    const { fetchFn, calls } = fetchScript([
      { status: 200, json: task("done", { output: {} }) },
    ]);
    const res = await pollTask(fetchFn, "t-1", { sleep: instant });
    expect(res.kind).toBe("terminal");
    expect(calls()).toBe(1);
  });

  it("polls through pending states to terminal", async () => {
    const { fetchFn, calls } = fetchScript([
      { status: 200, json: task("submitted") },
      { status: 200, json: task("running") },
      { status: 200, json: task("failed", { error: "boom" }) },
    ]);
    const res = await pollTask(fetchFn, "t-1", { sleep: instant });
    expect(res.kind).toBe("terminal");
    expect(calls()).toBe(3);
  });

  it("401 mid-sequence becomes a typed unauthorized outcome", async () => {
    const { fetchFn } = fetchScript([
      { status: 200, json: task("running") },
      { status: 401, body: "unauthorized" },
    ]);
    const res = await pollTask(fetchFn, "t-1", { sleep: instant });
    expect(res).toEqual({ kind: "unauthorized" });
  });

  it("persistent 500 fails fast, never spinning out the budget", async () => {
    const { fetchFn, calls } = fetchScript([{ status: 500, body: "load_failed" }]);
    const res = await pollTask(fetchFn, "t-1", { sleep: instant });
    expect(res).toEqual({ kind: "http_error", status: 500, body: "load_failed" });
    expect(calls()).toBe(1);
  });

  it("exhausts the budget on a never-terminal task", async () => {
    const { fetchFn } = fetchScript([{ status: 200, json: task("running") }]);
    const res = await pollTask(fetchFn, "t-1", {
      sleep: instant,
      intervalMs: 1000,
      budgetMs: 3000,
    });
    expect(res).toEqual({ kind: "timeout" });
  });

  it("abort stops the loop with no further fetches", async () => {
    const ctrl = new AbortController();
    let calls = 0;
    const fetchFn = (async () => {
      calls += 1;
      ctrl.abort();
      return {
        ok: true,
        status: 200,
        json: async () => task("running"),
        text: async () => "",
      } as unknown as Response;
    }) as typeof fetch;
    const res = await pollTask(fetchFn, "t-1", {
      sleep: instant,
      signal: ctrl.signal,
    });
    expect(res).toEqual({ kind: "aborted" });
    expect(calls).toBe(1);
  });
});

describe("inputPreview", () => {
  it("caps at 60 chars with an ellipsis (CLI truncate_chars parity)", () => {
    const long = { msg: "x".repeat(200) };
    const p = inputPreview(long);
    expect(p.length).toBe(61);
    expect(p.endsWith("…")).toBe(true);
    expect(inputPreview({ a: 1 })).toBe('{"a":1}');
  });
});
