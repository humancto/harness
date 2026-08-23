// @vitest-environment node
//
// Pure-function tests — no DOM needed (jsdom is an optional vitest
// peer and intentionally not a dependency).

import { describe, expect, it } from "vitest";
import { applyProgressLine, emptyProgress } from "./progress";

describe("applyProgressLine", () => {
  it("tracks plan steps: in_flight lights, settle lands, fraction follows", () => {
    let v = emptyProgress();
    v = applyProgressLine(
      v,
      JSON.stringify({
        step: { id: "s1", capability: "echo", state: "in_flight", task_id: "t-1" },
      }),
      2,
    );
    expect(v.steps.get("s1")?.state).toBe("in_flight");
    expect(v.steps.get("s1")?.taskId).toBe("t-1");
    expect(v.fraction).toBe(0);

    v = applyProgressLine(
      v,
      JSON.stringify({ step: { id: "s1", capability: "echo", state: "done" } }),
      2,
    );
    expect(v.steps.get("s1")?.state).toBe("done");
    // task_id learned at in_flight survives the settle frame.
    expect(v.steps.get("s1")?.taskId).toBe("t-1");
    expect(v.fraction).toBe(0.5);

    v = applyProgressLine(
      v,
      JSON.stringify({ step: { id: "s2", capability: "echo", state: "skipped" } }),
      2,
    );
    expect(v.fraction).toBe(1);
  });

  it("never regresses a settled step back to in_flight", () => {
    let v = emptyProgress();
    v = applyProgressLine(
      v,
      JSON.stringify({ step: { id: "s1", capability: "echo", state: "failed", error: "boom" } }),
      1,
    );
    v = applyProgressLine(
      v,
      JSON.stringify({ step: { id: "s1", capability: "echo", state: "in_flight" } }),
      1,
    );
    expect(v.steps.get("s1")?.state).toBe("failed");
    expect(v.steps.get("s1")?.error).toBe("boom");
  });

  it("derives the fraction from mesh completed/total frames and finishes on summary", () => {
    let v = emptyProgress();
    v = applyProgressLine(v, JSON.stringify({ target: { node: "n" }, completed: 1, total: 4 }), 0);
    expect(v.fraction).toBe(0.25);
    v = applyProgressLine(v, JSON.stringify({ summary: { total: 4 } }), 0);
    expect(v.fraction).toBe(1);
    expect(v.summarized).toBe(true);
  });

  it("follows federated streaming frames and settle", () => {
    let v = emptyProgress();
    v = applyProgressLine(
      v,
      JSON.stringify({
        federated: { stage: "streaming", node_name: "node-b", outcome: "ok", completed: 1, total: 2 },
      }),
      0,
    );
    expect(v.fraction).toBe(0.5);
    expect(v.federated).toEqual([{ node_name: "node-b", outcome: "ok" }]);
    v = applyProgressLine(v, JSON.stringify({ federated: { stage: "fanout_settled", ok: 2 } }), 0);
    expect(v.fraction).toBe(1);
  });

  it("plan_summary is terminal", () => {
    let v = emptyProgress();
    v = applyProgressLine(v, JSON.stringify({ plan_summary: { ok: 2, total: 2 } }), 2);
    expect(v.fraction).toBe(1);
    expect(v.summarized).toBe(true);
  });

  it("ignores malformed lines and unknown shapes", () => {
    let v = emptyProgress();
    v = applyProgressLine(v, "not json {", 2);
    v = applyProgressLine(v, JSON.stringify({ something: "else" }), 2);
    v = applyProgressLine(v, JSON.stringify(null), 2);
    expect(v.fraction).toBeNull();
    expect(v.steps.size).toBe(0);
  });
});
