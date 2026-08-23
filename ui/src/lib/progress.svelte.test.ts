// @vitest-environment node
//
// Runes-compiled regression for 4.8 diff review BLOCKER-1: the live
// DAG reads `progress.steps[id]` inside effects/deriveds, so the
// reducer's step writes MUST fire Svelte 5 reactivity. A `Map` here
// silently doesn't (the $state proxy never wraps Maps) — this test
// fails if anyone reintroduces one.

import { flushSync } from "svelte";
import { describe, expect, it } from "vitest";
import { applyProgressLine, emptyProgress, type RunProgress } from "./progress";

describe("runes reactivity of the progress reducer", () => {
  it("DagView-shaped effects re-fire as steps light and settle", () => {
    const cleanup = $effect.root(() => {
      let progress = $state<RunProgress>(emptyProgress());
      const states: (string | undefined)[] = [];
      const fractions: (number | null)[] = [];

      $effect(() => {
        // mimics DagView's stateOf(): reads one step's state
        states.push(progress.steps["s1"]?.state);
      });
      $effect(() => {
        fractions.push(progress.fraction);
      });
      flushSync();

      progress = applyProgressLine(
        progress,
        JSON.stringify({
          step: { id: "s1", capability: "echo", state: "in_flight", task_id: "t-1" },
        }),
        2,
      );
      flushSync();

      progress = applyProgressLine(
        progress,
        JSON.stringify({ step: { id: "s1", capability: "echo", state: "done" } }),
        2,
      );
      flushSync();

      expect(states).toEqual([undefined, "in_flight", "done"]);
      expect(fractions).toEqual([null, 0, 0.5]);
    });
    cleanup();
  });
});
