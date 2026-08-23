// @vitest-environment node
import { describe, expect, it } from "vitest";
import { activePlans, fillDays, fmtUsd, runRate, utcDay } from "./costs";

// 2026-08-23 12:00 UTC.
const NOW = 1_787_486_400_000;
const DAY = 86_400_000;

describe("fillDays", () => {
  it("fills the whole window with today at the right edge", () => {
    const sparse = [
      { day: utcDay(NOW), usd: 1.5 },
      { day: utcDay(NOW - 2 * DAY), usd: 0.5 },
    ];
    const filled = fillDays(sparse, 7, NOW);
    expect(filled).toHaveLength(7);
    expect(filled[6]).toEqual({ day: "2026-08-23", usd: 1.5 });
    expect(filled[4]).toEqual({ day: "2026-08-21", usd: 0.5 });
    expect(filled[0].usd).toBe(0);
    // Strictly ascending days — time-honest axis.
    for (let i = 1; i < filled.length; i += 1) {
      expect(filled[i].day > filled[i - 1].day).toBe(true);
    }
  });

  it("handles an empty ledger", () => {
    const filled = fillDays([], 3, NOW);
    expect(filled.map((d) => d.usd)).toEqual([0, 0, 0]);
  });
});

describe("runRate", () => {
  it("is null with no spend or <3 elapsed days since first spend", () => {
    expect(runRate([], 30, NOW)).toBeNull();
    // First spend today: 1 elapsed day — a day-old mesh must not
    // project 30x today (plan review MAJOR-5).
    expect(runRate([{ day: utcDay(NOW), usd: 9 }], 30, NOW)).toBeNull();
    expect(
      runRate([{ day: utcDay(NOW - DAY), usd: 9 }], 30, NOW),
    ).toBeNull();
  });

  it("divides by ELAPSED days, not spend-days", () => {
    // Spend on 2 of the last 4 days: denominator 4 (elapsed), not 2.
    const perDay = [
      { day: utcDay(NOW - 3 * DAY), usd: 4 },
      { day: utcDay(NOW - DAY), usd: 4 },
    ];
    const rate = runRate(perDay, 30, NOW);
    expect(rate).not.toBeNull();
    expect(rate).toBeCloseTo((8 / 4) * 30, 9);
  });

  it("clamps the denominator to the window", () => {
    // Earliest datum predates the window start: elapsed = window.
    const perDay = [{ day: utcDay(NOW - 40 * DAY), usd: 30 }];
    // (Out-of-window rows do not reach the UI in practice, but the
    // math must not divide by 40+ days.)
    const rate = runRate(perDay, 30, NOW);
    expect(rate).toBeCloseTo(30, 9);
  });
});

describe("activePlans", () => {
  const perPlan = [
    { plan_id: "p1", name: "demo", actual_usd: 2.5 },
    { plan_id: "p2", name: "done-plan", actual_usd: 1.0 },
  ];

  it("lists non-terminal plan.execute rows with the ledger join", () => {
    const rows = activePlans(
      [
        { id: "t1", capability: "plan.execute", state: "running", plan_id: "p1" },
        { id: "t2", capability: "plan.execute", state: "done", plan_id: "p2" },
        { id: "t3", capability: "echo", state: "running" },
        { id: "t4", capability: "plan.execute", state: "submitted" },
      ],
      perPlan,
    );
    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({
      task_id: "t1",
      plan_id: "p1",
      name: "demo",
      actual_usd: 2.5,
    });
    // A plan that has not spent yet still gets a stop row.
    expect(rows[1]).toEqual({
      task_id: "t4",
      plan_id: null,
      name: null,
      actual_usd: null,
    });
  });
});

describe("fmtUsd", () => {
  it("rounds sensibly across magnitudes", () => {
    expect(fmtUsd(0)).toBe("$0");
    expect(fmtUsd(0.000066)).toBe("$0.0001");
    expect(fmtUsd(1.234)).toBe("$1.23");
    expect(fmtUsd(null)).toBe("—");
  });
});
