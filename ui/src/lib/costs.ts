// 5.10 (ADR-0038) — pure logic behind the Costs page. The Svelte page
// stays thin; everything here is unit-tested (the nlsubmit/dag
// convention).

/** Shape of GET /api/v1/costs (5.9 ledger fold). */
export interface CostTotals {
  window_days: number;
  truncated: boolean;
  total_usd: number;
  today_usd: number;
  per_plan: PlanCost[];
  per_issuer: { node_id: string; usd: number }[];
  per_day: { day: string; usd: number }[];
}

export interface PlanCost {
  plan_id: string;
  name?: string | null;
  actual_usd: number;
  reported_spent_usd?: number | null;
  cap_usd?: number | null;
  soft_limit_usd?: number | null;
  triggered?: boolean | null;
  status?: string | null;
}

/** Minimal slice of the tasks listing the page consumes. */
export interface TaskRowLite {
  id: string;
  capability: string;
  state: string;
  plan_id?: string | null;
}

const TERMINAL = new Set(["done", "failed", "cancelled", "expired"]);
const DAY_MS = 86_400_000;

/** `YYYY-MM-DD` in UTC — the ledger's day strings are UTC days. */
export function utcDay(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

/**
 * Fill the window's day slots so the x-axis is time-honest: one entry
 * per UTC day ending TODAY, zero for spend-free days.
 */
export function fillDays(
  perDay: { day: string; usd: number }[],
  windowDays: number,
  nowMs: number,
): { day: string; usd: number }[] {
  const byDay = new Map(perDay.map((d) => [d.day, d.usd]));
  const out: { day: string; usd: number }[] = [];
  for (let i = windowDays - 1; i >= 0; i -= 1) {
    const day = utcDay(nowMs - i * DAY_MS);
    out.push({ day, usd: byDay.get(day) ?? 0 });
  }
  return out;
}

/**
 * Projected 30-day run rate (§17.8's "projected total", honest
 * version): window total ÷ ELAPSED days × 30 — the denominator is
 * days elapsed since max(window start, earliest datum), clamped ≥ 1,
 * and null until ≥ 3 elapsed days exist (a day-old mesh must not
 * project 30× today). Today is a partial day and is included in the
 * elapsed count — the projection reads slightly low intra-day rather
 * than high.
 */
export function runRate(
  perDay: { day: string; usd: number }[],
  windowDays: number,
  nowMs: number,
): number | null {
  // Days before the rendered window are excluded from the numerator
  // too (diff review m4): the server's ms-precise window can hand us
  // spend on a partial 31st UTC day the chart never draws — counting
  // it would over-project against a denominator clamped to the
  // window. Chart and projection read the same days.
  const windowStart = utcDay(nowMs - (windowDays - 1) * DAY_MS);
  const spendDays = perDay.filter((d) => d.usd > 0 && d.day >= windowStart);
  if (spendDays.length === 0) return null;
  const earliest = spendDays.map((d) => d.day).sort()[0];
  const since = earliest > windowStart ? earliest : windowStart;
  const sinceMs = Date.parse(`${since}T00:00:00Z`);
  const elapsed = Math.max(1, Math.floor((nowMs - sinceMs) / DAY_MS) + 1);
  if (elapsed < 3) return null;
  const total = spendDays.reduce((acc, d) => acc + d.usd, 0);
  return (total / elapsed) * 30;
}

export interface ActivePlan {
  task_id: string;
  plan_id: string | null;
  name: string | null;
  actual_usd: number | null;
}

/**
 * Non-terminal plan.execute rows, joined to ledger rows by plan_id
 * (stamped at mint since 5.10). Rows without a ledger match still
 * list — the stop button must reach a plan that has not yet spent.
 */
export function activePlans(
  tasks: TaskRowLite[],
  perPlan: PlanCost[],
): ActivePlan[] {
  const ledger = new Map(perPlan.map((p) => [p.plan_id, p]));
  return tasks
    .filter(
      (t) =>
        t.capability === "plan.execute" &&
        !TERMINAL.has(String(t.state).toLowerCase()),
    )
    .map((t) => {
      const hit = t.plan_id ? ledger.get(t.plan_id) : undefined;
      return {
        task_id: t.id,
        plan_id: t.plan_id ?? null,
        name: hit?.name ?? null,
        actual_usd: hit ? hit.actual_usd : null,
      };
    });
}

/** `$1.2346` under a cent, `$1.23` otherwise, `$0` for zero. */
export function fmtUsd(v: number | null | undefined): string {
  if (v === null || v === undefined) return "—";
  if (v === 0) return "$0";
  if (Math.abs(v) < 0.01) return `$${v.toFixed(4)}`;
  return `$${v.toFixed(2)}`;
}
