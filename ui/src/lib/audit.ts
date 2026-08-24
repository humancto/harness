// 5.13b (ADR-0041) — pure logic behind the History page. The Svelte
// page stays thin; everything here is unit-tested (the costs.ts /
// dag.ts convention).

/** One row of `GET /api/v1/audit`. */
export interface AuditEntry {
  node: string;
  seq: number;
  at_ms: number;
  action: string;
  subject?: string | null;
  detail?: unknown;
  actor: string;
  entry_hash: string;
}

/** The page-scoped verification block 5.13a returns. */
export interface Verification {
  scope: "page" | "none";
  node: string;
  checked: boolean;
  verified?: boolean;
  from_seq?: number;
  through_seq?: number;
  broken_at_seq?: number;
  error?: string;
}

export interface AuditPage {
  entries: AuditEntry[];
  next_cursor?: {
    before_ms: number;
    before_node: string;
    before_seq: number;
  } | null;
  verification: Verification;
}

/** Client-side filters (§18.6: actor, action, time, node). */
export interface AuditFilters {
  actor?: string;
  action?: string;
  node?: string;
  /** Inclusive lower bound, unix ms. */
  since_ms?: number;
  /** Exclusive upper bound, unix ms. */
  until_ms?: number;
}

/**
 * Query string for the audit endpoint.
 *
 * Only `action`, `node` and the cursor are server-side; actor and the
 * time window are applied client-side over the fetched page, because
 * the endpoint indexes action and node but not actor.
 */
export function auditQuery(
  filters: AuditFilters,
  cursor?: AuditPage["next_cursor"],
  limit = 100,
): string {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  if (filters.action) params.set("action", filters.action);
  if (filters.node) params.set("node", filters.node);
  if (cursor) {
    params.set("before_ms", String(cursor.before_ms));
    params.set("before_node", cursor.before_node);
    params.set("before_seq", String(cursor.before_seq));
  }
  return `/api/v1/audit?${params.toString()}`;
}

/** Apply the filters the server does not index. */
export function applyFilters(
  entries: AuditEntry[],
  filters: AuditFilters,
): AuditEntry[] {
  return entries.filter((e) => {
    if (filters.actor && !e.actor.startsWith(filters.actor)) return false;
    if (filters.since_ms !== undefined && e.at_ms < filters.since_ms) {
      return false;
    }
    if (filters.until_ms !== undefined && e.at_ms >= filters.until_ms) {
      return false;
    }
    return true;
  });
}

/**
 * §18.6 asks for cloud escalations highlighted. The set is deliberately
 * broader than one action: what an auditor scans for is the privileged
 * few — work leaving the LAN, a policy refusal, a secret read.
 */
export function isNotable(action: string): boolean {
  return (
    action === "cloud.escalated" ||
    action === "shell.denied" ||
    action === "secret.accessed"
  );
}

/** Human label for an action id. */
export function actionLabel(action: string): string {
  const labels: Record<string, string> = {
    "task.dispatched": "dispatched",
    "task.cancelled": "cancelled",
    "plan.resumed": "resumed",
    "shell.allowed": "shell allowed",
    "shell.denied": "shell DENIED",
    "secret.accessed": "secret read",
    "peer.approved": "peer approved",
    "policy.loaded": "policy loaded",
    "cloud.escalated": "CLOUD escalation",
    "audit.truncated": "log truncated",
  };
  return labels[action] ?? action;
}

/**
 * One line of human summary for the verification block.
 *
 * Deliberately never says "verified" unqualified: 5.13a checks the
 * local rows on ONE page plus their anchor, and a page with no local
 * rows proves nothing at all. Saying otherwise would be the same
 * overstatement the ADR refuses.
 */
export function verificationSummary(v: Verification | undefined): {
  tone: "ok" | "warn" | "broken";
  text: string;
} {
  if (!v || !v.checked) {
    return {
      tone: "warn",
      text: "chain not checked on this page (no local entries)",
    };
  }
  if (v.error) {
    return { tone: "warn", text: `chain check failed: ${v.error}` };
  }
  if (v.verified === false) {
    return {
      tone: "broken",
      text: `CHAIN BROKEN at seq ${v.broken_at_seq ?? "?"} — entries were altered or removed`,
    };
  }
  const span =
    v.from_seq !== undefined && v.through_seq !== undefined
      ? ` (seq ${v.from_seq}–${v.through_seq})`
      : "";
  return { tone: "ok", text: `chain verified for this page${span}` };
}

/** `YYYY-MM-DD HH:MM:SS` in UTC — audit rows are cross-node. */
export function fmtTime(ms: number): string {
  return new Date(ms).toISOString().replace("T", " ").slice(0, 19);
}

/** Short node id for display. */
export function shortNode(node: string): string {
  return node.length > 12 ? `${node.slice(0, 12)}…` : node;
}

/** Export the visible rows as JSON (§18.6). */
export function toJson(entries: AuditEntry[]): string {
  return JSON.stringify(entries, null, 2);
}

/**
 * Export the visible rows as CSV (§18.6).
 *
 * Quotes every field and doubles inner quotes: `detail` is JSON and
 * `subject` can be arbitrary, so an unquoted writer would corrupt the
 * file on the first comma — in an AUDIT export, silently.
 */
export function toCsv(entries: AuditEntry[]): string {
  const cell = (v: unknown): string => {
    const s =
      v === null || v === undefined
        ? ""
        : typeof v === "string"
          ? v
          : JSON.stringify(v);
    return `"${s.replace(/"/g, '""')}"`;
  };
  const header = [
    "at_utc",
    "node",
    "seq",
    "action",
    "actor",
    "subject",
    "detail",
    "entry_hash",
  ].join(",");
  const rows = entries.map((e) =>
    [
      cell(fmtTime(e.at_ms)),
      cell(e.node),
      cell(e.seq),
      cell(e.action),
      cell(e.actor),
      cell(e.subject),
      cell(e.detail),
      cell(e.entry_hash),
    ].join(","),
  );
  return [header, ...rows].join("\n");
}
