// Pure logic for the Remote Shell page (3.3-ui).
//
// Mirrors crates/harness-cli/src/run.rs: target resolution against the
// live mesh view, the shell.exec submit shape, and `[node-name]` output
// prefixing. Kept free of DOM/network so it is unit-testable.

import type { PeersSnapshot } from "./types";

// ---------------------------------------------------------------------------
// argv parsing
// ---------------------------------------------------------------------------

/**
 * Split a command line into argv on whitespace, with double-quote
 * grouping (`echo "two words"` → `["echo", "two words"]`) and `\"`
 * escapes inside quotes. Deliberately small: no single quotes, no env
 * expansion, no globbing — the command runs verbatim on the target
 * node via shell.exec (cmd + args, no shell).
 *
 * Throws on an unclosed double quote.
 */
export function parseArgv(line: string): string[] {
  const argv: string[] = [];
  let current = "";
  // `started` distinguishes an empty quoted token (`""`) from no token.
  let started = false;
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === "\\" && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
      started = true;
    } else if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      if (started) {
        argv.push(current);
        current = "";
        started = false;
      }
    } else {
      current += ch;
      started = true;
    }
  }

  if (inQuotes) throw new Error("unclosed double quote");
  if (started) argv.push(current);
  return argv;
}

// ---------------------------------------------------------------------------
// node-id helpers
// ---------------------------------------------------------------------------

/**
 * Convert a hex node-id string (32 hex chars = 16 bytes) into the byte
 * array `constraints.pin_to_node` expects on the wire.
 */
export function hexToBytes(hex: string): number[] {
  if (hex.length === 0 || hex.length % 2 !== 0) {
    throw new Error(`invalid hex node id: ${JSON.stringify(hex)}`);
  }
  const bytes: number[] = [];
  for (let i = 0; i < hex.length; i += 2) {
    const byte = Number.parseInt(hex.slice(i, i + 2), 16);
    if (Number.isNaN(byte)) {
      throw new Error(`invalid hex node id: ${JSON.stringify(hex)}`);
    }
    bytes.push(byte);
  }
  return bytes;
}

// ---------------------------------------------------------------------------
// target resolution (mirrors run.rs resolve_targets)
// ---------------------------------------------------------------------------

export type TargetMode = "self" | "all" | "node";

export interface ShellTarget {
  node_id: string;
  /** Display label: manifest hostname when known, else node-id hex. */
  label: string;
  os: string | null;
  last_seen_ms_ago: number;
  is_self: boolean;
}

/** Flatten a peers snapshot into run targets: local node first. */
export function meshTargets(snapshot: PeersSnapshot): ShellTarget[] {
  const toTarget = (
    p: PeersSnapshot["peers"][number],
    is_self: boolean,
  ): ShellTarget => ({
    node_id: p.node_id,
    label: p.node_name && p.node_name.length > 0 ? p.node_name : p.node_id,
    os: p.os ?? null,
    last_seen_ms_ago: p.last_seen_ms_ago,
    is_self,
  });
  return [
    toTarget(snapshot.local, true),
    ...snapshot.peers.map((p) => toTarget(p, false)),
  ];
}

/**
 * Resolve the selected targets. `self` → the local node; `all` → every
 * live node; `node` → the node whose node_id equals `selectedNodeId`.
 * Throws when nothing matches (mesh view empty, stale selection).
 */
export function resolveTargets(
  mode: TargetMode,
  selectedNodeId: string,
  targets: ShellTarget[],
): ShellTarget[] {
  if (targets.length === 0) {
    throw new Error("mesh view is empty (daemon returned no local node)");
  }
  switch (mode) {
    case "all":
      return [...targets];
    case "self": {
      const self = targets.find((t) => t.is_self);
      if (!self) throw new Error("local node missing from the mesh view");
      return [self];
    }
    case "node": {
      const node = targets.find((t) => t.node_id === selectedNodeId);
      if (!node) {
        const known = targets.map((t) => t.label).join(", ");
        throw new Error(`unknown node; known nodes: ${known}`);
      }
      return [node];
    }
  }
}

// ---------------------------------------------------------------------------
// per-node color (deterministic hash → small palette)
// ---------------------------------------------------------------------------

/**
 * Palette of Tailwind classes for `[node-name]` prefixes. Listed as
 * full literals so the Tailwind content scanner keeps them.
 */
export const NODE_COLOR_CLASSES = [
  "text-sky-600 dark:text-sky-400",
  "text-emerald-600 dark:text-emerald-400",
  "text-amber-600 dark:text-amber-400",
  "text-violet-600 dark:text-violet-400",
  "text-cyan-600 dark:text-cyan-400",
  "text-fuchsia-600 dark:text-fuchsia-400",
  "text-lime-600 dark:text-lime-500",
  "text-orange-600 dark:text-orange-400",
] as const;

/** FNV-1a 32-bit — small, stable, good enough to spread 8 palette slots. */
export function fnv1a(s: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < s.length; i += 1) {
    hash ^= s.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

/** Stable per-node color: same label → same class, across runs. */
export function nodeColorClass(label: string): string {
  return NODE_COLOR_CLASSES[fnv1a(label) % NODE_COLOR_CLASSES.length];
}

// ---------------------------------------------------------------------------
// exit summary (mirrors run.rs exit-code semantics)
// ---------------------------------------------------------------------------

export interface NodeRunResult {
  label: string;
  /** Terminal task state ("done", "failed", …) or "error" on transport failure. */
  state: string;
  /** CLI-equivalent exit code: done → exit_code (124 on timeout), else 1. */
  code: number;
  timedOut: boolean;
}

/**
 * "3 nodes · all exit 0" or a listing of the failures, e.g.
 * "3 nodes · 1 exit 0 · pi-c exit 2 · tower-b timed out (124)".
 */
export function summarizeExits(results: NodeRunResult[]): string {
  const n = results.length;
  const noun = n === 1 ? "node" : "nodes";
  const failures = results.filter((r) => r.code !== 0);
  if (failures.length === 0) return `${n} ${noun} · all exit 0`;
  const parts = failures.map((f) => {
    if (f.timedOut) return `${f.label} timed out (124)`;
    if (f.state !== "done") return `${f.label} ${f.state}`;
    return `${f.label} exit ${f.code}`;
  });
  return `${n} ${noun} · ${n - failures.length} exit 0 · ${parts.join(" · ")}`;
}
