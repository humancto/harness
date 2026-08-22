// @vitest-environment node
//
// Unit tests for the Remote Shell page logic (3.3-ui). Pure functions —
// no DOM needed, so this file overrides the jsdom default (jsdom is an
// optional vitest peer and intentionally not a dependency).

import { describe, expect, it } from "vitest";

import {
  fnv1a,
  hexToBytes,
  meshTargets,
  NODE_COLOR_CLASSES,
  nodeColorClass,
  parseArgv,
  resolveTargets,
  summarizeExits,
  type ShellTarget,
} from "./shell";
import type { PeersSnapshot } from "./types";

// ---------------------------------------------------------------------------
// parseArgv
// ---------------------------------------------------------------------------

describe("parseArgv", () => {
  it("splits on whitespace", () => {
    expect(parseArgv("uname -a")).toEqual(["uname", "-a"]);
  });

  it("collapses repeated whitespace and trims ends", () => {
    expect(parseArgv("  echo   hi \t there  ")).toEqual(["echo", "hi", "there"]);
  });

  it("returns [] for empty and blank input", () => {
    expect(parseArgv("")).toEqual([]);
    expect(parseArgv("   ")).toEqual([]);
  });

  it("groups double-quoted spans into one arg", () => {
    expect(parseArgv('echo "two words"')).toEqual(["echo", "two words"]);
  });

  it("supports quotes glued to a bare word", () => {
    expect(parseArgv('grep -e"a b" f.txt')).toEqual(["grep", "-ea b", "f.txt"]);
  });

  it("keeps an empty quoted arg", () => {
    expect(parseArgv('printf ""')).toEqual(["printf", ""]);
  });

  it("unescapes \\\" inside quotes", () => {
    expect(parseArgv('echo "say \\"hi\\""')).toEqual(["echo", 'say "hi"']);
  });

  it("throws on an unclosed quote", () => {
    expect(() => parseArgv('echo "oops')).toThrow(/unclosed double quote/);
  });
});

// ---------------------------------------------------------------------------
// hexToBytes
// ---------------------------------------------------------------------------

describe("hexToBytes", () => {
  it("converts a 16-byte node id", () => {
    const hex = "00ff10a0" + "01".repeat(12);
    const bytes = hexToBytes(hex);
    expect(bytes).toHaveLength(16);
    expect(bytes.slice(0, 4)).toEqual([0x00, 0xff, 0x10, 0xa0]);
    expect(bytes[15]).toBe(0x01);
  });

  it("rejects odd-length and empty input", () => {
    expect(() => hexToBytes("abc")).toThrow(/invalid hex/);
    expect(() => hexToBytes("")).toThrow(/invalid hex/);
  });

  it("rejects non-hex characters", () => {
    expect(() => hexToBytes("zz")).toThrow(/invalid hex/);
  });
});

// ---------------------------------------------------------------------------
// target resolution
// ---------------------------------------------------------------------------

function target(node_id: string, label: string, is_self = false): ShellTarget {
  return { node_id, label, os: "linux", last_seen_ms_ago: 0, is_self };
}

const MESH: ShellTarget[] = [
  target("aa".repeat(16), "laptop-a", true),
  target("bb".repeat(16), "tower-b"),
  target("cc".repeat(16), "pi-c"),
];

describe("resolveTargets", () => {
  it("self resolves to the local node only", () => {
    const t = resolveTargets("self", "", MESH);
    expect(t.map((n) => n.label)).toEqual(["laptop-a"]);
  });

  it("all returns every live node", () => {
    expect(resolveTargets("all", "", MESH)).toHaveLength(3);
  });

  it("node resolves by node_id", () => {
    const t = resolveTargets("node", "bb".repeat(16), MESH);
    expect(t.map((n) => n.label)).toEqual(["tower-b"]);
  });

  it("node with a stale selection lists known nodes", () => {
    expect(() => resolveTargets("node", "ff".repeat(16), MESH)).toThrow(
      /known nodes: laptop-a, tower-b, pi-c/,
    );
  });

  it("empty mesh throws for every mode", () => {
    for (const mode of ["self", "all", "node"] as const) {
      expect(() => resolveTargets(mode, "", [])).toThrow(/mesh view is empty/);
    }
  });
});

describe("meshTargets", () => {
  const snapshot = {
    local: {
      node_id: "aa".repeat(16),
      node_name: "laptop-a",
      os: "macos",
      last_seen_ms_ago: 0,
      is_local: true,
    },
    peers: [
      // No node_name → label falls back to node_id hex (CLI parity).
      { node_id: "bb".repeat(16), node_name: null, os: null, last_seen_ms_ago: 1234 },
    ],
    leader_belief: null,
    fetched_at_ms: 0,
  } as unknown as PeersSnapshot;

  it("puts local first and falls back to node_id label", () => {
    const t = meshTargets(snapshot);
    expect(t[0]).toMatchObject({ label: "laptop-a", is_self: true, os: "macos" });
    expect(t[1]).toMatchObject({
      label: "bb".repeat(16),
      is_self: false,
      os: null,
      last_seen_ms_ago: 1234,
    });
  });
});

// ---------------------------------------------------------------------------
// node colors
// ---------------------------------------------------------------------------

describe("nodeColorClass", () => {
  it("is deterministic per label", () => {
    expect(nodeColorClass("tower-b")).toBe(nodeColorClass("tower-b"));
  });

  it("always returns a palette member", () => {
    for (const label of ["laptop-a", "tower-b", "pi-c", "aa".repeat(16), ""]) {
      expect(NODE_COLOR_CLASSES).toContain(nodeColorClass(label));
    }
  });

  it("fnv1a spreads distinct labels (sanity, not proof)", () => {
    const hashes = new Set(["a", "b", "c", "laptop-a", "tower-b"].map(fnv1a));
    expect(hashes.size).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// exit summary
// ---------------------------------------------------------------------------

describe("summarizeExits", () => {
  const ok = (label: string) => ({ label, state: "done", code: 0, timedOut: false });

  it("all-zero fleet", () => {
    expect(summarizeExits([ok("a"), ok("b"), ok("c")])).toBe("3 nodes · all exit 0");
  });

  it("single node uses singular noun", () => {
    expect(summarizeExits([ok("a")])).toBe("1 node · all exit 0");
  });

  it("lists non-zero exits, timeouts, and non-done terminals", () => {
    const out = summarizeExits([
      ok("a"),
      { label: "b", state: "done", code: 2, timedOut: false },
      { label: "c", state: "done", code: 124, timedOut: true },
      { label: "d", state: "failed", code: 1, timedOut: false },
    ]);
    expect(out).toBe("4 nodes · 1 exit 0 · b exit 2 · c timed out (124) · d failed");
  });
});
