// @vitest-environment node
import { describe, expect, it } from "vitest";
import {
  applyFilters,
  auditQuery,
  isNotable,
  maxSeqForNode,
  toCsv,
  toJson,
  verificationSummary,
  type AuditEntry,
} from "./audit";

const entry = (over: Partial<AuditEntry> = {}): AuditEntry => ({
  node: "aabbccddeeff00112233445566778899",
  seq: 1,
  at_ms: 1_000,
  action: "task.dispatched",
  subject: "task-1",
  detail: { capability: "echo" },
  actor: "system",
  entry_hash: "ff".repeat(32),
  ...over,
});

describe("auditQuery", () => {
  it("passes the whole cursor, not just the timestamp", () => {
    // A bare before_ms skips every row sharing the boundary
    // millisecond — the bug 5.13a fixed server-side.
    const q = auditQuery(
      { action: "shell.denied" },
      {
        before_ms: 42,
        before_node: "node-hex",
        before_seq: 7,
      },
    );
    expect(q).toContain("action=shell.denied");
    expect(q).toContain("before_ms=42");
    expect(q).toContain("before_node=node-hex");
    expect(q).toContain("before_seq=7");
  });

  it("omits absent filters", () => {
    const q = auditQuery({});
    expect(q).toBe("/api/v1/audit?limit=100");
  });
});

describe("applyFilters", () => {
  const rows = [
    entry({ at_ms: 100, actor: "local_admin" }),
    entry({ at_ms: 200, actor: "webhook:sms" }),
    entry({ at_ms: 300, actor: "peer:abc" }),
  ];

  it("filters by actor prefix and a half-open time window", () => {
    expect(applyFilters(rows, { actor: "webhook" })).toHaveLength(1);
    // since is inclusive, until is exclusive.
    const windowed = applyFilters(rows, { since_ms: 200, until_ms: 300 });
    expect(windowed.map((r) => r.at_ms)).toEqual([200]);
  });

  it("returns everything when nothing is set", () => {
    expect(applyFilters(rows, {})).toHaveLength(3);
  });
});

describe("isNotable", () => {
  it("marks the privileged few an auditor scans for", () => {
    expect(isNotable("cloud.escalated")).toBe(true);
    expect(isNotable("shell.denied")).toBe(true);
    expect(isNotable("secret.accessed")).toBe(true);
    expect(isNotable("task.dispatched")).toBe(false);
  });
});

describe("verificationSummary", () => {
  it("never claims verification it did not do", () => {
    // A page with no local rows proves nothing — saying "verified"
    // there is the overstatement ADR-0041 refuses.
    expect(
      verificationSummary({ scope: "none", node: "n", checked: false }).tone,
    ).toBe("warn");
    expect(verificationSummary(undefined).tone).toBe("warn");
  });

  it("names the broken seq", () => {
    const s = verificationSummary({
      scope: "page",
      node: "n",
      checked: true,
      verified: false,
      broken_at_seq: 12,
    });
    expect(s.tone).toBe("broken");
    expect(s.text).toContain("12");
  });

  it("reports the span it actually covered", () => {
    const s = verificationSummary({
      scope: "page",
      node: "n",
      checked: true,
      verified: true,
      from_seq: 4,
      through_seq: 9,
    });
    expect(s.tone).toBe("ok");
    expect(s.text).toContain("4–9");
  });
});

describe("export", () => {
  it("quotes CSV fields so a comma cannot corrupt the file", () => {
    const rows = [
      entry({
        subject: "rm -rf /tmp, really",
        detail: { reason: 'he said "no"' },
      }),
    ];
    const csv = toCsv(rows);
    const [header, row] = csv.split("\n");
    expect(header.startsWith("at_utc,node,seq")).toBe(true);
    expect(row).toContain('"rm -rf /tmp, really"');
    // Inner quotes are doubled, not dropped (the detail arrives as
    // JSON, so its own escaping survives underneath).
    expect(row).toContain('\\""no\\""');
    // One row in, one row out — the comma did not split it.
    expect(csv.split("\n")).toHaveLength(2);
  });

  it("round-trips JSON", () => {
    const rows = [entry()];
    expect(JSON.parse(toJson(rows))).toEqual(rows);
  });
});

describe("csv injection", () => {
  it("neutralizes a formula a remote peer got into the log", () => {
    // A denial's `subject` IS the command the peer submitted, so a
    // paired peer can plant a formula by having it DENIED. Quoting
    // alone does not help — spreadsheets evaluate the unquoted cell
    // content (diff review MAJOR-5 on #65).
    const csv = toCsv([
      entry({ action: "shell.denied", subject: "=cmd|'/c calc'!A1" }),
    ]);
    const row = csv.split("\n")[1];
    expect(row).toContain("\"'=cmd");
    expect(row).not.toContain('"=cmd');
  });

  it("neutralizes every formula trigger, not just equals", () => {
    for (const lead of ["=", "+", "-", "@", "\t", "\r"]) {
      const csv = toCsv([entry({ subject: `${lead}danger` })]);
      expect(csv.split("\n")[1]).toContain(`"'${lead}danger"`);
    }
  });

  it("leaves an ordinary subject alone", () => {
    const csv = toCsv([entry({ subject: "task-42" })]);
    expect(csv.split("\n")[1]).toContain('"task-42"');
    expect(csv).not.toContain("'task-42");
  });
});

describe("verification coverage", () => {
  const NODE = "aabbccddeeff00112233445566778899";

  it("warns when the walk stopped below the newest row on the page", () => {
    // The server walks a bounded number of ROWS up from the lowest
    // local seq. With sparse local rows the walk can end below what
    // is on screen; painting those green would be the overstatement
    // the ADR refuses (diff review MAJOR-7 on #65).
    const entries = [
      entry({ node: NODE, seq: 900 }),
      entry({ node: NODE, seq: 500 }),
    ];
    const s = verificationSummary(
      {
        scope: "page",
        node: NODE,
        checked: true,
        verified: true,
        from_seq: 500,
        through_seq: 599,
      },
      maxSeqForNode(entries, NODE),
    );
    expect(s.tone).toBe("warn");
    expect(s.text).toContain("NOT checked");
  });

  it("stays green when the walk covers the page", () => {
    const entries = [entry({ node: NODE, seq: 550 })];
    const s = verificationSummary(
      {
        scope: "page",
        node: NODE,
        checked: true,
        verified: true,
        from_seq: 500,
        through_seq: 599,
      },
      maxSeqForNode(entries, NODE),
    );
    expect(s.tone).toBe("ok");
  });

  it("ignores other nodes' rows when measuring coverage", () => {
    const entries = [
      entry({ node: "ff".repeat(16), seq: 99_999 }),
      entry({ node: NODE, seq: 510 }),
    ];
    expect(maxSeqForNode(entries, NODE)).toBe(510);
    expect(maxSeqForNode(entries, undefined)).toBeUndefined();
  });
});
