// @vitest-environment node
//
// Pure-function tests — no DOM needed (jsdom is an optional vitest
// peer and intentionally not a dependency).

import { describe, expect, it } from "vitest";
import { layoutDag, NODE_H } from "./dag";

const caps = (ids: string[]) =>
  Object.fromEntries(ids.map((id) => [id, `cap-${id}`]));

describe("layoutDag", () => {
  it("layers a chain and points arrows dependency → dependent", () => {
    // c depends on b, b depends on a (Plan.edges orientation:
    // [from, to] = "from depends on to") — mirrors the Rust lock
    // `plan_edges_express_from_depends_on_to`.
    const l = layoutDag(
      ["a", "b", "c"],
      caps(["a", "b", "c"]),
      [
        ["c", "b"],
        ["b", "a"],
      ],
    );
    expect(l.cycle).toBe(false);
    const layer = Object.fromEntries(l.nodes.map((n) => [n.id, n.layer]));
    expect(layer).toEqual({ a: 0, b: 1, c: 2 });
    // Arrow tail sits on the dependency, head on the dependent — and
    // execution order flows downward (tail above head).
    const ab = l.edges.find((e) => e.from === "a" && e.to === "b");
    expect(ab).toBeDefined();
    expect(ab!.y1).toBeLessThan(ab!.y2);
    expect(l.edges).toHaveLength(2);
  });

  it("lays out a diamond with parallel middle lanes", () => {
    // b and c both depend on a; d depends on both.
    const l = layoutDag(
      ["a", "b", "c", "d"],
      caps(["a", "b", "c", "d"]),
      [
        ["b", "a"],
        ["c", "a"],
        ["d", "b"],
        ["d", "c"],
      ],
    );
    const layer = Object.fromEntries(l.nodes.map((n) => [n.id, n.layer]));
    expect(layer).toEqual({ a: 0, b: 1, c: 1, d: 2 });
    const middle = l.nodes.filter((n) => n.layer === 1);
    expect(new Set(middle.map((n) => n.lane))).toEqual(new Set([0, 1]));
    expect(l.height).toBeGreaterThan(3 * NODE_H);
  });

  it("puts an edgeless fan-out on one layer", () => {
    const ids = ["w", "x", "y", "z"];
    const l = layoutDag(ids, caps(ids), []);
    expect(l.nodes.every((n) => n.layer === 0)).toBe(true);
    expect(new Set(l.nodes.map((n) => n.lane)).size).toBe(4);
  });

  it("handles disconnected components", () => {
    const l = layoutDag(
      ["a", "b", "c"],
      caps(["a", "b", "c"]),
      [["b", "a"]],
    );
    const layer = Object.fromEntries(l.nodes.map((n) => [n.id, n.layer]));
    expect(layer["a"]).toBe(0);
    expect(layer["b"]).toBe(1);
    expect(layer["c"]).toBe(0);
  });

  it("flags a cycle instead of hanging or lying", () => {
    const l = layoutDag(
      ["a", "b"],
      caps(["a", "b"]),
      [
        ["a", "b"],
        ["b", "a"],
      ],
    );
    expect(l.cycle).toBe(true);
    expect(l.nodes).toHaveLength(0);
  });

  it("ignores edges naming unknown nodes", () => {
    const l = layoutDag(["a"], caps(["a"]), [["a", "ghost"]]);
    expect(l.cycle).toBe(false);
    expect(l.nodes[0].layer).toBe(0);
    expect(l.edges).toHaveLength(0);
  });
});
