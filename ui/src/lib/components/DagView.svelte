<script lang="ts">
  import { layoutDag, NODE_W, NODE_H, type DagLayout } from '$lib/dag';
  import type { StepView } from '$lib/progress';
  import type { PlanInput } from '$lib/types';

  let { plan, steps }: { plan: PlanInput; steps: Record<string, StepView> } = $props();

  const layout: DagLayout = $derived(
    layoutDag(
      Object.keys(plan.tasks),
      Object.fromEntries(Object.entries(plan.tasks).map(([id, n]) => [id, n.capability])),
      plan.edges,
    ),
  );

  function stateOf(id: string): string {
    return steps[id]?.state ?? 'pending';
  }

  // Node fill by live state. `in_flight` frames arrive at dispatch
  // (4.8); anything unseen is pending.
  const FILL: Record<string, string> = {
    pending: 'fill-zinc-100 dark:fill-zinc-800',
    in_flight: 'fill-sky-100 dark:fill-sky-900',
    done: 'fill-emerald-100 dark:fill-emerald-900',
    failed: 'fill-rose-100 dark:fill-rose-900',
    timed_out: 'fill-amber-100 dark:fill-amber-900',
    skipped: 'fill-zinc-200 dark:fill-zinc-700',
  };

  function shortCap(cap: string): string {
    return cap.length > 18 ? `${cap.slice(0, 17)}…` : cap;
  }
</script>

{#if layout.cycle}
  <p class="text-xs text-rose-600 dark:text-rose-400">
    plan graph contains a cycle — cannot render (the executor rejects it too)
  </p>
{:else}
  <div class="overflow-x-auto">
    <svg
      width={layout.width}
      height={layout.height}
      viewBox={`0 0 ${layout.width} ${layout.height}`}
      role="img"
      aria-label="plan DAG"
      class="min-w-full"
    >
      <defs>
        <marker
          id="dag-arrow"
          viewBox="0 0 8 8"
          refX="7"
          refY="4"
          markerWidth="6"
          markerHeight="6"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 8 4 L 0 8 z" class="fill-zinc-400 dark:fill-zinc-500" />
        </marker>
      </defs>
      {#each layout.edges as edge (edge.from + edge.to)}
        <line
          x1={edge.x1}
          y1={edge.y1}
          x2={edge.x2}
          y2={edge.y2}
          marker-end="url(#dag-arrow)"
          class="stroke-zinc-400 dark:stroke-zinc-500"
          stroke-width="1.5"
        />
      {/each}
      {#each layout.nodes as node (node.id)}
        {@const state = stateOf(node.id)}
        {@const taskId = steps[node.id]?.taskId}
        <g>
          <rect
            x={node.x}
            y={node.y}
            width={NODE_W}
            height={NODE_H}
            rx="8"
            class={`${FILL[state] ?? FILL.pending} stroke-zinc-300 dark:stroke-zinc-600`}
            stroke-width="1"
          >
            {#if state === 'in_flight'}
              <animate
                attributeName="opacity"
                values="1;0.55;1"
                dur="1.6s"
                repeatCount="indefinite"
              />
            {/if}
          </rect>
          {#if taskId}
            <a href={`/runs/${taskId}`}>
              <text
                x={node.x + NODE_W / 2}
                y={node.y + 18}
                text-anchor="middle"
                class="fill-zinc-800 text-[11px] underline dark:fill-zinc-200"
              >
                {shortCap(node.capability)}
              </text>
            </a>
          {:else}
            <text
              x={node.x + NODE_W / 2}
              y={node.y + 18}
              text-anchor="middle"
              class="fill-zinc-800 text-[11px] dark:fill-zinc-200"
            >
              {shortCap(node.capability)}
            </text>
          {/if}
          <text
            x={node.x + NODE_W / 2}
            y={node.y + 34}
            text-anchor="middle"
            class="fill-zinc-500 text-[10px] dark:fill-zinc-400"
          >
            {state}
          </text>
        </g>
      {/each}
    </svg>
  </div>
{/if}
