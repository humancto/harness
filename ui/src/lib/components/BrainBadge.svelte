<script lang="ts">
  export let active = false;
  export let score: number;
</script>

{#if active}
  <span
    class="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-900/40 dark:text-amber-200"
    aria-label="brain"
    title="This node is the current brain (planner/dispatcher)"
  >
    <svg viewBox="0 0 24 24" fill="currentColor" class="h-3 w-3">
      <path
        d="M12 2a5 5 0 0 0-5 5v1.05A4.5 4.5 0 0 0 4 12.5a4.5 4.5 0 0 0 1.5 3.36V17a4 4 0 0 0 4 4h1v-9.17l-1.59-1.59 1.42-1.41L12 11.41V9h2v2.41l1.67-1.59 1.42 1.41L15.5 12.83V21h1a4 4 0 0 0 4-4v-1.14a4.5 4.5 0 0 0 1.5-3.36 4.5 4.5 0 0 0-3-4.45V7a5 5 0 0 0-5-5h-2Z"
      />
    </svg>
    brain · {score}
  </span>
{:else}
  <span class="inline-flex items-center rounded-full bg-zinc-100 px-2 py-0.5 text-xs text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400">
    score {score}
  </span>
{/if}
