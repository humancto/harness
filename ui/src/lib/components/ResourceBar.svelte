<script lang="ts">
  export let label: string;
  /** 0..1 */
  export let value: number | null;
  export let suffix = '';

  $: pct = value === null ? null : Math.max(0, Math.min(1, value)) * 100;
</script>

<div class="flex items-center gap-2 text-xs">
  <span class="w-12 text-zinc-500">{label}</span>
  {#if pct === null}
    <span class="text-zinc-400">—</span>
  {:else}
    <div class="flex-1 h-1.5 rounded-full bg-zinc-200 dark:bg-zinc-800 overflow-hidden">
      <div
        class="h-full"
        class:bg-emerald-500={pct < 70}
        class:bg-amber-500={pct >= 70 && pct < 90}
        class:bg-rose-500={pct >= 90}
        style="width: {pct}%"
      ></div>
    </div>
    <span class="w-10 text-right tabular-nums text-zinc-600 dark:text-zinc-400">
      {pct.toFixed(0)}%{suffix}
    </span>
  {/if}
</div>
