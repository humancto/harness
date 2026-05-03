<script lang="ts">
  // Keyed by `seq` from the parent so each new heartbeat creates a fresh
  // DOM node, preventing CSS animation leaks.
  export let lastSeenMsAgo: number;
  $: live = lastSeenMsAgo < 4000;
  $: stale = lastSeenMsAgo >= 6000;
</script>

<span class="relative inline-flex h-3 w-3" aria-label={live ? 'live' : stale ? 'stale' : 'fading'}>
  {#if live}
    <span class="absolute inline-flex h-full w-full animate-beat rounded-full bg-emerald-400 opacity-75"></span>
  {/if}
  <span
    class="relative inline-flex h-3 w-3 rounded-full"
    class:bg-emerald-500={live}
    class:bg-amber-500={!live && !stale}
    class:bg-rose-500={stale}
  ></span>
</span>
