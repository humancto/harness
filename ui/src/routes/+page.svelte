<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import NodeCard from '$lib/components/NodeCard.svelte';
  import { peersStore, allNodes, leaderId, startPeersStore, stopPeersStore } from '$lib/stores/peers';

  onMount(() => {
    startPeersStore();
  });
  onDestroy(() => {
    stopPeersStore();
  });
</script>

<svelte:head>
  <title>harness · mesh</title>
</svelte:head>

<section>
  <div class="mb-6 flex items-end justify-between">
    <div>
      <h1 class="text-2xl font-semibold">Mesh</h1>
      <p class="text-sm text-zinc-500">Live topology — read-only.</p>
    </div>
    <div class="flex items-center gap-3 text-xs text-zinc-500">
      {#if $peersStore.polling}
        <span class="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-amber-800 dark:bg-amber-900/40 dark:text-amber-200">
          <span class="h-1.5 w-1.5 rounded-full bg-amber-500"></span>
          polling (websocket reconnecting)
        </span>
      {:else if $peersStore.snapshot}
        <span class="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-2 py-0.5 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-200">
          <span class="h-1.5 w-1.5 rounded-full bg-emerald-500"></span>
          live
        </span>
      {/if}
    </div>
  </div>

  {#if $peersStore.error}
    <div class="rounded-lg border border-rose-200 bg-rose-50 p-4 text-sm text-rose-800 dark:border-rose-900 dark:bg-rose-950 dark:text-rose-200">
      <strong>error:</strong>
      {$peersStore.error}
    </div>
  {:else if $peersStore.loading && !$peersStore.hasData}
    <div class="rounded-lg border border-dashed border-zinc-300 p-8 text-center text-sm text-zinc-500 dark:border-zinc-700">
      connecting…
    </div>
  {:else if $allNodes.length === 0}
    <div class="rounded-lg border border-dashed border-zinc-300 p-8 text-center text-sm text-zinc-500 dark:border-zinc-700">
      <p class="font-medium text-zinc-700 dark:text-zinc-300">No peers yet.</p>
      <p class="mt-1">
        Run
        <code class="rounded bg-zinc-100 px-1.5 py-0.5 font-mono text-xs dark:bg-zinc-800">
          harness join &lt;pairing-code&gt;
        </code>
        on another laptop on the same LAN.
      </p>
    </div>
  {:else}
    <div class="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
      {#each $allNodes as peer (peer.node_id)}
        <NodeCard
          {peer}
          isLocal={$peersStore.snapshot?.local.node_id === peer.node_id}
          isLeader={$leaderId === peer.node_id}
        />
      {/each}
    </div>
  {/if}
</section>
