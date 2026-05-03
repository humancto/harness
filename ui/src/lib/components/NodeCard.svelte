<script lang="ts">
  import type { PeerDto } from '$lib/types';
  import BrainBadge from './BrainBadge.svelte';
  import HeartbeatPulse from './HeartbeatPulse.svelte';
  import ResourceBar from './ResourceBar.svelte';

  export let peer: PeerDto;
  export let isLocal = false;
  export let isLeader = false;

  $: ramUsedPct =
    peer.resources?.ram_total_bytes && peer.resources?.ram_avail_bytes
      ? 1 - peer.resources.ram_avail_bytes / peer.resources.ram_total_bytes
      : null;
  $: cpuPct = peer.resources?.cpu_load_avg ?? null;
  $: batteryPct =
    peer.resources?.battery_percent != null ? peer.resources.battery_percent / 100 : null;
</script>

<article
  class="rounded-xl border border-zinc-200 bg-white p-4 shadow-sm dark:border-zinc-800 dark:bg-zinc-900 transition-shadow hover:shadow-md"
  class:ring-2={isLeader}
  class:ring-amber-400={isLeader}
>
  <header class="flex items-start justify-between gap-2">
    <div class="min-w-0">
      <div class="flex items-center gap-2">
        {#key peer.seq}
          <HeartbeatPulse lastSeenMsAgo={peer.last_seen_ms_ago} />
        {/key}
        <h3 class="truncate font-mono text-sm font-semibold">
          {peer.node_id.slice(0, 8)}…{peer.node_id.slice(-4)}
        </h3>
        {#if isLocal}
          <span class="rounded-full bg-sky-100 px-1.5 py-0.5 text-xs font-medium text-sky-800 dark:bg-sky-900/40 dark:text-sky-200">
            this node
          </span>
        {/if}
      </div>
      <p class="mt-0.5 truncate font-mono text-xs text-zinc-500" title="public-key fingerprint">
        {peer.pubkey_fp}
      </p>
    </div>
    <BrainBadge active={isLeader} score={peer.brain_score} />
  </header>

  <dl class="mt-3 space-y-1.5">
    <ResourceBar label="cpu" value={cpuPct} />
    <ResourceBar label="ram" value={ramUsedPct} />
    {#if batteryPct !== null}
      <ResourceBar label="batt" value={batteryPct} />
    {/if}
  </dl>

  {#if peer.capabilities_summary.length > 0}
    <footer class="mt-3 flex flex-wrap gap-1">
      {#each peer.capabilities_summary.slice(0, 6) as cap}
        <span class="rounded bg-zinc-100 px-1.5 py-0.5 font-mono text-[10px] text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400">
          {cap}
        </span>
      {/each}
      {#if peer.capabilities_summary.length > 6}
        <span class="text-[10px] text-zinc-400">
          +{peer.capabilities_summary.length - 6}
        </span>
      {/if}
    </footer>
  {/if}

  <p class="mt-2 text-[10px] text-zinc-400">
    last seen {(peer.last_seen_ms_ago / 1000).toFixed(1)}s ago · seq {peer.seq}
  </p>
</article>
