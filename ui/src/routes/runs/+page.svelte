<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import AuthGate from '$lib/components/AuthGate.svelte';
  import type { TaskSummaryDto } from '$lib/types';

  let authed = $state(false);
  let rows = $state<TaskSummaryDto[]>([]);
  let error = $state<string | null>(null);
  let interval: ReturnType<typeof setInterval> | null = null;

  // Sub-tasks group under their parent when the parent is on the page;
  // orphaned children (parent scrolled off the listing) render flat.
  const ordered = $derived.by(() => {
    const byParent = new Map<string, TaskSummaryDto[]>();
    const ids = new Set(rows.map((r) => r.id));
    const top: TaskSummaryDto[] = [];
    for (const row of rows) {
      if (row.parent && ids.has(row.parent)) {
        const kids = byParent.get(row.parent) ?? [];
        kids.push(row);
        byParent.set(row.parent, kids);
      } else {
        top.push(row);
      }
    }
    const out: { row: TaskSummaryDto; child: boolean }[] = [];
    for (const row of top) {
      out.push({ row, child: false });
      for (const kid of byParent.get(row.id) ?? []) {
        out.push({ row: kid, child: true });
      }
    }
    return out;
  });

  async function load() {
    try {
      const res = await fetch('/api/v1/tasks?limit=100');
      if (res.status === 401) {
        authed = false;
        return;
      }
      if (!res.ok) throw new Error(`status ${res.status}`);
      rows = (await res.json()) as TaskSummaryDto[];
      authed = true;
      error = null;
    } catch (err) {
      error = `${err}`;
    }
  }

  function start() {
    void load();
    interval = setInterval(load, 2000);
  }

  function stop() {
    if (interval) clearInterval(interval);
    interval = null;
  }

  onMount(() => {
    void load();
  });

  onDestroy(stop);

  $effect(() => {
    if (authed) start();
    else stop();
    return stop;
  });

  function fmtAge(ms: number): string {
    const age = Math.max(0, Date.now() - ms) / 1000;
    if (age < 60) return `${age.toFixed(0)}s ago`;
    if (age < 3600) return `${(age / 60).toFixed(0)}m ago`;
    return `${(age / 3600).toFixed(1)}h ago`;
  }

  const LIVE_STATES = new Set(['dispatched', 'claimed', 'running']);
</script>

<svelte:head>
  <title>harness · runs</title>
</svelte:head>

{#if !authed}
  <AuthGate
    onAuthed={() => {
      authed = true;
    }}
  />
{:else}
  <section class="mx-auto max-w-4xl">
    <header class="mb-6 flex items-end justify-between">
      <div>
        <h1 class="text-2xl font-semibold">Runs</h1>
        <p class="text-sm text-zinc-500">All states, newest first. Refreshing every 2s.</p>
      </div>
      <a
        href="/submit"
        class="rounded-md bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white hover:bg-zinc-800 dark:bg-zinc-100 dark:text-zinc-900"
      >
        + new task
      </a>
    </header>

    {#if error}
      <pre class="mb-4 rounded bg-rose-50 p-3 text-xs text-rose-800 dark:bg-rose-950 dark:text-rose-200">{error}</pre>
    {/if}

    {#if rows.length === 0}
      <div class="rounded-lg border border-dashed border-zinc-300 p-8 text-center text-sm text-zinc-500 dark:border-zinc-700">
        <p class="font-medium text-zinc-700 dark:text-zinc-300">No runs yet.</p>
        <p class="mt-1">
          Submit a task on
          <a class="underline" href="/submit">the submit page</a>
          to see it here.
        </p>
      </div>
    {:else}
      <div class="overflow-hidden rounded-xl border border-zinc-200 dark:border-zinc-800">
        <table class="w-full text-sm">
          <thead class="bg-zinc-50 text-left text-xs font-medium uppercase tracking-wide text-zinc-500 dark:bg-zinc-900">
            <tr>
              <th class="px-4 py-2">id</th>
              <th class="px-4 py-2">capability</th>
              <th class="px-4 py-2">state</th>
              <th class="px-4 py-2">issued</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-zinc-100 bg-white dark:divide-zinc-800 dark:bg-zinc-900">
            {#each ordered as { row, child } (row.id)}
              <tr class="hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                <td class="px-4 py-2 font-mono text-xs" class:pl-8={child}>
                  {#if child}
                    <span class="mr-1 text-zinc-400">└</span>
                  {/if}
                  <a class="underline decoration-zinc-300 underline-offset-2" href={`/runs/${row.id}`}>
                    {row.id.slice(0, 8)}…{row.id.slice(-4)}
                  </a>
                </td>
                <td class="px-4 py-2">
                  {row.capability}
                  {#if row.plan_id && !child}
                    <span class="ml-1 rounded bg-indigo-100 px-1.5 py-0.5 text-[10px] text-indigo-700 dark:bg-indigo-950 dark:text-indigo-300">plan</span>
                  {/if}
                </td>
                <td class="px-4 py-2">
                  <span
                    class="rounded-full px-2 py-0.5 text-xs dark:bg-zinc-800"
                    class:bg-amber-100={row.state === 'submitted'}
                    class:bg-sky-100={LIVE_STATES.has(String(row.state))}
                    class:animate-pulse={LIVE_STATES.has(String(row.state))}
                    class:bg-emerald-100={row.state === 'done'}
                    class:bg-rose-100={row.state === 'failed' || row.state === 'expired'}
                    class:bg-zinc-100={row.state === 'cancelled'}
                  >
                    {row.state}
                  </span>
                </td>
                <td class="px-4 py-2 text-xs text-zinc-500">{fmtAge(row.issued_at_ms)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>
{/if}
