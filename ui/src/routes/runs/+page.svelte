<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import AuthGate from '$lib/components/AuthGate.svelte';

  type TaskRow = {
    id: string;
    capability: string;
    state: string;
    issued_at_ms: number;
  };

  let authed = $state(false);
  let rows = $state<TaskRow[]>([]);
  let error = $state<string | null>(null);
  let interval: ReturnType<typeof setInterval> | null = null;

  async function load() {
    try {
      const res = await fetch('/api/v1/tasks');
      if (res.status === 401) {
        authed = false;
        return;
      }
      if (!res.ok) throw new Error(`status ${res.status}`);
      rows = await res.json();
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
        <p class="text-sm text-zinc-500">Refreshing every 2s.</p>
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
            {#each rows as row (row.id)}
              <tr>
                <td class="px-4 py-2 font-mono text-xs">
                  {row.id.slice(0, 8)}…{row.id.slice(-4)}
                </td>
                <td class="px-4 py-2">{row.capability}</td>
                <td class="px-4 py-2">
                  <span
                    class="rounded-full px-2 py-0.5 text-xs"
                    class:bg-amber-100={row.state === 'submitted'}
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
