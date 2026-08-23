<script lang="ts">
  import AuthGate from '$lib/components/AuthGate.svelte';
  import { onMount } from 'svelte';
  import {
    activePlans,
    fillDays,
    fmtUsd,
    runRate,
    type ActivePlan,
    type CostTotals,
    type TaskRowLite,
  } from '$lib/costs';

  let authed = $state(false);
  let totals = $state<CostTotals | null>(null);
  let tasks = $state<TaskRowLite[]>([]);
  let loadError = $state('');
  let stopPending = $state<string | null>(null);
  let confirmStop = $state<ActivePlan | null>(null);

  // Poll-loop hygiene (5.4 lesson; plan review m9): one interval,
  // a generation guard, and a per-cycle AbortController whose owner
  // is the $effect teardown — navigating away aborts in-flight
  // fetches and stops the loop.
  let gen = 0;
  let controller: AbortController | null = null;

  async function refresh(myGen: number) {
    if (document.visibilityState === 'hidden') return;
    controller?.abort();
    const ctrl = new AbortController();
    controller = ctrl;
    try {
      const [costsRes, tasksRes] = await Promise.all([
        fetch('/api/v1/costs', { signal: ctrl.signal }),
        fetch('/api/v1/tasks', { signal: ctrl.signal }),
      ]);
      if (gen !== myGen) return;
      if (costsRes.status === 401 || tasksRes.status === 401) {
        authed = false;
        return;
      }
      if (!costsRes.ok || !tasksRes.ok) {
        loadError = `fetch failed (${costsRes.status}/${tasksRes.status})`;
        return;
      }
      totals = (await costsRes.json()) as CostTotals;
      tasks = (await tasksRes.json()) as TaskRowLite[];
      loadError = '';
    } catch (e) {
      if (gen === myGen && !(e instanceof DOMException && e.name === 'AbortError')) {
        loadError = 'network error';
      }
    }
  }

  onMount(() => {
    fetch('/api/v1/tasks').then((res) => {
      if (res.ok) authed = true;
    });
  });

  $effect(() => {
    if (!authed) return;
    gen += 1;
    const myGen = gen;
    void refresh(myGen);
    const timer = setInterval(() => void refresh(myGen), 5000);
    return () => {
      gen += 1;
      clearInterval(timer);
      controller?.abort();
    };
  });

  async function stop(plan: ActivePlan) {
    confirmStop = null;
    stopPending = plan.task_id;
    try {
      const res = await fetch(`/api/v1/tasks/${plan.task_id}/cancel`, {
        method: 'POST',
      });
      if (res.status === 401) {
        authed = false;
        return;
      }
      // 409 = it finished (or was cancelled) while we looked — a
      // refetch shows the truth; no error state (plan review m8).
      await refresh(gen);
    } finally {
      stopPending = null;
    }
  }

  const nowMs = () => Date.now();
  let days = $derived(totals ? fillDays(totals.per_day, totals.window_days, nowMs()) : []);
  let projected = $derived(
    totals ? runRate(totals.per_day, totals.window_days, nowMs()) : null,
  );
  let active = $derived(totals ? activePlans(tasks, totals.per_plan) : []);
  let maxDay = $derived(days.reduce((m, d) => Math.max(m, d.usd), 0));

  // Chart geometry: thin bars, 2px gaps, rounded data ends (dataviz
  // mark spec); recessive baseline; values live in tooltips + the
  // tables, not painted on every bar.
  const CHART_H = 96;
  const BAR_GAP = 2;
</script>

<svelte:head>
  <title>harness · costs</title>
</svelte:head>

{#if !authed}
  <AuthGate
    onAuthed={() => {
      authed = true;
    }}
  />
{:else}
  <div class="mx-auto max-w-4xl space-y-6 p-4">
    <div class="flex items-baseline justify-between">
      <h1 class="text-lg font-semibold">Costs</h1>
      {#if totals}
        <span class="text-xs text-zinc-500">last {totals.window_days} days · local view</span>
      {/if}
    </div>

    {#if loadError}
      <p class="text-sm text-red-600 dark:text-red-400">{loadError}</p>
    {/if}

    {#if totals}
      {#if totals.truncated}
        <p
          class="rounded border border-amber-300 bg-amber-50 p-2 text-xs text-amber-800 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-200"
        >
          list truncated — totals may undercount
        </p>
      {/if}

      <div class="grid grid-cols-3 gap-3">
        <div class="rounded border border-zinc-200 p-3 dark:border-zinc-700">
          <div class="text-xs text-zinc-500">total ({totals.window_days}d)</div>
          <div class="text-xl font-semibold tabular-nums">{fmtUsd(totals.total_usd)}</div>
        </div>
        <div class="rounded border border-zinc-200 p-3 dark:border-zinc-700">
          <div class="text-xs text-zinc-500">today</div>
          <div class="text-xl font-semibold tabular-nums">{fmtUsd(totals.today_usd)}</div>
        </div>
        <div class="rounded border border-zinc-200 p-3 dark:border-zinc-700">
          <div class="text-xs text-zinc-500">projected 30-day run rate</div>
          <div class="text-xl font-semibold tabular-nums">
            {#if projected === null}<span class="text-sm text-zinc-400">insufficient data</span
              >{:else}{fmtUsd(projected)}{/if}
          </div>
        </div>
      </div>

      <section>
        <h2 class="mb-1 text-sm font-medium text-zinc-600 dark:text-zinc-300">
          daily spend (UTC days)
        </h2>
        {#if maxDay === 0}
          <p class="text-sm text-zinc-400">no spend in the window</p>
        {:else}
          <svg
            role="img"
            aria-label="daily spend bars"
            class="w-full"
            viewBox={`0 0 ${days.length * 12} ${CHART_H + 4}`}
            preserveAspectRatio="none"
            height={CHART_H + 4}
          >
            <line
              x1="0"
              y1={CHART_H + 1}
              x2={days.length * 12}
              y2={CHART_H + 1}
              class="stroke-zinc-200 dark:stroke-zinc-700"
              stroke-width="1"
            />
            {#each days as d, i (d.day)}
              {#if d.usd > 0}
                <rect
                  x={i * 12 + BAR_GAP / 2}
                  y={CHART_H - Math.max(3, (d.usd / maxDay) * (CHART_H - 4))}
                  width={12 - BAR_GAP}
                  height={Math.max(3, (d.usd / maxDay) * (CHART_H - 4))}
                  rx="2"
                  class="fill-sky-600 hover:fill-sky-500 dark:fill-sky-400"
                >
                  <title>{d.day}: {fmtUsd(d.usd)}</title>
                </rect>
              {:else}
                <rect
                  x={i * 12 + BAR_GAP / 2}
                  y={CHART_H - 1}
                  width={12 - BAR_GAP}
                  height="1"
                  class="fill-zinc-200 dark:fill-zinc-700"
                >
                  <title>{d.day}: $0</title>
                </rect>
              {/if}
            {/each}
          </svg>
        {/if}
      </section>

      {#if active.length > 0}
        <section>
          <h2 class="mb-1 text-sm font-medium text-zinc-600 dark:text-zinc-300">active plans</h2>
          <ul class="divide-y divide-zinc-100 rounded border border-zinc-200 dark:divide-zinc-800 dark:border-zinc-700">
            {#each active as plan (plan.task_id)}
              <li class="flex items-center justify-between gap-2 p-2 text-sm">
                <span class="truncate font-mono text-xs">{plan.name ?? plan.plan_id ?? plan.task_id}</span>
                <span class="tabular-nums text-zinc-500">{fmtUsd(plan.actual_usd)}</span>
                <button
                  class="rounded bg-red-600 px-3 py-1 text-xs font-semibold text-white hover:bg-red-500 disabled:opacity-50"
                  disabled={stopPending === plan.task_id}
                  onclick={() => (confirmStop = plan)}
                >
                  {stopPending === plan.task_id ? 'stopping…' : 'Stop'}
                </button>
              </li>
            {/each}
          </ul>
        </section>
      {/if}

      <section>
        <h2 class="mb-1 text-sm font-medium text-zinc-600 dark:text-zinc-300">plans</h2>
        {#if totals.per_plan.length === 0}
          <p class="text-sm text-zinc-400">no plan activity in the window</p>
        {:else}
          <table class="w-full text-left text-sm">
            <thead class="text-xs text-zinc-500">
              <tr>
                <th class="py-1 pr-2 font-normal">plan</th>
                <th class="py-1 pr-2 font-normal">actual</th>
                <th class="py-1 pr-2 font-normal">reported</th>
                <th class="py-1 pr-2 font-normal">cap</th>
                <th class="py-1 font-normal">status</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-zinc-100 dark:divide-zinc-800">
              {#each totals.per_plan as p (p.plan_id)}
                <tr>
                  <td class="max-w-40 truncate py-1 pr-2 font-mono text-xs"
                    >{p.name ?? p.plan_id}</td
                  >
                  <td class="py-1 pr-2 tabular-nums">{fmtUsd(p.actual_usd)}</td>
                  <td class="py-1 pr-2 tabular-nums text-zinc-500"
                    >{fmtUsd(p.reported_spent_usd)}</td
                  >
                  <td class="py-1 pr-2 tabular-nums text-zinc-500">{fmtUsd(p.cap_usd)}</td>
                  <td class="py-1">
                    <span
                      class="rounded px-1.5 py-0.5 text-xs
                      {p.status === 'aborted_budget'
                        ? 'bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300'
                        : p.status === 'paused_budget'
                          ? 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300'
                          : p.status === 'cancelled'
                            ? 'bg-zinc-200 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300'
                            : 'bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300'}"
                      >{p.status ?? 'running'}</span
                    >
                    {#if p.triggered}<span class="ml-1 text-xs text-amber-600" title="budget cap tripped"
                        >▲ budget</span
                      >{/if}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        {/if}
      </section>

      {#if totals.per_issuer.length > 1}
        <section>
          <h2 class="mb-1 text-sm font-medium text-zinc-600 dark:text-zinc-300">by issuer</h2>
          <ul class="space-y-0.5 text-sm">
            {#each totals.per_issuer as issuer (issuer.node_id)}
              <li class="flex justify-between">
                <span class="truncate font-mono text-xs">{issuer.node_id}</span>
                <span class="tabular-nums">{fmtUsd(issuer.usd)}</span>
              </li>
            {/each}
          </ul>
        </section>
      {/if}
    {:else if !loadError}
      <p class="text-sm text-zinc-400">loading…</p>
    {/if}
  </div>

  {#if confirmStop}
    <div class="fixed inset-0 flex items-center justify-center bg-black/40 p-4">
      <div class="w-full max-w-sm rounded bg-white p-4 shadow-lg dark:bg-zinc-900">
        <h3 class="mb-2 text-sm font-semibold">Stop this plan?</h3>
        <p class="mb-4 text-xs text-zinc-500">
          Stops new step dispatch at the next completion. Steps already running finish on their
          own timeouts.
        </p>
        <div class="flex justify-end gap-2">
          <button
            class="rounded border border-zinc-300 px-3 py-1 text-xs dark:border-zinc-600"
            onclick={() => (confirmStop = null)}>keep running</button
          >
          <button
            class="rounded bg-red-600 px-3 py-1 text-xs font-semibold text-white hover:bg-red-500"
            onclick={() => confirmStop && stop(confirmStop)}>Stop plan</button
          >
        </div>
      </div>
    </div>
  {/if}
{/if}
