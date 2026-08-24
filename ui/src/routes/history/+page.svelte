<script lang="ts">
  import AuthGate from '$lib/components/AuthGate.svelte';
  import { onMount } from 'svelte';
  import {
    actionLabel,
    applyFilters,
    auditQuery,
    fmtTime,
    isNotable,
    maxSeqForNode,
    shortNode,
    toCsv,
    toJson,
    verificationSummary,
    type AuditEntry,
    type AuditFilters,
    type AuditPage,
  } from '$lib/audit';

  const ACTIONS = [
    'task.dispatched',
    'task.cancelled',
    'plan.resumed',
    'shell.allowed',
    'shell.denied',
    'secret.accessed',
    'peer.approved',
    'policy.loaded',
    'cloud.escalated',
    'audit.truncated',
  ];

  let authed = $state(false);
  let page = $state<AuditPage | null>(null);
  let rows = $state<AuditEntry[]>([]);
  let cursors = $state<AuditPage['next_cursor'][]>([]);
  let loadError = $state('');
  let loading = $state(false);

  // §18.6 filters. `action` and `node` narrow the query; `actor` and
  // the time window filter the fetched page (the endpoint indexes the
  // first two, not the others) — the page count reflects that.
  let filters = $state<AuditFilters>({});
  // `datetime-local` gives a local wall-clock string; the log is in
  // unix ms. An unparsable or cleared box means "no bound".
  let sinceText = $state('');
  let untilText = $state('');

  function toMs(text: string): number | undefined {
    if (!text) return undefined;
    const ms = Date.parse(text);
    return Number.isNaN(ms) ? undefined : ms;
  }

  // Changing a filter starts a new query while the old one may still
  // be in flight; the slower response must not overwrite the newer
  // one and leave the table disagreeing with the controls above it
  // (Codex P2 on #65).
  let generation = 0;

  async function load(cursor?: AuditPage['next_cursor']) {
    const mine = ++generation;
    loading = true;
    try {
      const res = await fetch(auditQuery(filters, cursor));
      if (mine !== generation) return;
      if (res.status === 401) {
        authed = false;
        return;
      }
      if (!res.ok) {
        loadError = `fetch failed (${res.status})`;
        return;
      }
      const body = (await res.json()) as AuditPage;
      if (mine !== generation) return;
      page = body;
      rows = applyFilters(page.entries, filters);
      loadError = '';
    } catch {
      if (mine === generation) loadError = 'network error';
    } finally {
      if (mine === generation) loading = false;
    }
  }

  onMount(() => {
    // Only 401 means "log in". A 503 (no store) or a network failure
    // is an error to show, not a password prompt that can never
    // succeed (diff review MINOR-11).
    fetch('/api/v1/audit?limit=1')
      .then((res) => {
        if (res.status === 401) return;
        authed = true;
        if (res.ok) {
          void load();
        } else {
          loadError = `audit unavailable (${res.status})`;
        }
      })
      .catch(() => {
        authed = true;
        loadError = 'network error';
      });
  });

  function applyAndReload() {
    filters.since_ms = toMs(sinceText);
    filters.until_ms = toMs(untilText);
    cursors = [];
    void load();
  }

  function nextPage() {
    const next = page?.next_cursor;
    if (!next) return;
    cursors = [...cursors, next];
    void load(next);
  }

  function firstPage() {
    cursors = [];
    void load();
  }

  // Export what is on screen — the rows the operator is actually
  // looking at, filters included.
  // Named for what it is: the rows on screen, not the whole log
  // (diff review MINOR-11 — an operator who sets a January window and
  // exports gets only the January rows that fell in the fetched page).
  function exportName(kind: string): string {
    if (rows.length === 0) return `harness-audit-page.${kind}`;
    const newest = fmtTime(rows[0].at_ms).slice(0, 10);
    const oldest = fmtTime(rows[rows.length - 1].at_ms).slice(0, 10);
    return `harness-audit-page-${oldest}_${newest}.${kind}`;
  }

  function download(kind: 'json' | 'csv') {
    const body = kind === 'json' ? toJson(rows) : toCsv(rows);
    const blob = new Blob([body], {
      type: kind === 'json' ? 'application/json' : 'text/csv',
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = exportName(kind);
    a.click();
    URL.revokeObjectURL(url);
  }

  let banner = $derived(
    verificationSummary(
      page?.verification,
      maxSeqForNode(page?.entries ?? [], page?.verification?.node),
    ),
  );
  let detailText = (d: unknown) => (d === null || d === undefined ? '' : JSON.stringify(d));
</script>

<svelte:head>
  <title>harness · history</title>
</svelte:head>

{#if !authed}
  <AuthGate
    onAuthed={() => {
      authed = true;
      void load();
    }}
  />
{:else}
  <div class="mx-auto max-w-6xl space-y-4 p-4">
    <div class="flex items-baseline justify-between">
      <h1 class="text-lg font-semibold">History</h1>
      <span class="text-xs text-zinc-500">append-only · hash-chained</span>
    </div>

    <!-- The chain banner. If verification is not visible, the hash
         chain is decoration — so it sits above the table, and it
         never claims more than the server actually checked. -->
    <p
      class="rounded border p-2 text-xs
      {banner.tone === 'ok'
        ? 'border-emerald-300 bg-emerald-50 text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-200'
        : banner.tone === 'broken'
          ? 'border-red-400 bg-red-50 font-semibold text-red-800 dark:border-red-700 dark:bg-red-950 dark:text-red-200'
          : 'border-amber-300 bg-amber-50 text-amber-800 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-200'}"
    >
      {banner.text}
      {#if page?.verification?.node}
        <span class="opacity-70">· node {shortNode(page.verification.node)}</span>
      {/if}
    </p>

    {#if loadError}
      <p class="text-sm text-red-600 dark:text-red-400">{loadError}</p>
    {/if}

    <div class="flex flex-wrap items-end gap-2 text-xs">
      <label class="flex flex-col gap-1">
        <span class="text-zinc-500">action</span>
        <select
          class="rounded border border-zinc-300 bg-white px-2 py-1 dark:border-zinc-600 dark:bg-zinc-900"
          bind:value={filters.action}
          onchange={applyAndReload}
        >
          <option value={undefined}>any</option>
          {#each ACTIONS as action (action)}
            <option value={action}>{actionLabel(action)}</option>
          {/each}
        </select>
      </label>
      <label class="flex flex-col gap-1">
        <span class="text-zinc-500">actor</span>
        <input
          class="w-40 rounded border border-zinc-300 bg-white px-2 py-1 dark:border-zinc-600 dark:bg-zinc-900"
          placeholder="local_admin / webhook:"
          bind:value={filters.actor}
          onchange={applyAndReload}
        />
      </label>
      <label class="flex flex-col gap-1">
        <span class="text-zinc-500">node</span>
        <input
          class="w-40 rounded border border-zinc-300 bg-white px-2 py-1 font-mono dark:border-zinc-600 dark:bg-zinc-900"
          placeholder="hex node id"
          bind:value={filters.node}
          onchange={applyAndReload}
        />
      </label>
      <label class="flex flex-col gap-1">
        <span class="text-zinc-500">from</span>
        <input
          type="datetime-local"
          class="rounded border border-zinc-300 bg-white px-2 py-1 dark:border-zinc-600 dark:bg-zinc-900"
          bind:value={sinceText}
          onchange={applyAndReload}
        />
      </label>
      <label class="flex flex-col gap-1">
        <span class="text-zinc-500">to</span>
        <input
          type="datetime-local"
          class="rounded border border-zinc-300 bg-white px-2 py-1 dark:border-zinc-600 dark:bg-zinc-900"
          bind:value={untilText}
          onchange={applyAndReload}
        />
      </label>
      <div class="ml-auto flex gap-2">
        <button
          class="rounded border border-zinc-300 px-2 py-1 hover:bg-zinc-100 disabled:opacity-40 dark:border-zinc-600 dark:hover:bg-zinc-800"
          disabled={rows.length === 0}
          onclick={() => download('json')}>export page (JSON)</button
        >
        <button
          class="rounded border border-zinc-300 px-2 py-1 hover:bg-zinc-100 disabled:opacity-40 dark:border-zinc-600 dark:hover:bg-zinc-800"
          disabled={rows.length === 0}
          onclick={() => download('csv')}>export page (CSV)</button
        >
      </div>
    </div>

    {#if loading && rows.length === 0}
      <p class="text-sm text-zinc-400">loading…</p>
    {:else if rows.length === 0}
      <p class="text-sm text-zinc-400">
        no entries on this page match — the actor and time filters apply to the fetched
        page, so try <span class="font-medium">older →</span>
      </p>
    {:else}
      <table class="w-full text-left text-xs">
        <thead class="text-zinc-500">
          <tr>
            <th class="py-1 pr-2 font-normal">time (UTC)</th>
            <th class="py-1 pr-2 font-normal">action</th>
            <th class="py-1 pr-2 font-normal">actor</th>
            <th class="py-1 pr-2 font-normal">subject</th>
            <th class="py-1 pr-2 font-normal">detail</th>
            <th class="py-1 font-normal">node · seq</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-zinc-100 dark:divide-zinc-800">
          {#each rows as row (row.node + row.seq)}
            <tr class={isNotable(row.action) ? 'bg-amber-50/60 dark:bg-amber-950/30' : ''}>
              <td class="whitespace-nowrap py-1 pr-2 tabular-nums text-zinc-500"
                >{fmtTime(row.at_ms)}</td
              >
              <td class="py-1 pr-2">
                <span
                  class="rounded px-1.5 py-0.5
                  {row.action === 'cloud.escalated'
                    ? 'bg-sky-100 font-semibold text-sky-800 dark:bg-sky-950 dark:text-sky-300'
                    : row.action === 'shell.denied'
                      ? 'bg-red-100 font-semibold text-red-800 dark:bg-red-950 dark:text-red-300'
                      : row.action === 'secret.accessed'
                        ? 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300'
                        : 'bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300'}"
                  >{actionLabel(row.action)}</span
                >
              </td>
              <td class="py-1 pr-2 font-mono">{row.actor}</td>
              <td class="max-w-40 truncate py-1 pr-2 font-mono" title={row.subject ?? ''}
                >{row.subject ?? ''}</td
              >
              <td
                class="max-w-64 truncate py-1 pr-2 text-zinc-500"
                title={detailText(row.detail)}>{detailText(row.detail)}</td
              >
              <td class="whitespace-nowrap py-1 font-mono text-zinc-400"
                >{shortNode(row.node)} · {row.seq}</td
              >
            </tr>
          {/each}
        </tbody>
      </table>

    {/if}

    <!-- Outside the empty-state branch on purpose (diff review
         MAJOR-6 / Codex P2): the server's cursor is the last row of
         the page, so "older →" is live on the final page and yields
         an empty one — and client-side actor/time filters can empty a
         page whose successors have matches. Hiding the pager there
         strands the operator with no way back. -->
    {#if page}
      <div class="flex items-center gap-2 text-xs">
        <button
          class="rounded border border-zinc-300 px-2 py-1 disabled:opacity-40 dark:border-zinc-600"
          disabled={cursors.length === 0}
          onclick={firstPage}>newest</button
        >
        <button
          class="rounded border border-zinc-300 px-2 py-1 disabled:opacity-40 dark:border-zinc-600"
          disabled={!page.next_cursor}
          onclick={nextPage}>older →</button
        >
        <span class="text-zinc-400">
          {rows.length} shown{#if rows.length !== page.entries.length}
            &nbsp;of {page.entries.length} fetched (actor/time filtered here){/if}
        </span>
      </div>
    {/if}
  </div>
{/if}
