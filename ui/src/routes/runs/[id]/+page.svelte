<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { page } from '$app/stores';
  import AuthGate from '$lib/components/AuthGate.svelte';
  import DagView from '$lib/components/DagView.svelte';
  import { applyProgressLine, emptyProgress, type RunProgress } from '$lib/progress';
  import type {
    PartialFrame,
    PlanInput,
    RunStreamFrame,
    TaskDetailDto,
  } from '$lib/types';

  const taskId = $derived($page.params.id);

  let authed = $state(false);
  let detail = $state<TaskDetailDto | null>(null);
  let progress = $state<RunProgress>(emptyProgress());
  let logs = $state<PartialFrame[]>([]);
  let liveState = $state<string | null>(null);
  let output = $state<unknown>(undefined);
  let errorText = $state<string | null>(null);
  let pageError = $state<string | null>(null);
  let liveNote = $state<string | null>(null);

  let ws: WebSocket | null = null;
  let wsRetried = false;
  let pollTimer: ReturnType<typeof setInterval> | null = null;
  let seenSeq = new Set<number>();
  let logPane = $state<HTMLElement | null>(null);

  const TERMINAL = new Set(['done', 'failed', 'expired', 'cancelled']);

  const plan = $derived.by((): PlanInput | null => {
    const input = detail?.input as { plan?: PlanInput } | undefined;
    return input?.plan && typeof input.plan.tasks === 'object' ? input.plan : null;
  });
  const planSize = $derived(plan ? Object.keys(plan.tasks).length : 0);

  function ingestFrames(frames: PartialFrame[]) {
    let next = progress;
    for (const frame of frames) {
      if (seenSeq.has(frame.seq)) continue;
      seenSeq.add(frame.seq);
      if (frame.stream === 'progress') {
        next = applyProgressLine(next, frame.line, planSize);
      } else {
        logs.push(frame);
      }
    }
    // Reassign for runes reactivity (the reducer mutates in place).
    progress = next;
    logs = logs;
    queueMicrotask(() => {
      logPane?.scrollTo({ top: logPane.scrollHeight });
    });
  }

  async function fetchDetail(): Promise<boolean> {
    const res = await fetch(`/api/v1/tasks/${taskId}`);
    if (res.status === 401) {
      authed = false;
      return false;
    }
    // Any non-401 response proves the session works — render errors
    // (404 incl.) in the page, never bounce back to the login form.
    authed = true;
    if (!res.ok) {
      pageError = res.status === 404 ? 'task not found' : `fetch failed (${res.status})`;
      return false;
    }
    const d = (await res.json()) as TaskDetailDto;
    detail = d;
    liveState = String(d.state);
    if (d.output !== undefined) output = d.output;
    if (d.error !== undefined) errorText = d.error;
    if (d.partials) ingestFrames(d.partials);
    pageError = null;
    return true;
  }

  function startPolling() {
    if (pollTimer) return;
    pollTimer = setInterval(async () => {
      const ok = await fetchDetail();
      if (!ok || (liveState && TERMINAL.has(liveState))) stopPolling();
    }, 2000);
  }

  function stopPolling() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = null;
  }

  function connectWs() {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${proto}//${window.location.host}/api/v1/runs/${taskId}`);
    ws.onmessage = (msg) => {
      try {
        const frame = JSON.parse(msg.data) as RunStreamFrame;
        if ('partials' in frame) {
          ingestFrames(frame.partials);
          return;
        }
        liveState = frame.state;
        if (frame.output !== undefined) output = frame.output;
        if (frame.error !== undefined) errorText = frame.error;
        if (TERMINAL.has(frame.state)) {
          // Final snapshot: provenance and partials_dropped ride only
          // GET /tasks/:id — the terminal frame carries neither.
          void fetchDetail();
        }
      } catch {
        // Malformed frame — telemetry only; the poll fallback covers us.
      }
    };
    ws.onclose = (ev) => {
      ws = null;
      // 1000 = server pushed the terminal frame: never reconnect.
      if (ev.code === 1000 || (liveState && TERMINAL.has(liveState))) return;
      // 1011 = row-vanished/store-error; anything else (auth/origin
      // refusal, network) — one reconnect, then honest polling.
      if (!wsRetried) {
        wsRetried = true;
        setTimeout(connectWs, 500);
      } else {
        liveNote = 'live stream unavailable — polling every 2s';
        startPolling();
      }
    };
    ws.onerror = () => {
      // onclose follows; handled there.
    };
  }

  async function boot() {
    seenSeq = new Set();
    logs = [];
    progress = emptyProgress();
    const ok = await fetchDetail();
    if (!ok) return;
    if (liveState && TERMINAL.has(liveState)) return;
    connectWs();
  }

  onMount(() => {
    void boot();
  });

  onDestroy(() => {
    ws?.close(1000, 'leaving page');
    ws = null;
    stopPolling();
  });

  function fmtJson(v: unknown): string {
    try {
      return JSON.stringify(v, null, 2);
    } catch {
      return String(v);
    }
  }
</script>

<svelte:head>
  <title>harness · run {taskId?.slice(0, 8)}</title>
</svelte:head>

{#if !authed}
  <AuthGate
    onAuthed={() => {
      authed = true;
      void boot();
    }}
  />
{:else}
  <section class="mx-auto max-w-4xl space-y-6">
    <header class="flex items-end justify-between">
      <div>
        <a href="/runs" class="text-xs text-zinc-500 underline">← runs</a>
        <h1 class="mt-1 text-xl font-semibold">
          {detail?.capability ?? '…'}
          <span class="ml-2 font-mono text-xs text-zinc-500">{taskId}</span>
        </h1>
      </div>
      {#if liveState}
        <span
          class="rounded-full px-3 py-1 text-sm dark:bg-zinc-800"
          class:bg-amber-100={liveState === 'submitted'}
          class:bg-sky-100={!TERMINAL.has(liveState) && liveState !== 'submitted'}
          class:animate-pulse={!TERMINAL.has(liveState)}
          class:bg-emerald-100={liveState === 'done'}
          class:bg-rose-100={liveState === 'failed' || liveState === 'expired'}
          class:bg-zinc-100={liveState === 'cancelled'}
        >
          {liveState}
        </span>
      {/if}
    </header>

    {#if pageError}
      <pre class="rounded bg-rose-50 p-3 text-xs text-rose-800 dark:bg-rose-950 dark:text-rose-200">{pageError}</pre>
    {/if}
    {#if liveNote}
      <p class="text-xs text-amber-600 dark:text-amber-400">{liveNote}</p>
    {/if}

    <!-- Live progress bar (4.8): fraction when derivable, indeterminate
         sweep while only a state is known. -->
    {#if liveState && !TERMINAL.has(liveState)}
      <div class="h-2 overflow-hidden rounded-full bg-zinc-200 dark:bg-zinc-800">
        {#if progress.fraction !== null}
          <div
            class="h-full rounded-full bg-sky-500 transition-all duration-300"
            style={`width: ${Math.round(progress.fraction * 100)}%`}
          ></div>
        {:else}
          <div class="h-full w-1/3 animate-pulse rounded-full bg-sky-400"></div>
        {/if}
      </div>
      {#if progress.fraction !== null}
        <p class="text-xs text-zinc-500">{Math.round(progress.fraction * 100)}%</p>
      {/if}
    {/if}

    {#if detail?.partials_dropped}
      <p class="rounded bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:bg-amber-950 dark:text-amber-200">
        ⚠ {detail.partials_dropped} output frame{detail.partials_dropped === 1 ? '' : 's'} dropped
        (bounded buffers) — the log below is incomplete; the final result is authoritative.
      </p>
    {/if}

    {#if plan}
      <div>
        <h2 class="mb-2 text-sm font-medium text-zinc-600 dark:text-zinc-300">Plan DAG</h2>
        <div class="rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <DagView {plan} steps={progress.steps} />
        </div>
      </div>
    {/if}

    {#if logs.length > 0}
      <div>
        <h2 class="mb-2 text-sm font-medium text-zinc-600 dark:text-zinc-300">Output</h2>
        <div
          bind:this={logPane}
          class="max-h-80 overflow-y-auto rounded-xl border border-zinc-200 bg-zinc-950 p-3 font-mono text-xs text-zinc-100 dark:border-zinc-800"
        >
          {#each logs as frame (frame.seq)}
            <div class="flex gap-2">
              <span
                class="shrink-0"
                class:text-zinc-500={frame.stream === 'stdout'}
                class:text-rose-400={frame.stream === 'stderr'}
              >
                {frame.stream === 'stderr' ? '!' : '·'}
              </span>
              <span class="whitespace-pre-wrap break-all">{frame.line}</span>
            </div>
          {/each}
        </div>
      </div>
    {/if}

    {#if detail?.provenance?.length}
      <div>
        <h2 class="mb-2 text-sm font-medium text-zinc-600 dark:text-zinc-300">
          Federated contributions
        </h2>
        <div class="overflow-hidden rounded-xl border border-zinc-200 dark:border-zinc-800">
          <table class="w-full text-sm">
            <thead class="bg-zinc-50 text-left text-xs uppercase tracking-wide text-zinc-500 dark:bg-zinc-900">
              <tr>
                <th class="px-4 py-2">node</th>
                <th class="px-4 py-2">status</th>
                <th class="px-4 py-2">duration</th>
                <th class="px-4 py-2">items</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-zinc-100 bg-white dark:divide-zinc-800 dark:bg-zinc-900">
              {#each detail.provenance as row (row.node_id)}
                <tr>
                  <td class="px-4 py-2 font-mono text-xs">{row.node_id.slice(0, 12)}…</td>
                  <td class="px-4 py-2">
                    <span
                      class="rounded-full px-2 py-0.5 text-xs dark:bg-zinc-800"
                      class:bg-emerald-100={row.status === 'ok'}
                      class:bg-rose-100={row.status === 'failed' || row.status === 'timed_out'}
                      class:bg-zinc-100={row.status === 'skipped'}
                    >
                      {row.status}
                    </span>
                  </td>
                  <td class="px-4 py-2 text-xs text-zinc-500">{row.duration_ms}ms</td>
                  <td class="px-4 py-2 text-xs">{row.item_count}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </div>
    {/if}

    {#if output !== undefined}
      <div>
        <h2 class="mb-2 text-sm font-medium text-zinc-600 dark:text-zinc-300">Result</h2>
        <pre class="overflow-x-auto rounded-xl border border-emerald-200 bg-emerald-50 p-3 text-xs dark:border-emerald-900 dark:bg-emerald-950">{fmtJson(output)}</pre>
      </div>
    {/if}
    {#if errorText}
      <div>
        <h2 class="mb-2 text-sm font-medium text-zinc-600 dark:text-zinc-300">Error</h2>
        <pre class="overflow-x-auto rounded-xl border border-rose-200 bg-rose-50 p-3 text-xs text-rose-800 dark:border-rose-900 dark:bg-rose-950 dark:text-rose-200">{errorText}</pre>
      </div>
    {/if}
  </section>
{/if}
