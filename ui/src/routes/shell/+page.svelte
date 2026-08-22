<script lang="ts">
  import { onMount } from 'svelte';
  import AuthGate from '$lib/components/AuthGate.svelte';
  import type {
    PeersSnapshot,
    ShellExecOutput,
    SubmitTaskRequest,
    SubmitTaskResponse,
    TaskDetailDto,
  } from '$lib/types';
  import {
    hexToBytes,
    meshTargets,
    nodeColorClass,
    parseArgv,
    resolveTargets,
    summarizeExits,
    type NodeRunResult,
    type ShellTarget,
    type TargetMode,
  } from '$lib/shell';

  // Mirrors crates/harness-cli/src/run.rs constants.
  const EXECUTION_TIMEOUT_SLACK_MS = 5_000;
  const POLL_INITIAL_MS = 250;
  const POLL_MAX_MS = 1_000;
  const LEASE_MS = 10_000;
  const TERMINAL_STATES = ['done', 'failed', 'expired', 'cancelled'];

  const TIMEOUT_CHOICES = [
    { label: '10s', ms: 10_000 },
    { label: '60s', ms: 60_000 },
    { label: '5m', ms: 300_000 },
  ];

  type LineKind = 'cmd' | 'out' | 'err' | 'fail';
  interface OutLine {
    /** null for the echoed command line. */
    label: string | null;
    color: string;
    text: string;
    kind: LineKind;
  }
  interface ShellRun {
    id: number;
    command: string;
    targets: string[];
    lines: OutLine[];
    summary: string | null;
    ok: boolean;
  }

  let authed = $state(false);
  let targets = $state<ShellTarget[]>([]);
  let peersError = $state<string | null>(null);

  let mode = $state<TargetMode>('self');
  let selectedNodeId = $state('');
  let command = $state('');
  let timeoutMs = $state(60_000);

  let running = $state(false);
  let formError = $state<string | null>(null);
  let runs = $state<ShellRun[]>([]);
  let runSeq = 0;
  let scrollbackEl = $state<HTMLDivElement | null>(null);
  let commandEl = $state<HTMLInputElement | null>(null);

  async function refreshPeers(): Promise<void> {
    try {
      const res = await fetch('/api/v1/peers', { headers: { Accept: 'application/json' } });
      if (res.status === 401) {
        authed = false;
        return;
      }
      if (!res.ok) throw new Error(`status ${res.status}`);
      const snapshot = (await res.json()) as PeersSnapshot;
      targets = meshTargets(snapshot);
      peersError = null;
      // Keep the dropdown selection valid across refreshes.
      if (!targets.some((t) => t.node_id === selectedNodeId)) {
        selectedNodeId = targets[0]?.node_id ?? '';
      }
    } catch (err) {
      peersError = `failed to load mesh view: ${err}`;
    }
  }

  function probeAuth() {
    fetch('/api/v1/tasks').then((res) => {
      if (res.ok) authed = true;
    });
  }

  onMount(probeAuth);

  $effect(() => {
    if (authed) void refreshPeers();
  });

  function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function scrollToBottom() {
    requestAnimationFrame(() => {
      scrollbackEl?.scrollTo({ top: scrollbackEl.scrollHeight });
    });
  }

  function pushLines(run: ShellRun, lines: OutLine[]) {
    run.lines.push(...lines);
    scrollToBottom();
  }

  function fmtLastSeen(ms: number): string {
    if (ms < 1_000) return 'now';
    if (ms < 60_000) return `${Math.round(ms / 1000)}s ago`;
    return `${Math.round(ms / 60_000)}m ago`;
  }

  // --- submit + poll (mirrors harness run) ---

  async function submitTask(target: ShellTarget, argv: string[]): Promise<string> {
    const body: SubmitTaskRequest = {
      capability: 'shell.exec',
      input: { cmd: argv[0], args: argv.slice(1), timeout_ms: timeoutMs },
      constraints: {
        deadline: null,
        max_cost_usd: null,
        must_be_local: false,
        require_tags: [],
        exclude_tags: [],
        pin_to_node: hexToBytes(target.node_id),
        pin_to_scope: null,
      },
      execution: {
        redundancy: 1,
        timeout_ms: timeoutMs + EXECUTION_TIMEOUT_SLACK_MS,
        on_partial: 'fail_fast',
        lease_ms: LEASE_MS,
      },
    };
    const res = await fetch('/api/v1/tasks', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (res.status === 401) {
      authed = false;
      throw new Error('session expired — sign in again');
    }
    const text = await res.text();
    if (!res.ok) throw new Error(`submit failed (${res.status}): ${text}`);
    const parsed = JSON.parse(text) as SubmitTaskResponse;
    if (!parsed.task_id) throw new Error(`submit response missing task_id: ${text}`);
    return parsed.task_id;
  }

  async function pollUntilTerminal(taskId: string, deadlineMs: number): Promise<TaskDetailDto> {
    const started = Date.now();
    let backoff = POLL_INITIAL_MS;
    for (;;) {
      if (Date.now() - started >= deadlineMs) {
        throw new Error('task did not complete within deadline');
      }
      const res = await fetch(`/api/v1/tasks/${taskId}`, {
        headers: { Accept: 'application/json' },
      });
      if (res.status === 401) {
        authed = false;
        throw new Error('session expired — sign in again');
      }
      if (!res.ok) throw new Error(`poll failed: HTTP ${res.status}`);
      const bodyJson = (await res.json()) as TaskDetailDto;
      if (TERMINAL_STATES.includes(bodyJson.state)) return bodyJson;

      const remaining = deadlineMs - (Date.now() - started);
      if (remaining <= 0) throw new Error('task did not complete within deadline');
      await sleep(Math.min(backoff, remaining));
      backoff = Math.min(backoff * 2, POLL_MAX_MS);
    }
  }

  /** Terminal envelope → prefixed lines + CLI-equivalent exit code. */
  function renderEnvelope(run: ShellRun, target: ShellTarget, envelope: TaskDetailDto): NodeRunResult {
    const color = nodeColorClass(target.label);
    if (envelope.state !== 'done') {
      pushLines(run, [
        {
          label: target.label,
          color,
          text: `${envelope.state}: ${envelope.error ?? 'unknown error'}`,
          kind: 'fail',
        },
      ]);
      return { label: target.label, state: envelope.state, code: 1, timedOut: false };
    }
    const output = (envelope.output ?? {}) as Partial<ShellExecOutput>;
    const timedOut = output.timed_out === true;
    const code = timedOut ? 124 : (output.exit_code ?? 1);
    const lines: OutLine[] = [];
    for (const [text, kind] of [
      [output.stdout ?? '', 'out'],
      [output.stderr ?? '', 'err'],
    ] as const) {
      if (text.length === 0) continue;
      const body = text.endsWith('\n') ? text.slice(0, -1) : text;
      for (const line of body.split('\n')) {
        lines.push({ label: target.label, color, text: line, kind });
      }
    }
    if (timedOut) {
      lines.push({ label: target.label, color, text: `timed out after ${timeoutMs / 1000}s`, kind: 'fail' });
    }
    pushLines(run, lines);
    return { label: target.label, state: 'done', code, timedOut };
  }

  async function runOnNode(run: ShellRun, target: ShellTarget, argv: string[], deadlineMs: number): Promise<NodeRunResult> {
    try {
      const taskId = await submitTask(target, argv);
      const envelope = await pollUntilTerminal(taskId, deadlineMs);
      return renderEnvelope(run, target, envelope);
    } catch (err) {
      pushLines(run, [
        {
          label: target.label,
          color: nodeColorClass(target.label),
          text: `error: ${err instanceof Error ? err.message : err}`,
          kind: 'fail',
        },
      ]);
      return { label: target.label, state: 'error', code: 1, timedOut: false };
    }
  }

  async function runCommand(e: Event) {
    e.preventDefault();
    if (running) return;
    formError = null;

    let argv: string[];
    try {
      argv = parseArgv(command);
    } catch (err) {
      formError = `${err instanceof Error ? err.message : err}`;
      return;
    }
    if (argv.length === 0) {
      formError = 'empty command';
      return;
    }

    running = true;
    try {
      // Re-resolve against the live mesh view, like the CLI does per run.
      await refreshPeers();
      let resolved: ShellTarget[];
      try {
        resolved = resolveTargets(mode, selectedNodeId, targets);
      } catch (err) {
        formError = `${err instanceof Error ? err.message : err}`;
        return;
      }

      runs.push({
        id: (runSeq += 1),
        command,
        targets: resolved.map((t) => t.label),
        lines: [],
        summary: null,
        ok: false,
      });
      // Re-read through the $state proxy so nested mutations
      // (lines.push from runOnNode) stay reactive.
      const run = runs[runs.length - 1];
      scrollToBottom();

      const deadlineMs = timeoutMs + EXECUTION_TIMEOUT_SLACK_MS;
      // Fan out; outputs interleave in completion order via pushLines.
      const results = await Promise.all(resolved.map((t) => runOnNode(run, t, argv, deadlineMs)));
      run.summary = summarizeExits(results);
      run.ok = results.every((r) => r.code === 0);
      scrollToBottom();
    } finally {
      running = false;
      // Keep focus in the input so Enter re-runs immediately.
      requestAnimationFrame(() => commandEl?.focus());
    }
  }
</script>

<svelte:head>
  <title>harness · shell</title>
</svelte:head>

{#if !authed}
  <AuthGate
    onAuthed={() => {
      authed = true;
    }}
  />
{:else}
  <section class="mx-auto max-w-5xl">
    <header class="mb-6">
      <h1 class="text-2xl font-semibold">Remote Shell</h1>
      <p class="text-sm text-zinc-500">
        Run a command on one node or the whole fleet — output interleaves with
        <code class="font-mono">[node-name]</code> prefixes, like <code class="font-mono">harness run</code>.
      </p>
    </header>

    {#if peersError}
      <pre class="mb-4 rounded bg-rose-50 p-3 text-xs text-rose-800 dark:bg-rose-950 dark:text-rose-200">{peersError}</pre>
    {/if}

    <form
      class="space-y-4 rounded-xl border border-zinc-200 bg-white p-6 shadow-sm dark:border-zinc-800 dark:bg-zinc-900"
      onsubmit={runCommand}
    >
      <fieldset>
        <legend class="text-sm font-medium">target</legend>
        <div class="mt-2 flex flex-wrap items-center gap-2">
          {#each [{ value: 'self', label: 'self' }, { value: 'all', label: 'all nodes' }, { value: 'node', label: 'pick a node' }] as choice (choice.value)}
            <label
              class="cursor-pointer rounded-full border px-3 py-1 text-sm transition-colors {mode === choice.value
                ? 'border-zinc-900 bg-zinc-900 text-white dark:border-zinc-100 dark:bg-zinc-100 dark:text-zinc-900'
                : 'border-zinc-300 text-zinc-600 hover:border-zinc-500 dark:border-zinc-700 dark:text-zinc-400'}"
            >
              <input type="radio" class="sr-only" name="target-mode" value={choice.value} bind:group={mode} />
              {choice.label}
            </label>
          {/each}

          {#if mode === 'node'}
            <select
              bind:value={selectedNodeId}
              class="rounded-md border border-zinc-300 bg-white px-3 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-950"
              aria-label="node"
            >
              {#each targets as t (t.node_id)}
                <option value={t.node_id}>
                  {t.label}{t.is_self ? ' (self)' : ''} · {t.os ?? 'unknown os'} · {fmtLastSeen(t.last_seen_ms_ago)}
                </option>
              {/each}
            </select>
          {:else if mode === 'all'}
            <span class="text-xs text-zinc-500">
              {targets.length}
              {targets.length === 1 ? 'live node' : 'live nodes'}
            </span>
          {/if}
        </div>
      </fieldset>

      <div class="flex flex-wrap items-end gap-3">
        <label class="min-w-64 grow">
          <span class="text-sm font-medium">command</span>
          <input
            bind:this={commandEl}
            bind:value={command}
            type="text"
            placeholder='uname -a · echo "two words"'
            spellcheck="false"
            autocomplete="off"
            class="mt-1 w-full rounded-md border border-zinc-300 bg-zinc-50 px-3 py-2 font-mono text-sm dark:border-zinc-700 dark:bg-zinc-950"
          />
        </label>

        <label class="shrink-0">
          <span class="text-sm font-medium">timeout</span>
          <select
            bind:value={timeoutMs}
            class="mt-1 block rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-950"
          >
            {#each TIMEOUT_CHOICES as choice (choice.ms)}
              <option value={choice.ms}>{choice.label}</option>
            {/each}
          </select>
        </label>

        <button
          type="submit"
          disabled={running || command.trim().length === 0}
          class="shrink-0 rounded-md bg-zinc-900 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
        >
          {running ? 'running…' : 'Run'}
        </button>
      </div>

      {#if formError}
        <pre class="rounded bg-rose-50 p-3 text-xs text-rose-800 dark:bg-rose-950 dark:text-rose-200">{formError}</pre>
      {/if}
    </form>

    <div
      bind:this={scrollbackEl}
      class="mt-6 max-h-[32rem] overflow-y-auto rounded-xl border border-zinc-200 bg-zinc-950 p-4 font-mono text-xs leading-5 text-zinc-200 shadow-sm dark:border-zinc-800"
    >
      {#if runs.length === 0}
        <p class="text-zinc-500">No runs yet — type a command and press Enter.</p>
      {:else}
        {#each runs as run (run.id)}
          <div class="mb-4 last:mb-0">
            <div class="text-zinc-400">
              <span class="select-none text-zinc-500">$</span>
              {run.command}
              <span class="text-zinc-600">→ {run.targets.join(', ')}</span>
            </div>
            {#each run.lines as line, i (i)}
              <div class="whitespace-pre-wrap break-all">
                {#if line.label !== null}<span class={line.color}>[{line.label}]</span>{/if}
                <span
                  class={line.kind === 'fail'
                    ? 'text-rose-500'
                    : line.kind === 'err'
                      ? 'text-amber-300'
                      : 'text-zinc-200'}>{line.text}</span
                >
              </div>
            {/each}
            {#if run.summary !== null}
              <div class={run.ok ? 'mt-1 text-emerald-400' : 'mt-1 text-rose-400'}>
                — {run.summary}
              </div>
            {:else}
              <div class="mt-1 animate-pulse text-zinc-500">running…</div>
            {/if}
          </div>
        {/each}
      {/if}
    </div>
  </section>
{/if}
