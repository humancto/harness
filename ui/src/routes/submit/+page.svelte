<script lang="ts">
  import { goto } from '$app/navigation';
  import { onMount } from 'svelte';
  import AuthGate from '$lib/components/AuthGate.svelte';
  import DagView from '$lib/components/DagView.svelte';
  import {
    execSubmitBody,
    inputPreview,
    parsePlanOutcome,
    planSubmitBody,
    pollTask,
    type PlanOutcome,
  } from '$lib/nlsubmit';
  import type { PlanInput, SubmitTaskResponse } from '$lib/types';
  import type { StepView } from '$lib/progress';

  type CapabilityDto = {
    id: string;
    version: string;
    cardinality: string;
    cost_hint: string;
  };

  let authed = $state(false);
  let tab = $state<'nl' | 'advanced'>('nl');

  // ── NL mode state machine: idle → planning → preview → submitting.
  // `plan_failed` renders the 5.3 per-tier diagnostics verbatim.
  type NlPhase = 'idle' | 'planning' | 'preview' | 'submitting' | 'plan_failed';
  let phase = $state<NlPhase>('idle');
  let goal = $state('');
  let allowCloud = $state(false);
  let keepGoing = $state(false);
  let planError = $state('');
  let planned = $state<Extract<PlanOutcome, { kind: 'planned' }> | null>(null);

  // Teardown discipline (4.8 lesson, plan review MAJOR-3): every
  // async callback checks its generation before touching state or
  // navigating; the AbortController is owned by an $effect whose
  // teardown fires on destroy, so navigating away never leaves the
  // poll loop running against unmounted state.
  let gen = 0;
  let controller: AbortController | null = null;

  $effect(() => {
    return () => {
      gen += 1;
      controller?.abort();
    };
  });

  const previewSteps: Record<string, StepView> = {};

  function resetNl() {
    controller?.abort();
    gen += 1;
    phase = 'idle';
    planError = '';
    planned = null;
  }

  async function startPlanning(e: Event) {
    e.preventDefault();
    if (!goal.trim()) return;
    const myGen = ++gen;
    controller?.abort();
    controller = new AbortController();
    phase = 'planning';
    planError = '';
    planned = null;
    try {
      const res = await fetch('/api/v1/tasks', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(planSubmitBody(goal.trim(), allowCloud)),
      });
      if (gen !== myGen) return;
      if (res.status === 401) {
        // Recover the phase BEFORE gating (Codex P2): a stuck
        // 'planning'/'submitting' phase would come back from re-auth
        // with dead controls.
        phase = 'idle';
        authed = false;
        return;
      }
      if (!res.ok) {
        phase = 'plan_failed';
        planError = `submit failed (${res.status}): ${await res.text()}`;
        return;
      }
      const submitted = (await res.json()) as SubmitTaskResponse;
      const polled = await pollTask(fetch, submitted.task_id, {
        signal: controller.signal,
      });
      if (gen !== myGen) return;
      switch (polled.kind) {
        case 'aborted':
          return; // reset/cancel already updated the UI
        case 'unauthorized':
          phase = 'idle';
          authed = false;
          return;
        case 'timeout':
          phase = 'plan_failed';
          planError = 'planning timed out — the mesh never returned a terminal state';
          return;
        case 'http_error':
          phase = 'plan_failed';
          planError = `poll failed (${polled.status}): ${polled.body}`;
          return;
        case 'terminal': {
          const outcome = parsePlanOutcome(polled.task);
          if (outcome.kind === 'planned') {
            planned = outcome;
            phase = 'preview';
          } else {
            phase = 'plan_failed';
            planError = outcome.kind === 'failed' ? outcome.error : 'planner returned no plan';
          }
          return;
        }
      }
    } catch (err) {
      if (gen !== myGen) return;
      phase = 'plan_failed';
      planError = `${err}`;
    }
  }

  async function confirmAndRun() {
    if (!planned) return;
    const myGen = ++gen;
    phase = 'submitting';
    try {
      const res = await fetch('/api/v1/tasks', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(execSubmitBody(planned.plan, keepGoing)),
      });
      if (gen !== myGen) return;
      if (res.status === 401) {
        // Codex P2: leaving phase='submitting' would render the
        // restored preview with permanently disabled buttons.
        phase = 'preview';
        authed = false;
        return;
      }
      if (!res.ok) {
        phase = 'preview';
        planError = `execute submit failed (${res.status}): ${await res.text()}`;
        return;
      }
      const submitted = (await res.json()) as SubmitTaskResponse;
      if (gen !== myGen) return;
      void goto(`/runs/${submitted.task_id}`);
    } catch (err) {
      if (gen !== myGen) return;
      phase = 'preview';
      planError = `${err}`;
    }
  }

  function planStepRows(plan: unknown): { id: string; capability: string; preview: string }[] {
    const p = plan as PlanInput;
    if (!p || typeof p.tasks !== 'object') return [];
    return Object.entries(p.tasks).map(([id, node]) => ({
      id,
      capability: node.capability,
      preview: inputPreview((node as Record<string, unknown>).input),
    }));
  }

  // ── Advanced mode (the pre-5.4 form, unchanged behavior).
  let caps = $state<CapabilityDto[]>([]);
  let selectedCapability = $state('');
  let inputJson = $state('{}');
  let submitting = $state(false);
  let lastResult = $state<string | null>(null);
  let lastError = $state<string | null>(null);

  async function loadCapabilities() {
    try {
      const res = await fetch('/api/v1/capabilities');
      if (!res.ok) throw new Error(`status ${res.status}`);
      const json = await res.json();
      caps = json.capabilities ?? [];
      if (caps.length > 0 && !selectedCapability) {
        selectedCapability = caps[0].id;
      }
    } catch (err) {
      lastError = `failed to load capabilities: ${err}`;
    }
  }

  async function submitAdvanced(e: Event) {
    e.preventDefault();
    submitting = true;
    lastError = null;
    lastResult = null;
    try {
      const parsed = JSON.parse(inputJson);
      const res = await fetch('/api/v1/tasks', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ capability: selectedCapability, input: parsed }),
      });
      const text = await res.text();
      if (res.status === 401) {
        authed = false;
        lastError = 'session expired — sign in again';
        return;
      }
      if (!res.ok) {
        lastError = `submit failed (${res.status}): ${text}`;
        return;
      }
      lastResult = text;
    } catch (err) {
      lastError = `${err}`;
    } finally {
      submitting = false;
    }
  }

  function probeAuth() {
    fetch('/api/v1/tasks').then((res) => {
      if (res.ok) {
        // The authed $effect below is the single capabilities-load
        // path (pre-5.4 this ALSO called loadCapabilities — a cold
        // load double-fetch; plan review MINOR-5).
        authed = true;
      }
    });
  }

  onMount(() => {
    probeAuth();
  });

  $effect(() => {
    if (authed) void loadCapabilities();
  });
</script>

<svelte:head>
  <title>harness · submit</title>
</svelte:head>

{#if !authed}
  <AuthGate
    onAuthed={() => {
      authed = true;
    }}
  />
{:else}
  <section class="mx-auto max-w-3xl">
    <header class="mb-4">
      <h1 class="text-2xl font-semibold">Submit</h1>
      <p class="text-sm text-zinc-500">
        Describe what you want, preview the plan, run it — or drop to the raw capability form.
      </p>
    </header>

    <nav class="mb-4 flex gap-1 rounded-lg bg-zinc-100 p-1 text-sm dark:bg-zinc-900">
      <button
        class={`flex-1 rounded-md px-3 py-1.5 font-medium ${tab === 'nl' ? 'bg-white shadow dark:bg-zinc-800' : 'text-zinc-500'}`}
        onclick={() => (tab = 'nl')}
      >
        Describe it
      </button>
      <button
        class={`flex-1 rounded-md px-3 py-1.5 font-medium ${tab === 'advanced' ? 'bg-white shadow dark:bg-zinc-800' : 'text-zinc-500'}`}
        onclick={() => (tab = 'advanced')}
      >
        Advanced
      </button>
    </nav>

    {#if tab === 'nl'}
      <div
        class="space-y-4 rounded-xl border border-zinc-200 bg-white p-6 shadow-sm dark:border-zinc-800 dark:bg-zinc-900"
      >
        {#if phase === 'idle' || phase === 'plan_failed'}
          <form class="space-y-4" onsubmit={startPlanning}>
            <label class="block">
              <span class="text-sm font-medium">what should the mesh do?</span>
              <textarea
                bind:value={goal}
                rows="4"
                placeholder="run: uname -a — or: summarize every markdown file in ~/notes"
                class="mt-1 w-full rounded-md border border-zinc-300 bg-zinc-50 px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-950"
              ></textarea>
            </label>
            <label class="flex items-center gap-2 text-sm">
              <input type="checkbox" bind:checked={allowCloud} />
              <span>
                allow cloud escalation
                <span class="text-xs text-zinc-500">
                  (mesh policy must also allow it — this is the per-task opt-in)
                </span>
              </span>
            </label>
            <button
              type="submit"
              disabled={!goal.trim()}
              class="rounded-md bg-zinc-900 px-3 py-2 text-sm font-medium text-white hover:bg-zinc-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
            >
              Plan it
            </button>
            {#if phase === 'plan_failed'}
              <div>
                <p class="mb-1 text-xs font-medium text-rose-700 dark:text-rose-300">
                  planning failed — the per-tier diagnostics:
                </p>
                <pre
                  class="overflow-x-auto rounded bg-rose-50 p-3 text-xs text-rose-800 dark:bg-rose-950 dark:text-rose-200">{planError}</pre>
              </div>
            {/if}
          </form>
        {:else if phase === 'planning'}
          <div class="space-y-3">
            <p class="animate-pulse text-sm">
              walking the planner tiers (LocalFast → LocalStrong → Cloud → Template — up to ~4
              min)…
            </p>
            <button
              class="rounded-md border border-zinc-300 px-3 py-1.5 text-sm dark:border-zinc-700"
              onclick={resetNl}
            >
              Cancel
            </button>
            <p class="text-xs text-zinc-500">
              cancel stops waiting here; the planning task itself finishes (or times out) on the
              mesh.
            </p>
          </div>
        {:else if (phase === 'preview' || phase === 'submitting') && planned}
          <div class="space-y-4">
            <div class="flex flex-wrap items-center gap-4 text-sm">
              <div class="flex items-center gap-2">
                <span class="text-zinc-500">confidence</span>
                <div class="h-2 w-24 overflow-hidden rounded bg-zinc-200 dark:bg-zinc-800">
                  <div
                    class="h-full bg-emerald-500"
                    style={`width: ${Math.round(planned.confidence * 100)}%`}
                  ></div>
                </div>
                <span class="font-mono text-xs">{planned.confidence.toFixed(2)}</span>
              </div>
              <span class="text-zinc-500">
                est. cost <span class="font-mono">${planned.costUsd.toFixed(4)}</span>
              </span>
              <span class="text-zinc-500">
                est. duration <span class="font-mono">{planned.durationMs}ms</span>
              </span>
            </div>
            {#if planned.rationale}
              <p class="text-sm italic text-zinc-600 dark:text-zinc-400">
                “{planned.rationale}”
              </p>
            {/if}

            <DagView plan={planned.plan as PlanInput} steps={previewSteps} />

            <ul class="space-y-1 text-xs">
              {#each planStepRows(planned.plan) as row (row.id)}
                <li class="flex gap-2">
                  <span class="font-mono text-zinc-400">{row.id.slice(0, 8)}</span>
                  <span class="font-medium">{row.capability}</span>
                  <span class="truncate font-mono text-zinc-500">{row.preview}</span>
                </li>
              {/each}
            </ul>

            <label class="flex items-center gap-2 text-sm">
              <input type="checkbox" bind:checked={keepGoing} />
              <span>keep executing independent branches past a step failure</span>
            </label>

            {#if planError}
              <pre
                class="overflow-x-auto rounded bg-rose-50 p-3 text-xs text-rose-800 dark:bg-rose-950 dark:text-rose-200">{planError}</pre>
            {/if}

            <div class="flex gap-2">
              <button
                disabled={phase === 'submitting'}
                class="rounded-md bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
                onclick={confirmAndRun}
              >
                {phase === 'submitting' ? 'starting…' : 'Confirm & run'}
              </button>
              <button
                disabled={phase === 'submitting'}
                class="rounded-md border border-zinc-300 px-3 py-2 text-sm dark:border-zinc-700"
                onclick={resetNl}
              >
                Discard
              </button>
            </div>
          </div>
        {/if}
      </div>
    {:else}
      <form
        class="space-y-4 rounded-xl border border-zinc-200 bg-white p-6 shadow-sm dark:border-zinc-800 dark:bg-zinc-900"
        onsubmit={submitAdvanced}
      >
        <label class="block">
          <span class="text-sm font-medium">capability</span>
          <select
            bind:value={selectedCapability}
            class="mt-1 w-full rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-950"
            required
          >
            {#each caps as c (c.id)}
              <option value={c.id}>
                {c.id} · {c.cardinality} · {c.cost_hint}
              </option>
            {/each}
          </select>
        </label>

        <label class="block">
          <span class="text-sm font-medium">input (JSON)</span>
          <textarea
            bind:value={inputJson}
            rows="6"
            class="mt-1 w-full rounded-md border border-zinc-300 bg-zinc-50 px-3 py-2 font-mono text-sm dark:border-zinc-700 dark:bg-zinc-950"
          ></textarea>
        </label>

        <button
          type="submit"
          disabled={submitting || !selectedCapability}
          class="rounded-md bg-zinc-900 px-3 py-2 text-sm font-medium text-white hover:bg-zinc-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
        >
          {submitting ? 'submitting…' : 'Submit'}
        </button>

        {#if lastError}
          <pre
            class="rounded bg-rose-50 p-3 text-xs text-rose-800 dark:bg-rose-950 dark:text-rose-200">{lastError}</pre>
        {/if}
        {#if lastResult}
          <pre
            class="rounded bg-emerald-50 p-3 text-xs text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200">{lastResult}</pre>
          <a href="/runs" class="text-xs text-zinc-500 underline">view runs →</a>
        {/if}
      </form>
    {/if}
  </section>
{/if}
