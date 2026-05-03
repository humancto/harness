<script lang="ts">
  import { onMount } from 'svelte';
  import AuthGate from '$lib/components/AuthGate.svelte';

  type CapabilityDto = {
    id: string;
    version: string;
    cardinality: string;
    cost_hint: string;
  };

  let authed = $state(false);
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

  async function submit(e: Event) {
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
        authed = true;
        void loadCapabilities();
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
    <header class="mb-6">
      <h1 class="text-2xl font-semibold">Submit a task</h1>
      <p class="text-sm text-zinc-500">
        Pick a capability, give it some JSON, and the dispatcher will route it.
      </p>
    </header>

    <form
      class="space-y-4 rounded-xl border border-zinc-200 bg-white p-6 shadow-sm dark:border-zinc-800 dark:bg-zinc-900"
      onsubmit={submit}
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
        <pre class="rounded bg-rose-50 p-3 text-xs text-rose-800 dark:bg-rose-950 dark:text-rose-200">{lastError}</pre>
      {/if}
      {#if lastResult}
        <pre
          class="rounded bg-emerald-50 p-3 text-xs text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200">{lastResult}</pre>
        <a href="/runs" class="text-xs text-zinc-500 underline">view runs →</a>
      {/if}
    </form>
  </section>
{/if}
