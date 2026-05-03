<script lang="ts">
  import { onMount } from 'svelte';

  let password = $state('');
  let error = $state<string | null>(null);
  let loading = $state(false);

  let { onAuthed }: { onAuthed: () => void } = $props();

  async function login() {
    error = null;
    loading = true;
    try {
      const res = await fetch('/api/v1/auth/login', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ password }),
      });
      if (!res.ok) {
        if (res.status === 503) {
          error =
            'admin password not set yet — run `harness admin set-password` on the daemon host';
        } else {
          error = `login failed (${res.status})`;
        }
        return;
      }
      onAuthed();
    } finally {
      loading = false;
    }
  }
</script>

<form
  class="mx-auto mt-12 max-w-sm space-y-3 rounded-xl border border-zinc-200 bg-white p-6 shadow-sm dark:border-zinc-800 dark:bg-zinc-900"
  onsubmit={(e) => {
    e.preventDefault();
    void login();
  }}
>
  <h2 class="text-lg font-semibold">Sign in</h2>
  <p class="text-xs text-zinc-500">Admin password from <code>harness admin set-password</code>.</p>
  <input
    type="password"
    placeholder="password"
    bind:value={password}
    class="w-full rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-950"
    autocomplete="current-password"
    required
  />
  {#if error}
    <p class="text-xs text-rose-600 dark:text-rose-400">{error}</p>
  {/if}
  <button
    type="submit"
    disabled={loading || password.length === 0}
    class="w-full rounded-md bg-zinc-900 px-3 py-2 text-sm font-medium text-white hover:bg-zinc-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
  >
    {loading ? 'signing in…' : 'Sign in'}
  </button>
</form>
