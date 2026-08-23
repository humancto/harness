<script lang="ts">
  import '../app.css';
</script>

<div class="min-h-full">
  <header class="border-b border-zinc-200 bg-white/70 backdrop-blur dark:border-zinc-800 dark:bg-zinc-950/70">
    <div class="mx-auto flex max-w-6xl items-center justify-between px-4 py-3">
      <div class="flex items-center gap-2">
        <span class="font-mono text-sm font-bold tracking-tight">harness</span>
        <span class="rounded bg-zinc-100 px-1.5 py-0.5 font-mono text-[10px] text-zinc-500 dark:bg-zinc-800">
          mesh
        </span>
      </div>
      <nav class="flex gap-4 text-sm">
        <a href="/" class="text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100">
          mesh
        </a>
        <a href="/submit" class="text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100">
          submit
        </a>
        <a href="/runs" class="text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100">
          runs
        </a>
        <a href="/shell" class="text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100">
          shell
        </a>
        <a href="/costs" class="text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100">
          costs
        </a>
      </nav>
    </div>
  </header>
  <main class="mx-auto max-w-6xl px-4 py-6">
    <slot />
  </main>
</div>
