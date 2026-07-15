<script>
  import AppShell from "$lib/components/AppShell.svelte"
</script>

<AppShell active="development">
  <main class="mx-auto w-full max-w-5xl px-4 py-6 sm:px-6">
    <section class="border border-[#d8d3c4] bg-white p-5">
      <h2 class="text-xl font-extrabold">Development Notes</h2>
      <div class="mt-4 grid gap-4 text-[#465750] sm:grid-cols-2">
        <p>
          The first screen replaces the legacy war-type menu with selector state. War types are a multi-select filter,
          and the war selector groups options by war type with participant and dyad counts in the secondary label.
        </p>
        <p>
          The graph is intentionally lean for the first pass: it uses the Step 3 node/link grain and leaves richer
          descriptor panels for the next increment.
        </p>
      </div>
    </section>
  </main>
</AppShell>
