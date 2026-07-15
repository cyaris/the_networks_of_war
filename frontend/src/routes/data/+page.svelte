<script>
  import AppShell from "../../lib/components/AppShell.svelte"
  import graphData from "../../lib/static/graphData.json"
</script>

<AppShell active="data">
  <main class="mx-auto w-full max-w-5xl px-4 py-6 sm:px-6">
    <section class="border border-[#d8d3c4] bg-white p-5">
      <h2 class="text-xl font-extrabold">Data Loading Shape</h2>
      <p class="mt-3 text-[#465750]">
        This frontend snapshot is generated from Step 3 DuckDB outputs. The browser currently uses
        <code>final_wars</code>, <code>d3_war_nodes</code>, and <code>d3_war_links</code> so the UI stays normalized and compact.
      </p>
      <p class="mt-3 text-[#465750]">
        For an API-backed version, <code>d3_war_json</code> can serve one graph payload by <code>war_id</code> while
        <code>final_wars</code> continues to power selectors and filters.
      </p>
      <div class="mt-5 grid gap-3 sm:grid-cols-3">
        <div class="border border-[#d2d7d3] bg-[#fbfcf9] p-4">
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Wars</div>
          <div class="mt-1 text-2xl font-extrabold">{graphData.wars.length.toLocaleString()}</div>
        </div>
        <div class="border border-[#d2d7d3] bg-[#fbfcf9] p-4">
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Graph Tables</div>
          <div class="mt-1 text-2xl font-extrabold">2</div>
        </div>
        <div class="border border-[#d2d7d3] bg-[#fbfcf9] p-4">
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Payload Mode</div>
          <div class="mt-1 text-2xl font-extrabold">Static</div>
        </div>
      </div>
    </section>
  </main>
</AppShell>
