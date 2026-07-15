<script>
  import { Select } from "svelte-lib/components"

  import graphData from "$lib/static/graphData.json"
  import AppShell from "./AppShell.svelte"
  import NetworkGraph from "./NetworkGraph.svelte"

  let wars = graphData.wars
  let graphsByWarNum = graphData.graphsByWarNum

  function plural(value, noun) {
    return `${Number(value || 0).toLocaleString()} ${noun}${Number(value || 0) == 1 ? "" : "s"}`
  }

  function timeframe(war) {
    if (!war) return ""

    if (war.ongoing_conflict) return `${war.start_year}-Present`
    if (war.start_year == war.end_year) return String(war.start_year)

    return `${war.start_year}-${war.end_year}`
  }

  function warSecondaryLabel(war) {
    return `${plural(war.total_participants, "participant")} | ${plural(war.total_dyads, "dyad")} | ${timeframe(war)}`
  }

  let warTypeItems = Array.from(new Set(wars.map(war => war.war_type)))
    .sort()
    .map(warType => ({
      value: warType,
      label: warType,
    }))
  let selectedWarTypes = [...warTypeItems]
  let selectedWarItem = null

  $: selectedWarTypeValues = selectedWarTypes?.length ? selectedWarTypes.map(d => d.value) : []
  $: filteredWars = selectedWarTypeValues.length
    ? wars.filter(war => selectedWarTypeValues.includes(war.war_type))
    : wars
  $: warItems = filteredWars.map(war => ({
    value: String(war.war_num),
    label: war.war_name,
    selectedLabel: `${war.war_name} (${timeframe(war)})`,
    secondaryLabel: warSecondaryLabel(war),
    war_type: war.war_type,
    war,
  }))
  $: if (warItems.length && !warItems.some(item => item.value == selectedWarItem?.value)) {
    selectedWarItem = warItems[0]
  }
  $: selectedWar = selectedWarItem?.war
  $: selectedGraph = selectedWar ? graphsByWarNum[String(selectedWar.war_num)] || { nodes: [], links: [] } : null
  $: totalParticipants = filteredWars.reduce((total, war) => total + Number(war.total_participants || 0), 0)
  $: totalDyads = filteredWars.reduce((total, war) => total + Number(war.total_dyads || 0), 0)
  $: largestWar = filteredWars.reduce(
    (current, war) => (Number(war.total_participants || 0) > Number(current?.total_participants || 0) ? war : current),
    null
  )
  $: selectedTypeLabel = selectedWarTypeValues.length == warTypeItems.length ? "All war types" : selectedWarTypeValues.join(", ")
</script>

<AppShell active="browser">
  <main class="mx-auto flex w-full max-w-none flex-col gap-4 px-4 py-5 sm:px-6">
    <section class="grid gap-4 border border-[#d8d3c4] bg-white p-4 xl:grid-cols-[minmax(260px,360px)_minmax(360px,1fr)_minmax(420px,0.9fr)]">
      <div>
        <div class="mb-3">
          <h2 class="text-base font-extrabold">Filters</h2>
          <p class="mt-1 text-sm text-[#60706a]">{selectedTypeLabel}</p>
        </div>

        <div class="mb-1 text-xs font-extrabold uppercase tracking-[0.16em] text-[#596b64]">War Types</div>
        <Select
          items={warTypeItems}
          bind:value={selectedWarTypes}
          multiple={true}
          placeholder="Filter war types"
          on:valueChange={({ detail }) => (selectedWarTypes = detail.d || [])}
        />
      </div>

      <div>
        <div class="mb-1 text-xs font-extrabold uppercase tracking-[0.16em] text-[#596b64]">War</div>
        <Select
          items={warItems}
          bind:value={selectedWarItem}
          groupBy="war_type"
          labelConstruction={true}
          selectedLabelIdentifier="selectedLabel"
          secondaryLabelIdentifier="secondaryLabel"
          placeholder="Select a war"
          noItemsMessage="No wars match the selected filters."
          on:valueChange={({ detail }) => (selectedWarItem = detail.d)}
        />
      </div>

      <div class="grid grid-cols-2 gap-2 sm:grid-cols-4 xl:grid-cols-2">
        <div class="border border-[#d8d3c4] bg-[#fbfcf9] p-3">
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Wars</div>
          <div class="mt-1 text-2xl font-extrabold">{filteredWars.length.toLocaleString()}</div>
        </div>
        <div class="border border-[#d8d3c4] bg-[#fbfcf9] p-3">
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Participants</div>
          <div class="mt-1 text-2xl font-extrabold">{totalParticipants.toLocaleString()}</div>
        </div>
        <div class="border border-[#d8d3c4] bg-[#fbfcf9] p-3">
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Dyads</div>
          <div class="mt-1 text-2xl font-extrabold">{totalDyads.toLocaleString()}</div>
        </div>
        <div class="border border-[#d8d3c4] bg-[#fbfcf9] p-3">
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Largest</div>
          <div class="mt-1 truncate text-sm font-extrabold" title={largestWar?.war_name}>{largestWar?.war_name}</div>
        </div>
      </div>
    </section>

    {#if selectedWar}
      <section class="grid gap-3 border border-[#d8d3c4] bg-white p-4 text-sm md:grid-cols-[minmax(260px,1.2fr)_repeat(4,minmax(120px,0.6fr))]">
        <div>
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Selected War</div>
          <div class="mt-1 text-base font-extrabold">{selectedWar.war_name}</div>
        </div>
        <div>
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Type</div>
          <div class="mt-1 font-semibold">{selectedWar.war_type}</div>
        </div>
        <div>
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Subtype</div>
          <div class="mt-1 font-semibold">{selectedWar.war_subtype || "Unspecified"}</div>
        </div>
        <div>
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Timeframe</div>
          <div class="mt-1 font-semibold">{timeframe(selectedWar)}</div>
        </div>
        <div>
          <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Duration</div>
          <div class="mt-1 font-semibold">{Number(selectedWar.total_days_in_war || 0).toLocaleString()} days</div>
        </div>
      </section>
    {/if}

    <NetworkGraph graph={selectedGraph} {selectedWar} />
  </main>
</AppShell>
