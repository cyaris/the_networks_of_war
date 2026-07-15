<script>
  import { Select } from "svelte-lib/components"

  import graphData from "../static/graphData.json"
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
    let dashCount = linkDashFieldCountsByWarNum[String(war.war_num)] || 0
    let dashLabel = dashCount ? ` | ${plural(dashCount, "dash field")}` : ""

    return `${plural(war.total_participants, "participant")} | ${plural(war.total_dyads, "dyad")} | ${timeframe(war)}${dashLabel}`
  }

  function graphForWar(war) {
    return graphsByWarNum[String(war?.war_num)] || { nodes: [], links: [] }
  }

  function linkDashFieldCount(war) {
    let graph = graphForWar(war)
    let fields = Array.from(
      new Set(
        (graph.links || []).flatMap(link =>
          Object.keys(link).filter(field => field.endsWith("_x") || field.endsWith("_y") || field.endsWith("_z"))
        )
      )
    )

    return fields.filter(field => graph.links.some(link => Number(link[field]) > 0)).length
  }

  function preferredWarItem(items) {
    return items.find(item => item.linkDashFieldCount > 0) || items[0]
  }

  let linkDashFieldCountsByWarNum = Object.fromEntries(wars.map(war => [String(war.war_num), linkDashFieldCount(war)]))
  let warTypeItems = Array.from(new Set(wars.map(war => war.war_type)))
    .sort()
    .map(warType => ({
      value: warType,
      label: warType,
    }))
  let selectedWarTypes = [...warTypeItems]
  let selectedWarItem = null
  let width
  let toolViewportWidth

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
    linkDashFieldCount: linkDashFieldCountsByWarNum[String(war.war_num)] || 0,
    war,
  }))
  $: if (warItems.length && !warItems.some(item => item.value == selectedWarItem?.value)) {
    selectedWarItem = preferredWarItem(warItems)
  }
  $: selectedWar = selectedWarItem?.war
  $: selectedGraph = selectedWar ? graphForWar(selectedWar) : null
  $: selectedLinkDashFieldCount = selectedWar ? linkDashFieldCountsByWarNum[String(selectedWar.war_num)] || 0 : 0
  $: totalParticipants = filteredWars.reduce((total, war) => total + Number(war.total_participants || 0), 0)
  $: totalDyads = filteredWars.reduce((total, war) => total + Number(war.total_dyads || 0), 0)
  $: warsWithLinkDashes = filteredWars.filter(war => linkDashFieldCountsByWarNum[String(war.war_num)] > 0).length
  $: selectedTypeLabel =
    selectedWarTypeValues.length == warTypeItems.length ? "All war types" : selectedWarTypeValues.join(", ")
  $: toolViewportWidth = width ? width * 0.7 : 0
</script>

<AppShell active="browser">
  <main class="flex h-full w-full flex-col items-center justify-center" bind:clientWidth={width}>
    <div class="px-8 py-12 text-center text-lg min-[1300px]:hidden">
      This visualization is best viewed on a larger screen. So, grab a computer and come back soon!
    </div>
    <div class="hidden min-[1300px]:block" style="width:{toolViewportWidth}px">
      <div class="mx-auto flex w-full flex-col gap-4 py-5">
        <section
          class="grid gap-4 border border-[#d8d3c4] bg-white p-4 xl:grid-cols-[minmax(260px,360px)_minmax(360px,1fr)_minmax(420px,0.9fr)]"
        >
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
              <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Dash Data</div>
              <div class="mt-1 text-2xl font-extrabold">{warsWithLinkDashes.toLocaleString()}</div>
            </div>
          </div>
        </section>

        {#if selectedWar}
          <section
            class="grid gap-3 border border-[#d8d3c4] bg-white p-4 text-sm md:grid-cols-[minmax(260px,1.2fr)_repeat(5,minmax(120px,0.6fr))]"
          >
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
            <div>
              <div class="text-xs font-bold uppercase tracking-[0.14em] text-[#60706a]">Dash Fields</div>
              <div class="mt-1 font-semibold">{selectedLinkDashFieldCount.toLocaleString()}</div>
            </div>
          </section>
        {/if}

        <div class="relative w-full overflow-hidden border border-solid border-black">
          <NetworkGraph graph={selectedGraph} {selectedWar} />
        </div>
      </div>
    </div>
  </main>
</AppShell>
