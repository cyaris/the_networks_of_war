<script>
  import { forceCenter, forceCollide, forceLink, forceManyBody, forceSimulation } from "d3-force"
  import { onDestroy } from "svelte"

  export let graph = { nodes: [], links: [] }
  export let selectedWar = null

  let width = 900
  let height = 560
  let simulation
  let nodes = []
  let links = []
  let hoverNode = null

  let sideColors = {
    1: "#2f7f66",
    2: "#b54f72",
    3: "#5f70b8",
    null: "#71717a",
    undefined: "#71717a",
  }

  function nodeRadius(node) {
    let deaths = Number(node.battle_deaths || 0)

    return Math.max(6, Math.min(20, 6 + Math.sqrt(deaths) / 45))
  }

  function resetSimulation() {
    if (simulation) {
      simulation.stop()
    }

    nodes = (graph?.nodes || []).map(d => ({ ...d }))
    links = (graph?.links || []).map(d => ({ ...d }))

    if (!nodes.length || !width || !height) {
      return
    }

    simulation = forceSimulation(nodes)
      .force(
        "link",
        forceLink(links)
          .id(d => d.id)
          .distance(d => (d.source?.side == d.target?.side ? 72 : 116))
          .strength(0.2)
      )
      .force("charge", forceManyBody().strength(-260))
      .force("collide", forceCollide(d => nodeRadius(d) + 7))
      .force("center", forceCenter(width / 2, height / 2))
      .on("tick", () => {
        nodes = nodes
        links = links
      })
  }

  $: if (graph && width && height) {
    resetSimulation()
  }

  $: timeframe = selectedWar
    ? selectedWar.ongoing_conflict
      ? `${selectedWar.start_year}-Present`
      : selectedWar.start_year == selectedWar.end_year
        ? selectedWar.start_year
        : `${selectedWar.start_year}-${selectedWar.end_year}`
    : ""

  onDestroy(() => {
    if (simulation) {
      simulation.stop()
    }
  })
</script>

<section class="min-h-[620px] border border-[#d2d7d3] bg-[#fbfcf9]" bind:clientWidth={width}>
  <div class="flex flex-col gap-1 border-b border-[#d2d7d3] bg-white px-4 py-3 sm:flex-row sm:items-end sm:justify-between">
    <div>
      <h2 class="text-lg font-extrabold">{selectedWar?.war_name || "Select a war"}</h2>
      <p class="text-sm text-[#60706a]">{selectedWar ? `${selectedWar.war_type} | ${timeframe}` : ""}</p>
    </div>
    {#if selectedWar}
      <div class="flex gap-3 text-sm font-bold text-[#344348]">
        <span>{selectedWar.total_participants} participants</span>
        <span>{selectedWar.total_dyads} dyads</span>
      </div>
    {/if}
  </div>

  {#if nodes.length}
    <div class="relative">
      <svg class="block w-full touch-none" {height} viewBox="0 0 {width} {height}" role="img">
        <rect width={width} height={height} fill="#fbfcf9" />
        <g>
          {#each links as link, i (i)}
            <line
              x1={link.source?.x || 0}
              y1={link.source?.y || 0}
              x2={link.target?.x || 0}
              y2={link.target?.y || 0}
              stroke="#8a948f"
              stroke-opacity="0.45"
              stroke-width="1.4"
            />
          {/each}
        </g>
        <g role="list">
          {#each nodes as node (node.id)}
            <g
              class="cursor-default"
              role="listitem"
              transform="translate({node.x || width / 2}, {node.y || height / 2})"
              on:mouseenter={() => (hoverNode = node)}
              on:mouseleave={() => (hoverNode = null)}
            >
              <circle
                r={nodeRadius(node)}
                fill={sideColors[node.side]}
                stroke={hoverNode?.id == node.id ? "#111827" : "white"}
                stroke-width={hoverNode?.id == node.id ? 3 : 1.5}
              />
              {#if hoverNode?.id == node.id}
                <text class="pointer-events-none text-[12px] font-bold" x={nodeRadius(node) + 8} y="4" fill="#111827">
                  {node.participant}
                </text>
              {/if}
            </g>
          {/each}
        </g>
      </svg>

      {#if hoverNode}
        <div class="absolute left-4 top-4 max-w-xs border border-[#c4cec8] bg-white px-3 py-2 text-sm shadow-sm">
          <div class="font-extrabold">{hoverNode.participant}</div>
          <div class="mt-1 text-[#50615b]">Side {hoverNode.side || "unknown"}</div>
          <div class="text-[#50615b]">Battle deaths: {Number(hoverNode.battle_deaths || 0).toLocaleString()}</div>
        </div>
      {/if}
    </div>
  {:else}
    <div class="flex h-[560px] items-center justify-center px-6 text-center text-[#60706a]">
      No graph rows are available for the current selection.
    </div>
  {/if}
</section>
