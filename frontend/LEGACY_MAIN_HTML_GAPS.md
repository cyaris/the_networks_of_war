# Legacy `main.html` Gaps

This file tracks behavior that existed in the root `main.html` legacy D3 implementation and is not currently rewritten in
the Svelte frontend under `frontend/src`.

## Known Bug From Legacy Sizing

- `main.html` assigned missing node-size descriptor values to the average non-`null` descriptor input. In Greek
  Independence War, the `Greeks` node has `urban_population_z = "nan"`, so choosing `Urban Population` made an unknown
  value appear as a medium-sized node with only a `?` marker. The Svelte graph now keeps the `?` marker but renders
  missing selected descriptor values at the minimum radius so unknown data is not visually overstated.

## User-Facing Legacy Behavior Not Rewritten

- The standalone introductory message above the menu is not present. The Svelte app uses the shared app shell header
  instead.
- The detailed inline data-source list from `#list_of_data_sources` is not present in the Svelte frontend. Source
  documentation lives mainly in the project README and backend metadata.
- The legacy desktop-only/mobile-hidden behavior is not present. `main.html` hid the graph, menu, message, and data
  sources below 750px and showed a desktop prompt instead.
- The legacy war-type-specific entry flow is not present. `main.html` inferred a war type from the page URL and rendered
  one table-style menu for that type; the Svelte app uses a war-type multi-select and a grouped war selector.
- The graph's back-to-menu button is not present. In Svelte, the selector remains visible above the graph.
- The D3 slider widget is not used. The Svelte graph uses a select control for `First Year`, `Last Year`, and
  `All Years`.
- The hover-built descriptor dropdown rectangles are not present. Svelte uses shared `Select` components for node sizes
  and link dashes.
- The blue `i` tooltip symbols beside legacy descriptor controls are not present.
- The legacy node tooltip text was descriptor-focused and positioned relative to SVG transforms. The Svelte tooltip is a
  simplified absolute-positioned panel with participant, side, battle deaths, and the selected descriptor value.

## Implementation Helpers Not Rewritten Directly

- The ad hoc SVG helpers from `main.html` (`addStdText`, `addStdRect`, `addStdSVG`, `defineTooltip`,
  `addTooltipSymbol`) are not carried over. Svelte markup and shared components replace them.
- The custom array helpers (`Array.prototype.contains`, `Array.prototype.uniqueNonNull`) are not carried over.
- Dummy-SVG text measurement is not carried over. `NetworkGraph.svelte` uses a simple text-width estimate for label
  placement.
- The legacy page-level D3 CSV/JSON loading flow is not carried over. The Svelte frontend consumes the generated
  `frontend/src/lib/static/graphData.json` payload.
- The legacy direct DOM cleanup pattern (`d3.selectAll('svg').remove()` and tooltip removal) is not carried over.
  Component lifecycle now owns graph setup and teardown.
