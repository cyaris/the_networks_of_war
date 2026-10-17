# Frontend Guidance

## Routing And Hosting

- Use `/the_networks_of_war` as the simulated GitHub Pages route base. The network graph itself belongs on
  `src/routes/tool/+page.svelte`.

## Chart Data Derivations

- Compute node sizing per descriptor field as a value-and-domain pair (`getNodeDescriptiveValues`): give nodes with no
  finite value a shared fallback radius rather than dropping them, and feed the resulting max domain into the radius
  scale rather than recomputing it inline.
- Filter node/link descriptor fields down to selectable options only when they have at least one positive finite
  value, real sizing variation across nodes, and fewer than half their nodes `null`; keep this filtering in the shared
  descriptor-items derivation instead of the select markup.
- Derive hover tooltip metric rows (`nodeMetricRows`) from the hovered node's current-timeframe metrics, filtering out
  zero values unless the field is explicitly allow-listed to always show, and excluding the field currently driving
  node sizing.

## Embedded Build

- The bundle outputs are `dist/bundle.js` and `dist/bundle.css`.

## Workspace Rule Files

- Before changing Canvas, D3, or chart code, read `../../shared-automation/agents/charts.md`.
- Before changing a Jekyll-hosted page, route, or bundle prefix, read
  `../../shared-automation/agents/jekyll-hosting.md`.
