# Frontend Guidance

## Shared Svelte Conventions

- Use `../../svelte-lib/AGENTS.md` as the source of truth for shared Svelte formatting, config, lint, dependency, D3, Vite, Rollup, CSS import, and scoped embedded styling conventions.
- Keep short inline object literals compact when they are easier to scan in local graph logic. For example, prefer `let adjustedPoint = {x: getXAdjusted(dragNode.id, point.x),y: getYAdjusted(dragNode.id, point.y)}` over expanding the object across multiple lines.

## Local Dependencies

- Keep `linklocal` and local `file:` dependencies in `package.json`; sibling workspace packages such as `svelte-lib` should use `file:../../...` paths.

## Routing And Hosting

- Use `/the_networks_of_war` as the simulated GitHub Pages route base. The network graph itself belongs on `src/routes/tool/+page.svelte`.

## Chart Data Derivations

- Keep `Tool.svelte` focused on project-specific data, chart state, and markup. Move generic reusable rendering helpers to `svelte-lib` and import them from `svelte-lib/functions` or `svelte-lib/components`.
- Compute node sizing per descriptor field as a value-and-domain pair (`getNodeDescriptiveValues`): give nodes with no finite value a shared fallback radius rather than dropping them, and feed the resulting max domain into the radius scale rather than recomputing it inline.
- Filter node/link descriptor fields down to selectable options only when they have at least one positive finite value, real sizing variation across nodes, and fewer than half their nodes null; keep this filtering in the shared descriptor-items derivation instead of the select markup.
- Derive hover tooltip metric rows (`nodeMetricRows`) from the hovered node's current-timeframe metrics, filtering out zero values unless the field is explicitly allow-listed to always show, and excluding the field currently driving node sizing.

## Chart Layout

- Avoid deriving SVG plot dimensions from the component's own `clientHeight` when that can create circular initial-render sizing. Prefer viewport-based sizing or explicit constraints, and ensure SVG width/height/rect dimensions cannot become negative.

## Embedded Build

- Run `npm run rollup` from `frontend` when changes must affect the Jekyll-rendered bundle; the artifacts are `dist/bundle.js` and `dist/bundle.css`.
