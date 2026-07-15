# Frontend Guidance

## Shared Svelte Conventions

- Use `../../svelte-lib/AGENTS.md` as the source of truth for shared Svelte formatting, config, lint, dependency, D3, Vite, Rollup, CSS import, and scoped embedded styling conventions.

## Local Dependencies

- Keep `linklocal` and local `file:` dependencies in `package.json`; sibling workspace packages such as `svelte-lib` should use `file:../../...` paths.

## Routing And Hosting

- Use `/the_networks_of_war` as the simulated GitHub Pages route base. The forecasting tool itself belongs on `src/routes/tool/+page.svelte`.

## Chart Data Derivations

- Keep `NetworkGraph.svelte` focused on project-specific data, chart state, and markup. Move generic reusable rendering helpers to `svelte-lib` and import them from `svelte-lib/functions` or `svelte-lib/components`.
- Keep sorted/parsed time-series rows and indexed observed/forecast row lists as shared derived data instead of rebuilding them inside layout or pointer-driven reactive blocks.
- Moving-average helpers must preserve row alignment and treat valid `0` values as data. Use finite-value checks rather than truthiness filters for chart paths, points, domains, and trends.
- Cache hover-derived comparative series and model metrics by stable inputs such as prediction column, moving-average window, year, and timeframe.

## Chart Layout

- Avoid deriving SVG plot dimensions from the component's own `clientHeight` when that can create circular initial-render sizing. Prefer viewport-based sizing or explicit constraints, and ensure SVG width/height/rect dimensions cannot become negative.

## Embedded Build

- Run `npm run rollup` from `frontend` when changes must affect the Jekyll-rendered bundle; the artifacts are `dist/bundle.js` and `dist/bundle.css`.
