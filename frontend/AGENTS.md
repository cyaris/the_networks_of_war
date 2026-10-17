# Frontend Guidance

## Routing And Hosting

- Use `/the_networks_of_war` as the simulated GitHub Pages route base. The network graph itself belongs on `src/routes/tool/+page.svelte`.

## Chart Data Derivations

- Compute node sizing per descriptor field as a value-and-domain pair (`getNodeDescriptiveValues`): give nodes with no finite value a shared fallback radius rather than dropping them, and feed the resulting max domain into the radius scale rather than recomputing it inline.
- Filter node/link descriptor fields down to selectable options only when they have at least one positive finite value, real sizing variation across nodes, and fewer than half their nodes `null`; keep this filtering in the shared descriptor-items derivation instead of the select markup.
- Derive hover tooltip metric rows (`nodeMetricRows`) from the hovered node's current-timeframe metrics, filtering out zero values unless the field is explicitly allow-listed to always show, and excluding the field currently driving node sizing.

## Embedded Build

- The bundle outputs are `dist/bundle.js` and `dist/bundle.css`.

# Inherited Rules

Copied from `../../svelte-lib/AGENTS.md` by `sync-agents.js`. Edit them there, then run
`node ../../shared-automation/.github/scripts/sync-agents.js` from this repository's root.

<!-- inherit: svelte-lib/shared-code -->
## Shared Code

- Treat `svelte-lib` as the place for generic reusable Svelte components, functions, and patterns extracted from project work. Place code according to the abstraction itself, not its current consumer count: a general primitive may belong here with one consumer, while application- or visualization-specific behavior belongs downstream even when several callers happen to use it. Prefer composing shared primitives in the consuming repository over moving domain semantics into the library.
- Keep `svelte-lib` free of anything proprietary to a consuming product, so the library could be licensed or published on its own terms without exposing a product: no domain logic, product or provider names, data, branding, artwork, or credentials, and no mechanism its owner treats as proprietary. Accept only conventional, generic code, write its comments, tests, examples, README, and commit messages without product or provider names, and leave anything whose generality is in doubt in the consumer until the owner decides.
- When a requested change touches something rendered or computed by shared code, whether a library primitive or a consuming repository's own shared component, helper, data-boundary function, or one entry of a data-driven list, change the shared code so every consumer picks it up. Do not copy the shared markup or logic into a one-off for the place that was mentioned; if the request genuinely applies to one consumer only, add a prop, option, or data field to the shared code rather than a fork. Prefer a generic prop over project-specific hard-coding.
- Consolidate shared code as soon as it is written, in the same change, rather than leaving a second copy for a later cleanup. When new code would repeat something that already exists in the repository, or two places would now hold the same thing, extract it and make both use it: a constant table, request helper, name or identity normalizer, or matching routine that two data-boundary modules need belongs in one module both import, with each external provider owning one module rather than spreading its constants and helpers across the callers; repeated markup, such as a page header, an icon badge, a primary button, or a selectable card's class pair, becomes one component or one class helper; and a list of near-identical elements, such as FAQ entries, navigation links, or option cards, is rendered from a data array through one `{#each}` block instead of hand-copied blocks. Give the shared piece props or data fields for the ways its consumers differ instead of keeping a diverging copy for one of them.
- When a repository inherits these conventions, copy each topic that applies to it into the `AGENTS.md` nearest the code it governs through inherit markers, and list only the conventions that genuinely differ. Do not restate or paraphrase a rule a topic file already owns: a downstream copy drifts from the original wording over time and obscures which file is authoritative, so the rule ends up enforced in two places that disagree.
- Make a change that belongs in `svelte-lib` there, never as a workaround in a consumer. Commit and push it to
  `svelte-lib`'s `dev` first, with its tests and README, rebuild its `dist`, then change the consumer to use it, since a
  consumer's `dev` CI builds against `svelte-lib`'s `dev` and its `main` against `svelte-lib`'s `main`. Leave merging
  `svelte-lib` to `main` to the owner, and say when a consumer change needs that merge before it can reach production.
<!-- /inherit: svelte-lib/shared-code -->

<!-- inherit: svelte-lib/javascript -->
## JavaScript

These rules add to the Code Style topic.

- Prefer functional array methods (`.map`, `.filter`, `.reduce`, `.forEach`, and similar) over manual `for`/`while` loops for traversals and transformations, consistent with the codebase's existing functional style. Use an imperative loop only when it is clearly simpler or an early exit/performance need makes it a better fit, and prefer `for...of` over an indexed `for` loop that uses the index only to access the current element. In performance-sensitive rendering and animation paths, prioritize measured performance over stylistic modernization.
- Let the shared Prettier config, with `trailingComma: "none"` and `objectWrap: "collapse"`, decide wrapping, and limit condensing only by its `printWidth`. Remove trailing commas that only force simple objects, calls, arrays, commands, or Svelte markup attributes multiline, and collapse simple object literals, return values, parenthesized expressions, and arrow callback bodies that fit, such as `return { x, y, width: bounds.width, height: bounds.height }`.
- Prefer concise conditional expressions for simple side-effect-free value selection or normalization in returns and variable definitions, such as `return value === "true" ? true : value === "false" ? false : value` or `let label = active ? "Active" : "Inactive"`. Keep guard returns and `if` blocks when they represent distinct cases, validation, error handling, repeated work avoidance, or multi-step logic.
- Group variable declarations within a scope by conceptual role, such as styling and geometry, control state, or application state, and keep each role contiguous. Keep a standalone declaration group near the code that owns or first uses it when that improves locality, rather than hoisting it merely to collect variables. Do not split a cohesive conceptual group merely because its members are first referenced at different points. Within a role, keep `const` and `let` declarations in separate blocks with a single blank line between them rather than interleaving declaration kinds. Preserve dependency, initialization, reactivity, binding, and source-order requirements when they require a different placement.
- Use the real boolean literals `true`/`false` for actual boolean values in code. Only write the quoted strings `"true"`/`"false"` when the value is inherently string-typed by its target or source: an HTML/ARIA attribute value (e.g. `aria-hidden="true"`, since ARIA's boolean-like attributes are spec'd as string enums, not boolean HTML attributes), a DOM `dataset` entry (`HTMLElement.dataset` is a `DOMStringMap` that coerces any assigned value to a string regardless), or a comparison/normalization against external input that may literally arrive as the string `"true"`/`"false"` (config values, serialized attributes, `localStorage`, URL params), matching the parsing pattern above.
- Keep reusable utilities in `src/lib/functions/` split by durable concern rather than in a broad helper bucket. Prefer focused modules such as `textFormatting.js`, `date.js`, `dom.js`, `color.js`, and `geometry.js`.
<!-- /inherit: svelte-lib/javascript -->

<!-- inherit: svelte-lib/svelte-components -->
## Svelte Components

### Component Structure

- Key an `{#each}` block only when the list can change during the component's life — items added, removed, or reordered — where Svelte needs the key to preserve the right node and its state. Leave a list that is built once and never mutated unkeyed: the key is inert, and writing one implies the list is dynamic when it is not. The shared ESLint config turns `svelte/require-each-key` off for exactly this case.
- Key by a value that identifies the item, not by its position. `(i)` on an index, or a key whose value is the item's own index, reproduces the index matching Svelte already does unkeyed, so it reads as identity-preserving reconciliation while providing none.
- Keep text a person reads as words or figures at `12px` (`text-xs`) or larger, including secondary lines, captions,
  table figures, help text, and a list's own content. Size a list's heading at least as large as the entries beneath
  it so the heading leads, and set content rather than a caption at the body size. A short label that stands alone in
  a pill, badge, or tile beside larger text, such as a status pill, a count badge, or a navigation label, may use
  `11px` (`text-[0.6875rem]`). Go smaller only for a mark that repeats information available as text, such as a sort
  arrow or a projection triangle with an accessible label, and never for words or figures.
- For any component that exposes a `wrapperClasses` prop, prefer Tailwind utilities through that existing
  API, including descendant arbitrary variants, over adding styling-only component props or internal style
  state. Add a new prop only when it controls behavior or when the styling cannot be reliably expressed
  through `wrapperClasses`.
- Do not pass fixed/default markup values through helper functions when they can be declared directly in markup.
- When multiple sibling variables represent the same concept across a repeated or keyed UI surface, prefer one dictionary keyed by that surface over parallel standalone variables. For example, use `selectNoItemsMessage.timeframe`, `selectNoItemsMessage.nodeDescriptor`, and `selectNoItemsMessage.linkDescriptor` instead of separate `noTimeframeItemsMessage`, `noNodeSizeItemsMessage`, and `noLinkDashItemsMessage` constants. Keep variables separate when the values do not share a durable key set or grouping them would obscure behavior.
- Keep one control's state in simple variables whose names identify the control concept, such as `selectItems`,
  `selectValue`, or `sliderValue`. When several related controls share a durable key set, group each kind of state in an
  object keyed by control name, such as `selectItems.timeframe`, `selectValue.timeframe`, or
  `sliderValue.movingAverageWindow`. Preserve the value type each component expects: selected item objects for `Select`
  and primitives for `Slider`.
- Pass Svelte props as expressions without quotes whenever the value type allows it, such as `radius={1}` instead of `radius="1"`.
- Inline trivial lifecycle callbacks and event handlers when they only assign simple state, including callbacks repeated across a lifecycle registration and Svelte markup. Apply the general single-use guidance to nontrivial handlers: retain a named handler only when its complexity or another concrete reason makes the separation materially useful.
- Declare numbered variables, tokens, and similarly named definitions in ascending numeric order so related values are easy to scan. Do not reorder them around an artificial dependency when each definition can derive directly from the same shared reference; depart from numeric order only when a genuine dependency requires it.
- Write `on:event` handlers as explicit function references or arrow functions, such as `on:click={() => toggleParent(d)}`, rather than a bare call expression like `on:click={toggleParent(d)}`. Svelte 4 only defers a bare call expression to click-time when it can prove every referenced variable is reactive; when it can't, the call runs once at mount instead of on each event, with no compiler warning. An explicit arrow wrapper behaves the same either way and stays correct through refactors that change a variable's reactivity classification.
- Keep the shared focus treatment consistent for native buttons and links and elements with `role="button"` or `role="link"`: pointer or click activation must not leave a visible focus indicator, while keyboard Tab focus must show the shared `ui-focus` outline. Use the global `:focus-visible` treatment instead of adding component-specific focus rings. A clicked control keeps focus, and any later key press, even a screenshot shortcut, makes the browser show the ring on it, so each app calls `releasePointerFocus` from `functions/dom.js` once at its root to blur pointer-generated activation, and the shared stylesheet clears the mobile tap highlight.
- Give enabled links, buttons, and elements with `role="button"` or `role="link"` a pointer cursor whenever their
  component does not already provide one, and keep disabled controls visually distinct with a non-pointer disabled
  cursor.
- Make any element with both a minimum height and padding `box-border`, so the minimum is its whole height rather than
  its content's. A button is already border-box.
- When an anchor opens a new browsing context with `target="_blank"`, pair it with `rel="noopener noreferrer"` so the opened page cannot retain access to the originating window and referrer behavior stays explicit.
- Do not mutate exported Svelte props to control internal component lifecycle. Use local component state, such as mount or one-shot launch flags, for internal gating.
- In Svelte components, order helpers, functions, classes, and derived values before their first practical use where Svelte reactivity allows it. This enforces `no-use-before-define` behavior for `.svelte` files through this convention rather than ESLint, since ESLint's rule produces false positives for component-scope variables in `.svelte` files. During AGENTS alignment, inspect `.svelte` files for call-before-declaration cases and move declarations upward when that does not change behavior.
- Place a component's `export let` prop declarations immediately after its imports, before other component-scope variables, helpers, or logic.
- A store passed to `Select` as `loadItemsStore` must use the same options-object request contract as `createAPIStore`:
  `request({ query, urlParams, body, method })`. Publish the request body as `params` with each result so `Select` can
  reject stale searches, and publish the current page in `data`; `Select` owns accumulation and deduplicates later pages
  by `itemIdentifier`. A boolean `hasMore` is optional but preferred when the source knows whether another page exists.
- Keep a component's own logic inside its `.svelte` file. Do not add a sibling module named after the component, such as
  `carousel.js` beside `Carousel.svelte` or `horizontalBarChart.js` beside `HorizontalBarChart.svelte`, to hold helpers
  that only that component uses. Unit-testability is not a reason to create one: a sidecar covers only the fragment that
  was pulled out, leaves the component behavior it came from untested, and splits one component's logic across two files
  that then drift. Reusable logic belongs in `src/lib/functions/` under a durable concern module per the JavaScript topic, and
  everything else stays in the component, inlined at its call site per the Code Style topic.
- Honor the viewer's reduced-motion preference through `prefersReducedMotion` from `functions/dom.js` in every
  JavaScript-driven transition and animation.

### Styling

- Prefer the fewest DOM elements and CSS styling rules/classes that clearly express the layout and behavior, including Tailwind utilities, component style blocks, inline styles, and global selectors. Avoid adding wrapper `div`s, responsive variants, utility classes, selectors, or declarations unless they solve a concrete structure, spacing, sizing, state, or breakpoint need; remove them when the same result is already provided by an existing parent, child, component prop, inherited style, or browser default. This applies to markup a change leaves behind as much as to markup it adds: when removing or moving an element leaves its wrapper holding a single child, or makes a layout utility on it, such as `justify-between`, `flex-1`, or `items-end`, do nothing, collapse the wrapper into its child or parent and drop the dead utilities in the same change, and position a lone absolutely placed control by its own classes rather than through a wrapper.
- Treat Tailwind utilities as the first choice for static layout, spacing, sizing, typography, and wrapping behavior,
  over component `<style>` blocks, CSS sections, or inline styles. If a Tailwind fix fails, adjust the utility choice
  or markup structure first, including responsive grid placement and arbitrary values such as `basis-[max-content]`.
  Use a component `<style>` block only when Tailwind cannot express the selector, state, or value clearly, do not create
  custom classes solely as styling hooks for static CSS, and reserve Tailwind `!` modifiers for real external or
  host-page CSS override needs.
- Render a genuinely repeated or dynamic list of near-identical elements from a data array through one `{#each}` block,
  per the Shared Code topic, but keep a small fixed set explicit when a loop would need per-item conditionals or styling that
  make it harder to read.

### Mobile Layout

- Do not hide interactive apps behind a desktop-only message unless the experience genuinely cannot be made usable. Prefer a full-width mobile shell with compact spacing, stacked controls, and preserved desktop behavior at the existing desktop breakpoint.
- On mobile, prefer stacking controls, fitting summaries/tables within the viewport, and shortening overly tall chart canvases. Keep horizontal scrolling only inside the chart surface when it is part of the core visualization workflow, such as a long time-series axis; avoid page-level horizontal scrolling.
- Keep responsive Svelte app geometry stable across browser-only zoom changes while still responding to real window
  resizes. Use shared `observeZoomStableViewport` for resize lifecycle management, or `createZoomStableViewport` when
  the caller already owns that lifecycle, if a resize can otherwise feed zoomed viewport dimensions back into an app's
  layout or Canvas coordinate system; continue updating the current device pixel ratio so Canvas rendering remains
  sharp.

### Color Tokens

- Use opaque colors derived from the semantic parent surface for visible UI text, borders, disabled states, hover states,
  and selection states so their appearance does not change when an ancestor background changes. Prefer live
  `color-mix()` values based on tokens such as `--ui-surface`, or runtime contrast calculation when readability requires
  choosing a foreground from the computed background. Reserve alpha colors and Tailwind opacity utilities for intentional
  compositing, including visibility, fades, shadows, and visualization layers that are meant to reveal content underneath.
- When a hard-coded hexadecimal color is the same value as a global CSS custom property from
  `src/lib/static/styles/root.css`, use the custom property instead of duplicating the literal. In direct component
  styling, keep a concrete fallback when scoped embedded apps need it, such as `var(--ui-text, #33413f)`.
- Use `ui-text` for normal UI and chart text:
  - This includes Select values, searchable input text, ordinary option text, and tooltip text; reserve `ui-control`
    for control chrome and icons rather than body copy.
  - Axis labels and titles
  - Forecast or reference labels
  - Legend labels and empty-state symbols
- Use the shared semantic `ui-*` and `chart-*` Tailwind classes and CSS custom properties for colors owned by
  `svelte-lib`, and the `brand`, `link`, `success`, `ui-focus`, and `ui-selection` roles for shared accents and
  interaction states. Do not recreate numbered palettes, such as the removed `theme-*` palette, `themegray-*` aliases,
  or generic gray/slate/neutral substitutions for shared roles. Retain a built-in utility only when the color is
  genuinely local to the consuming application.
- Preserve specialized text roles for muted, disabled, inverse, and control text. Preserve data-encoding colors for chart marks and legend swatches.
- Host-specific palette integration belongs in the consuming repository at the embedded app root. Map the host's palette variables to the shared semantic tokens there, using CSS variable references rather than resolved color values so runtime palette changes continue to update already rolled-up bundles.
<!-- /inherit: svelte-lib/svelte-components -->

<!-- inherit: svelte-lib/packages-and-config -->
## Packages And Config

### Shared Consumer Config

- Keep reusable build behavior in shared config exports. The shared Vite helper owns importing `@sveltejs/kit/vite`, keeps linked local `svelte-lib` development working, and preserves app-level server overrides; consuming apps should call exported helpers such as `createViteConfig()` and `createRollupConfig({ scopeClass })` instead of carrying project-local copies of shared setup. A plain Vite single-page app with no SvelteKit routes calls `createViteConfig({ kit: false })` rather than forking the config to drop the SvelteKit plugin. Keep every local shared-config file, including build, lint, format, and styling config, a thin wrapper around the package export. Add an app-local override only for a durable app-specific need, and fix a shared config issue in `svelte-lib` rather than replacing the thin re-export.
- Keep a consumer's `tailwindConfigPath` on a CommonJS file. `createRollupConfig` loads it with `require()`, so an ESM
  config either throws on a runtime that cannot require it, or arrives as a namespace object whose configuration sits
  under `default`. Merging that namespace keeps the shared baseline intact and silently ignores every consumer
  customization instead of failing, so the bundle builds and only the consumer's own tokens go missing.

### Package Imports

- Import shared components, charts, icons, functions, and stores from the narrowest public `svelte-lib` package subpath that owns them. Do not import from aggregate barrels such as `svelte-lib/components`, `svelte-lib/components/charts`, `svelte-lib/components/charts/cards`, `svelte-lib/components/icons`, or `svelte-lib/functions`; the shared ESLint config enforces these consumer-facing boundaries because loading one shared primitive must not evaluate unrelated primitives or optional heavy dependencies. Apply the same rule to imports between modules inside `svelte-lib`, while retaining aggregate exports only as a compatibility surface for external consumers that have not migrated yet.
- Consuming app Rollup entry files should import shared CSS and other package assets through package exports, not through `node_modules/svelte-lib/src/...` source paths. Exports must point only to files included in the published or packed output.
- Consuming Rollup-bundled code should avoid SvelteKit-only aliases such as `$lib` unless the shared Rollup config explicitly supports them. Prefer relative imports for package/library code and embedded bundle entries.

### Dependency Ownership

- Keep package dependencies owned by the package that imports them: `svelte-lib` should list only packages it imports directly or needs for exported shared config/build/lint/format/package/check tooling, while app-only runtime dependencies belong in the consuming app's `package.json`.
- When a package uses D3 functionality, depend on and import its specific `d3-*` subpackages rather than the umbrella `d3` package.
- Every consuming repository needs its own `.npmrc` containing exactly `engine-strict=true` and `install-links=true`. npm reads `.npmrc` only from the project directory, the user home directory, and the global config, so this cannot be inherited from `svelte-lib` and has to be duplicated per repo. `install-links=true` is the load-bearing setting: it makes npm materialize the `file:` dependency as a real package so `svelte-lib`'s dependencies and their `.bin` entries land in the consumer's tree. Under `install-links=false` npm symlinks instead, nothing is hoisted, and the consumer is forced to redeclare shared build, lint, format, and check tooling it should be inheriting.
- Consuming repositories must not declare a `devDependencies` block for tooling `svelte-lib` already provides. With `install-links=true` that tooling is inherited, and redeclaring it duplicates version pins across every consumer and lets them drift from the library. A consumer that appears to need its own tooling has an `.npmrc` or lockfile problem to fix, not a dependency to add.
- Do not add `resolution-mode` to an `.npmrc`. It is a pnpm setting; npm reports it as an unknown project config and warns that it will stop working in the next major version.
- Keep link-local tooling and relative `file:` dependency specs aligned with the sibling checkouts. After `svelte-lib`'s
  dependency metadata changes, reinstall or refresh the linked dependency and the consumer's lockfile before
  validating it.
<!-- /inherit: svelte-lib/packages-and-config -->

<!-- inherit: svelte-lib/embedded-bundles -->
## Embedded Bundles

- Consuming embedded apps should keep Rollup entrypoints small and explicit: create the scoped app wrapper, mount the app component, and document app-specific entrypoint names or wrapper classes locally only when they are not obvious from the code.
- `mountEmbeddedRoot` adds the shared `svelte-lib-app` class alongside each consumer-specific class. Host palette mappings that apply to every embedded app should target that shared root instead of enumerating consumers.
- Components rendered inside Rollup-scoped embedded apps must not depend on resolving theme variables from `document.body` or global `:root` for direct visual styling. The shared Rollup config can scope `:root` variables onto the app wrapper, so prefer CSS custom properties directly in DOM/CSS values with concrete fallbacks, such as `var(--ui-surface-alt, #f2f2f2)`. When a JavaScript rendering API requires a resolved color, pass the mounted component or app root to `getCSSCustomProperty` instead of reading from `document.body`.
- Tooltip portal behavior should support scoped embedded apps via `[data-svelte-lib-tooltip-root]`; keep the default tooltip root inside the closest app container so scoped styles continue to apply, and avoid solving scoped Tailwind portal issues with broad global CSS.
- Tooltip and overlay positioning must work inside embedded pages with scroll. Prefer document coordinates such as `pageX`/`pageY` with scroll-aware clamping and stable inline positioning over viewport-only `clientX`/`clientY` plus `position: fixed`.
- Run the package's `rollup` script when a change must reach the hosted bundle, and check the bundle outputs its
  `AGENTS.md` documents.
<!-- /inherit: svelte-lib/embedded-bundles -->

<!-- inherit: svelte-lib/jekyll-hosting -->
## Jekyll Hosting

- Mirror the GitHub Pages paths an embedded app is hosted under with source-app routing. Keep route components under
  `src/routes`, and use `src/routes/+page.svelte` as the app navigation hub unless the app has a durable reason for a
  different structure. Derive each route's path variants from one helper rather than repeating the project slug in
  every `Route` declaration: the extensionless route, the Jekyll `.html` shell page, and, for the project home route,
  the directory and index forms. For example, derive `/my_project/tool` and `/my_project/tool.html`, and `/my_project`,
  `/my_project/`, and `/my_project/index.html`.
- Make a shell page's bundle prefix configurable through a shared include or site variable rather than hard-coding
  staging and production paths in each page, since the shared Rollup workflow names live and staged `dev_bundle.*`
  uploads by branch. Leave a public production config's prefix blank unless staged bundles are intentionally under
  review on the live shell route.
- Keep Jekyll shell pages lightweight: front matter, page-specific host layout/full-width options, the compiled bundle
  CSS link, and the compiled bundle script tag. Change behavior, routes, and app-scoped styles in the source Svelte
  app, then rebuild and upload its bundle. Never patch a generated bundle or duplicate route content in Jekyll.
- A bundle's scoped Tailwind CSS applies only below its app wrapper. Style the Jekyll page around it with page, inline,
  or global site CSS.
- Do not use manually configured S3 asset version variables in Jekyll shell pages. Query-string cache busters are fine
  when a shared include derives them automatically from Jekyll build metadata such as `site.time`, or at runtime when
  a development config enables a strategy such as `s3_bundle_cache_bust_strategy: runtime`. Keep automatic cache busting scoped to
  mutable bundle assets by default, provide a config switch between runtime and build-time strategies, and make upload tooling set
  `Cache-Control: no-cache, max-age=0, must-revalidate` so direct S3 visits still revalidate same-name uploads.
- When a Rollup app renders differently inside a Jekyll host, compare computed styles and element geometry at the same viewport to identify the winning host declarations. Keep intrinsic component layout and spacing in the source app; compensate only for host-introduced cascade differences with narrowly page-scoped Jekyll CSS, preserving explicitly accepted host effects such as borders or alternating row colors. When existing Tailwind classes or other source-app CSS declarations serve only to counter Jekyll rendering differences, identify them during related work, move that compensation to the narrowly scoped Jekyll CSS, and remove the obsolete source styling after verifying both render paths. Verify the standalone and hosted geometry after the override rather than copying broad reset rules into either repository.
- Project home content embedded above Jekyll project pagination should reserve bottom space in the source app, matching the existing `mb-8` route or root wrapper convention rather than relying on host-page pager margins.
<!-- /inherit: svelte-lib/jekyll-hosting -->

<!-- inherit: svelte-lib/charts -->
## Charts

### Canvas Rendering And D3

- For dense or frequently animated graphics where Canvas is the appropriate renderer, prefer native Canvas 2D for rendering and animated rendering rather than representing individual graphical objects as SVG/DOM elements.
- Prefer the flow `data -> D3/layout/animation calculations -> render-ready data -> Canvas drawing`. Use D3 where it provides an appropriate abstraction for scales, interpolation, easing, layouts, force simulations, data transformations, or similar visualization math rather than reimplementing that functionality in raw JavaScript. Use D3 utilities directly with Canvas when they support a Canvas context; D3 does not imply SVG rendering.
- Avoid DOM-oriented `d3-selection`/`d3-transition` patterns for dense Canvas-rendered graphics. Transitions should generally interpolate numerical/application state and render the resulting state to Canvas.
- Keep renderers simple and declarative. Separate computation, state, animation, interaction, and drawing when doing so materially improves clarity, testing, or reuse, while applying the applicable single-use guidance: calculation logic used by only one Canvas drawing function may live directly in that function when it remains clear and manageable. Name geometry and extract semantic drawing primitives only when the name or abstraction adds material value.
- Keep `requestAnimationFrame` callbacks small by delegating substantial state updates, calculations, and drawing to appropriately scoped named functions, and make animations time-based rather than frame-count-based. This callback boundary does not require each delegated operation to be split into additional single-use helpers.
- Pause timer-driven animation launches and repeating effect cadence while the document is hidden. Use `createPausableTimer` instead of raw `setTimeout`, `setInterval`, or `d3-timer` scheduling for animation work so hidden time preserves the remaining delay and returning to the page does not replay accumulated callbacks. Audit RAF-only paths separately; browsers pause RAF for hidden documents, and those paths need no timer replacement when a resumed timestamp gap cannot enqueue missed work.
- Contain temporary Canvas state with `context.save()`/`context.restore()` when drawing changes transforms or shared context properties.
- Use D3 color scales/interpolation where applicable while preserving application-defined palettes/colors as the source of truth.
- Do not use D3 merely for the sake of using D3; simple application-specific calculations should remain ordinary JavaScript when that is clearer.
- Do not introduce a Canvas framework or retained-mode scene graph when native Canvas plus appropriate D3 utilities is sufficient, and do not add abstractions that merely rename Canvas APIs without semantic value.
- Preserve meaningful rendering performance. Isolate and document a less-readable hot-path optimization when profiling justifies it rather than sacrificing performance solely for abstraction.
- Reusable Canvas mechanics (context/DPR setup, resize handling, `requestAnimationFrame` and visibility-aware timer lifecycle, elapsed-time/progress helpers, generic primitive drawing) belong in `svelte-lib`. Application/domain-specific rendering behavior, such as chart- or scene-specific drawing and motion, belongs in the consuming repository.
- Prefer a d3-shape line/curve generator's `.context(canvasContext)` mode to draw directly to a Canvas 2D context instead of building and re-parsing an SVG `d` string every frame.
- Canvas has no CSS `:hover`. To hit-test a stroked line/curve, build a `Path2D` from the same d3-shape generator and use `context.isPointInStroke(path2D, x, y)` against the tracked pointer position rather than reimplementing point-to-curve distance math.
- Canvas rasterizes sub-device-pixel strokes unevenly across browsers (Safari in particular can render the same fractional-width line anywhere from near-invisible to roughly double its requested width depending on position). Snap stroke geometry to the device pixel grid with `snapToDevicePixel` before drawing a line that needs to look the same weight everywhere.
- When two adjacent shapes share a boundary, draw that boundary once rather than letting each shape stroke its own edge; a shared edge stroked from both sides renders roughly twice as heavy as an edge stroked from only one side.
- A circle stroked with `context.arc` + `context.stroke()` renders as a hollow ring whenever its diameter is smaller than its stroke width, because the stroke's inner edge falls past the center instead of closing over it. Use `strokeOrFillCircle` for a circle whose radius may shrink below half its stroke width so it fills solid instead of hollowing out.
- Keep canvas/SVG helper names tied to the primitive or behavior they provide rather than to project-specific chart layer names.

### Responsive Charts

- Use mobile-first sizing for embedded chart/tool shells: full width below desktop, and preserve established desktop widths at `min-[1300px]` when a project already uses that as its large-screen breakpoint.
- For chart internals, use `640px` as the compact mobile cutoff, `900px` as the point where desktop chart behavior should usually return, and `1300px` as the large-shell breakpoint unless local project conventions require different values. Keep desktop-affecting scales, force layouts, and dimensions unchanged at or above the applicable desktop cutoff.
- For responsive charts, derive render-ready dimensions and density from the active viewport mode while preserving
  established desktop geometry at or above the desktop cutoff. Keep breakpoint selection and generic layout mechanics
  shared; keep chart-specific domains, labels, marks, and interaction semantics in the consuming application.

### Chart Colors

- When de-emphasizing chart marks relative to a semantic surface, keep the rendered colors opaque and use
  `color-mix(in srgb, <mark-color> 50%, var(--ui-surface))`, matching filtered legend marks. Keep active marks and any
  interaction-defined neighborhood at their original colors. Do not use element opacity when the intended result is a
  stable surface-relative color rather than revealing content underneath.
- Use `chart-line` for fully opaque neutral chart geometry:
  - Default it to `ui-border` so chart geometry keeps the same quiet neutral weight as control borders.
  - Axes and ticks
  - Forecast or reference dividers
  - Neutral outlines and outer frames
- Use the opaque `chart-band` and `chart-band-subtle` roles for plot-region shading. Derive their default colors from `chart-line` and `ui-surface` instead of recreating them with element opacity or fixed palette utilities.
<!-- /inherit: svelte-lib/charts -->

<!-- inherit: svelte-lib/web-pages -->
## Web Pages

### Search Metadata

- Keep public search metadata truthful and consistent with visible content, including page titles, descriptions,
  canonical URLs, social previews, web app manifests, and structured data. Give every indexable page a distinct title,
  a description of what it shows, and its own indexing decision. Keep account, contact, error, settings,
  navigation-hub, and duplicate-variant pages out of search unless their purpose changes.
- Never add hidden, transparent, off-screen, zero-size, or `sr-only` copy for search engines. `sr-only` text remains for
  accessibility labels and equivalents. Search copy must be visible to an ordinary visitor in the same state, though
  state-based personalization is allowed. An embedded app's search copy belongs in its source repository, not its host
  page.

### User-Facing Copy

- Translate failures at every user-facing boundary into concise language that says what failed, what the viewer can do
  next, and any genuinely useful alternative. Never display raw exception messages, stack traces, selector names,
  database or infrastructure details, provider payloads, or unexplained HTTP status codes. Preserve diagnostic detail
  only in credential-safe developer logs, and add regression coverage when a new external failure can reach the UI.
- Never put build, deployment, configuration, or rollout state in visible copy, such as "unavailable in this build",
  "not configured", "coming soon", or a feature flag's name. When a build lacks an optional integration, hide its
  controls and give the guidance a viewer could act on without it, as though the feature did not exist. Gate pre-launch
  states in code, never with visible placeholder text.
<!-- /inherit: svelte-lib/web-pages -->
