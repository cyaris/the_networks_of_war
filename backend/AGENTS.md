# Backend Coding Preferences

- Format Python from `backend` with the repository's Black and isort settings. Avoid non-functional trailing commas that make Black preserve unnecessary multiline layouts, while keeping commas that are semantically required or improve readability.
- When backend code directly imports a runtime package, declare that package explicitly in `backend/pyproject.toml` rather than relying on transitive dependencies.

## SQL And Data Layout

- Keep transformations as native DuckDB SQL in `.sql` files instead of embedding transformation SQL or Python transformation functions in pipeline code.
- Keep numbered pipeline-stage SQL filenames as a numeric sequence without letter suffixes. Use separate numbered files for tightly related create/insert pairs.
- Keep base compatibility tables separate from derived yearly-expansion tables. For example, `dyads` should hold the base dyad rows, while `dyad_years` should hold one row per dyad-year.
- When adding or splitting out a stage materialized table, add it as its own `create or replace table ... as` SQL file and update the stage SQL manifest, inspection SQL, README table lists/assumptions, and any tests that refer to the old table shape.
- If the pipeline pre-drops existing relations for `create or replace` compatibility, apply that behavior generically to all table/view targets detected from the SQL file. Do not add one-off Python special cases for individual SQL files or relation names.
- Source ingestion should mirror raw CSVs with explicit schemas and selections: write source table column definitions by hand, avoid `read_csv_auto` in `create table` statements, explicitly select loaded columns in insert files, and ingest all relevant CSV columns unless they are documented as calculated or intentionally excluded. Keep `source_` tables as direct CSV ingestion; cross-source enrichment and derived convenience fields belong in later transformation SQL.
- Keep raw source A/B, role, and row-position fields distinct from transformed side semantics. Preserve source role columns with clear aliases such as `role_a`, `role_b`, `dyad_role_a`, and `dyad_role_b`; derive transformed fields such as `side_a` and `side_b` only in downstream transformations that explicitly define side membership.
- Do not add row identifiers unless a downstream transformation explicitly needs them.
- Keep source facts in the layer that owns them: documented data-entry fixes present in CSVs stay in source inserts, absent source facts belong in version-scoped source adjustment tables, and small tightly scoped rows can stay inline in SQL when they are part of transformation logic. Keep metadata, mappings, and side overrides out of source-shaped CSV ingestion tables.
- Keep source adjustments version-aware and lean: tie them to the applicable CSV/source version, reassess them when replacing a CSV, store only keys and values needed by downstream joins, and document rationale/facts in the README instead of adding narrative columns.
- When upgrading a source CSV version, compare the previous and new CSV columns before changing ingestion. Keep currently ingested columns that still exist, remove columns that are truly absent instead of fabricating null source columns, and document any dropped or newly available columns in the README.
- Store open-ended lookup/reference data that is expected to grow independently in `backend/manual/*.json`, such as participant name replacements and source metadata. Keep small static reference data such as `war_types` inline in SQL, with table creation and row insertion in separate numbered reference files.
- Treat `backend/data` as pipeline-owned downloaded source state. Name source subdirectories with the source key directly, such as `backend/data/interstate_mid_dyads/`, without a `source_` prefix; the default data-preparation behavior should create or refresh only missing source subdirectories, and explicit recreate options should be required when deleting and rebuilding the entire data directory.
- For SQL `insert into ... values` statements, omit the target column list when inserting into tables created immediately nearby with an obvious column order, and collapse small inline `values` inserts to one row per tuple when that remains readable.
- Prefer compact DuckDB SQL idioms: concise aliases, postfix casts, unquoted identifiers unless required, and `group by`-based row deduplication instead of `select distinct`. Aggregate forms such as `count(distinct ...)` are acceptable when distinctness belongs inside the aggregate.
- Keep related columns grouped consistently in source and transformed select lists: opposite A/B columns adjacent, repeated date components in source order by span and component, and battle-death columns at the end with actual deaths before estimated values and estimate flags.
- In numbered pipeline-stage SQL union blocks, order branches by first-created primary table to last-created primary table. When a primary table contributes mirrored A/B branches, put the original non-flipped branch before the flipped branch for that same table.
- Choose `union all` for additive source stacking when later logic handles deduplication or duplicates are meaningful. Use plain `union` only when set semantics are required at that exact point. Do not write `union distinct`.
- Final dyad tables should contain one row per unordered dyad and use stable canonical dyad keys for deduplication.
- Avoid ordering tables or query results unless deterministic output order is explicitly needed. In tests, compare unordered results in Python unless the query prints or asserts on raw rows, where a deterministic `order by` makes diagnostics stable.
- Avoid CTEs when a direct query is clearer, but prefer a CTE over a derived-table subselect when one of those shapes is needed. Do not use derived-table subselects in `from` or `join` clauses.
- Prefer explicit `left join ... where matched_key is null` anti-joins over `not exists` filters in numbered pipeline-stage SQL. Keep anti-join null checks for `left join` patterns in the `where` clause, not inside the `on` clause; for inner joins, placing row filters in `on` is acceptable when it improves locality and does not obscure the join keys.
- Prefer simple conditional expressions: use `if(...)` instead of a `case` statement with only one `when`, avoid nested `case` statements, and keep `if`, `least`, and `greatest` column calculations on one line even when the line is long.
- When manual mappings can be expressed by a stable source identifier instead of enumerating country-code pairs, prefer the source identifier after validating that it yields the same rows.
- Keep synthetic or fake IDs explicit and deterministic. Prefer meaningful source identifiers when available.
- Before removing filters, joins, or selected columns after the reference-table steps, compare the affected stage deliverables and downstream tables or otherwise verify that the pipeline keeps the intended behavior.
- Do not update `.ipynb` files while working on the backend replacement unless explicitly requested.

## Documentation

- Keep explanatory comments out of SQL unless they are needed to understand non-obvious logic. Put assumptions, examples, and rationale in the README instead.
- Keep the README pipeline command documentation in sync with `backend/src/pipeline.py` parser parameters, including compatibility aliases and accepted-but-not-yet-implemented `--step` values.
- For README links that intentionally open a new tab, use an HTML anchor with `target="_blank"` and `rel="noopener noreferrer"`.
- Split long README assumption sections into small, scannable subsections rather than maintaining one long bullet list.
- Document synthetic/manual IDs and other source-data deviations in the README, including the source gap that made the manual value necessary.

## Tests

- Keep stage data-quality and transformation expectations in stage-specific expectation test files; reserve pipeline query-file tests for pipeline query-file behavior.
- Prefer simple, direct test code over broad abstractions or helper layers unless the duplication is clearly painful.
- Do not remove, skip, or allowlist away known data-quality failures just to make the suite pass. Expected failures, including missing source relationships such as unresolved MID war numbers, should remain visible until an explicit source-data, source-adjustment, or transformation fix resolves them.
- Tests should protect data semantics and pipeline behavior at the layer that owns them rather than freezing metadata placeholders or treating source row-position fields as transformed participant-side semantics.
- Diagnostic SQL should be focused and readable: select only columns needed to identify failing rows, avoid unnecessary subselect wrappers, and use simple `count(*)` checks plus focused detail queries for broad source-data quality checks. Prefer loops or named helpers over dense inline repeated SQL fragments, and use cross-table allowlists only for columns that exist in inspected tables.
- Diagnostic failures should be plain and self-contained: show the SQL query, concise failure summary, and detected rows without Python traceback/code-frame noise, synthetic table-name columns when the check label already identifies the table, or noisy styled table formatting.
- Test assertions should expose offending data directly. Fetch and assert on raw unexpected rows when useful, and prefer named SQL variables with compact scalar assertions over deeply nested multi-line assertions.
