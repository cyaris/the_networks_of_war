# Backend Coding Preferences

- For Python files, avoid non-functional trailing commas that cause Black to keep short calls, literals, or expressions split across extra lines.
- Prefer the most condensed layout that Black accepts.
- Keep commas that are functional, such as single-item tuples, or that preserve readability for intentionally tabular data structures.

## SQL And Data Layout

- Keep transformations as native DuckDB SQL in `.sql` files instead of embedding transformation SQL or Python transformation functions in pipeline code.
- Keep numbered pipeline-stage SQL filenames as a numeric sequence without letter suffixes. Use separate numbered files for tightly related create/insert pairs, such as `03_create_source_adjustment_tables.sql` and `04_insert_source_adjustments.sql`.
- Keep base compatibility tables separate from derived yearly-expansion tables. For example, `dyads` should hold the base dyad rows, while `dyad_years` should hold one row per dyad-year.
- When adding or splitting out a stage materialized table, add it as its own `create or replace table ... as` SQL file and update the stage SQL manifest, inspection SQL, README table lists/assumptions, and any tests that refer to the old table shape.
- If the pipeline pre-drops existing relations for `create or replace` compatibility, apply that behavior generically to all table/view targets detected from the SQL file. Do not add one-off Python special cases for individual SQL files or relation names.
- For source tables, write explicit `create table` column definitions. Do not use `read_csv_auto` in `create table` statements.
- Source-table insert files may use `read_csv_auto`, but should explicitly select the loaded columns and apply aliases, casts, cleaning macros, and documented source-file data-entry fixes there.
- Ingest all relevant CSV columns in the paired `01_create_source_tables.sql` / `02_insert_source_tables.sql` source-table files unless a column is documented as calculated from other ingested columns or is otherwise intentionally excluded. When treating fields as original source columns, cover only columns that exist in the ingested source files; do not add derived or convenience columns that were never present in the CSVs.
- `source_` tables should mean direct CSV ingestion. Do not join to other source or derived tables in `02_insert_source_tables.sql`; cross-source enrichment belongs in later transformation SQL.
- Keep source row-position fields distinct from transformed semantic fields. Do not materialize source-side columns that only restate A/B row position when existing source columns such as `c_code_a` and `c_code_b` already encode that position.
- Preserve source role fields as role fields with clear aliases, such as `role_a`, `role_b`, `dyad_role_a`, and `dyad_role_b`; do not repurpose source role fields as semantic side fields.
- Do not add row identifiers unless a downstream transformation explicitly needs them.
- Keep source facts in the layer that owns them: documented data-entry fixes present in CSVs stay in `02_insert_source_tables.sql`, absent source facts belong in source adjustment tables, and small tightly scoped rows can stay inline in SQL when they are part of transformation logic.
- Tie source adjustments to the applicable CSV/source version, prefer direct joins or filters against `source_file_versions` over intermediate `active_` views when that stays simpler, and reassess affected adjustment rows when replacing a CSV with a newer version.
- When upgrading a source CSV version, compare the previous and new CSV columns before changing ingestion. Keep currently ingested columns that still exist, remove columns that are truly absent from the new source instead of fabricating null source columns, and document any dropped or newly available columns in the README.
- For source adjustment SQL, keep table definitions in `03_create_source_adjustment_tables.sql` as `create table` statements and put row-loading logic in `04_insert_source_adjustments.sql`. Keep adjustment tables lean: store the keys and values needed by downstream joins, and document rationale/facts in the README instead of adding reason, citation, narrative, or generic `adjustment_id` columns.
- Do not insert documentation-only rows into `source_file_versions`.
- Do not insert metadata-only, mapping-only, or side-only adjustment rows into source-shaped CSV ingestion tables. Keep source tables row-shaped: for example, `source_interstate_wars` rows should remain participant rows with a real `c_code`, and `source_interstate_war_dyads` rows should remain dyad rows with real participant codes.
- Keep absent cross-source facts in version-scoped source adjustment tables and have downstream transformations join the active adjustment rows through `source_file_versions`. This applies to MID `disno` to `war_num` mappings, synthetic MID-only war metadata, and participant-side overrides.
- For MID-to-war assignments, prefer explicit `disno`-level source facts over broad transformation fallbacks such as assigning every unmatched pre-1946 MID row to World War II.
- Store open-ended lookup/reference data that is expected to grow independently in `backend/manual/*.json`, such as participant name replacements and source metadata. Keep small static reference data such as `war_types` inline in SQL, with table creation and row insertion in separate numbered reference files.
- Treat `backend/data` as pipeline-owned downloaded source state. Name source subdirectories with the source key directly, such as `backend/data/interstate_mid_dyads/`, without a `source_` prefix; the default data-preparation behavior should create or refresh only missing source subdirectories, and explicit recreate options should be required when deleting and rebuilding the entire data directory.
- For SQL `insert into ... values` statements, omit the target column list when inserting into tables created immediately nearby with an obvious column order, and collapse small inline `values` inserts to one row per tuple when that remains readable.
- Prefer compact SQL aliases such as `c_code_a c_code`; use `as` only when required by DuckDB or when it materially improves clarity.
- Prefer DuckDB postfix casts such as `value::integer` over `cast(value as integer)`.
- Remove unnecessary quoted SQL identifiers. Keep quotes only for source columns or aliases that require them.
- Do not use `select distinct`; write row deduplication as `group by` with numeric column positions, such as `group by 1` or `group by 1, 2`. Aggregate forms such as `count(distinct ...)` are acceptable when distinctness belongs inside the aggregate.
- Keep related columns grouped consistently in source and transformed select lists: opposite A/B columns adjacent, repeated date components in source order by span and component, and battle-death columns at the end with actual deaths before estimated values and estimate flags.
- In numbered pipeline-stage SQL union blocks, order branches by first-created primary table to last-created primary table. When a primary table contributes mirrored A/B branches, put the original non-flipped branch before the flipped branch for that same table.
- Choose `union all` for additive source stacking when later logic handles deduplication or duplicates are meaningful. Use plain `union` only when set semantics are required at that exact point. Do not write `union distinct`.
- Compute transformed interstate `war_dyads.side_a` and `war_dyads.side_b` from participant-side source data rather than from directed dyad row position. Extra-state and intra-state dyads may use literal A/B sides when the source table's A/B columns are the side definition.
- Keep interstate participant rows sourced from participant-level interstate war data. Do not use directed interstate dyad rows as participant rows when those rows carry dyad-level dates or deaths.
- Keep `lagging_war` and `leading_war` nullable when source data does not provide a value; do not coalesce them to sentinel values.
- Final `dyads` should contain one row per unordered dyad. Use stable canonical dyad keys for deduplication, and derive `wars.total_dyads` from the resulting dyad row count rather than dividing a directed-row count by two.
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
- Use standard Markdown links in README documentation. Do not rely on raw HTML anchors with `target="_blank"` because common Markdown renderers may sanitize or ignore those attributes.
- Split long README assumption sections into small, scannable subsections rather than maintaining one long bullet list.
- Document synthetic/manual IDs and other source-data deviations in the README, including the source gap that made the manual value necessary.

## Tests

- Keep stage data-quality and transformation expectations in stage-specific expectation test files, such as `tests/test_step_1_expectations.py`; reserve `tests/test_pipeline_query_file.py` for pipeline query-file behavior.
- Prefer simple, direct test code over broad abstractions or helper layers unless the duplication is clearly painful.
- Do not remove, skip, or allowlist away known data-quality failures just to make the suite pass. Expected failures, including missing source relationships such as unresolved MID war numbers, should remain visible until an explicit source-data, source-adjustment, or transformation fix resolves them.
- Tests should protect data semantics and pipeline behavior at the layer that owns them rather than freezing metadata placeholders or treating source row-position fields as transformed participant-side semantics.
- Diagnostic failure SQL should select only the columns needed to identify and understand failing rows, and should avoid subselect wrappers unless they are necessary for the query shape. Pytest diagnostic failures should show each check as its own self-contained block with the SQL query, a concise failure summary, and detected rows, while avoiding Python traceback/code-frame noise for intentional data-quality failures.
- Do not add Python helpers whose only purpose is to reformat SQL strings for logging. Logged SQL should generally appear as authored in the test query string, while preserving existing color handling.
- Do not include synthetic `table_name` columns in detected-row output when the table or check is already identified by the diagnostic label or failure summary.
- For scalar assertions, prefer assigning the SQL to a named variable and using a compact assertion such as `assert scalar(conn, query) == 0` over deeply nested multi-line `assert (scalar(...))` formatting.
- When logging source-row diagnostics, omit raw date-part columns after resolved `start_date` and `end_date` are shown. Keep `source_year` when it helps identify the source record.
- For broad source-data quality checks, prefer a simple `count(*)` filtered to the failure condition, then run a separate focused query to show flagged rows when the count is nonzero. Avoid large cross-table `union all` diagnostic queries when a loop over table/column checks is clearer, and combine symmetric A/B fields into one diagnostic check when they represent one logical expectation.
- When a test is intended to catch unexpected rows, fetch and assert on the raw rows rather than only asserting on `count(*)`, so failures show the offending data.
- Keep diagnostic failure output plain and concise. Avoid formatting choices that make pytest render large failure tables as visually noisy styled blocks.
- For tests that inspect comparable columns across many source tables, use one cross-table allowlist and include a column only when it exists in the inspected table.
- When tests generate repeated SQL fragments, prefer a named local helper or explicit loop over dense inline f-string comprehensions.
