# Backend Coding Preferences

- For Python files, avoid non-functional trailing commas that cause Black to keep short calls, literals, or expressions split across extra lines.
- Prefer the most condensed layout that Black accepts.
- Keep commas that are functional, such as single-item tuples, or that preserve readability for intentionally tabular data structures.

## SQL And Data Layout

- Keep SQL statements in `.sql` files instead of embedding transformation SQL in Python.
- Prefer native DuckDB SQL transformations over Python transformation functions.
- Keep numbered pipeline-stage SQL filenames as a numeric sequence without letter suffixes. Use separate numbered files for tightly related create/insert pairs, such as `03_create_source_adjustment_tables.sql` and `04_insert_source_adjustments.sql`.
- Keep base compatibility tables separate from derived yearly-expansion tables. For example, `dyads` should hold the base dyad rows, while `dyad_years` should hold one row per dyad-year.
- When adding or splitting out a stage materialized table, add it as its own `create or replace table ... as` SQL file and update the stage SQL manifest, inspection SQL, README table lists/assumptions, and any tests that refer to the old table shape.
- If the pipeline pre-drops existing relations for `create or replace` compatibility, apply that behavior generically to all table/view targets detected from the SQL file. Do not add one-off Python special cases for individual SQL files or relation names.
- For source tables, write explicit `create table` column definitions. Do not use `read_csv_auto` in `create table` statements.
- Source-table insert files may use `read_csv_auto`, but should explicitly select the loaded columns and apply aliases, casts, cleaning macros, and documented source-file data-entry fixes there.
- Ingest all relevant CSV columns in the paired `01_create_source_tables.sql` / `02_insert_source_tables.sql` source-table files unless a column is documented as calculated from other ingested columns or is otherwise intentionally excluded.
- `source_` tables should mean direct CSV ingestion. Do not join to other source or derived tables in `02_insert_source_tables.sql`; cross-source enrichment belongs in later transformation SQL.
- Keep source row-position fields distinct from transformed semantic fields. Do not materialize source-side columns that only restate A/B row position when existing source columns such as `c_code_a` and `c_code_b` already encode that position.
- Preserve source role fields as role fields with clear aliases, such as `role_a`, `role_b`, `dyad_role_a`, and `dyad_role_b`; do not repurpose source role fields as semantic side fields.
- Do not add row identifiers unless a downstream transformation explicitly needs them.
- Keep small, tightly scoped source adjustments inline in SQL when the rows are part of the transformation logic.
- Keep documented source data-entry fixes that are applied during ingestion in `02_insert_source_tables.sql`; do not move them into source adjustment tables merely to normalize all manual fixes.
- Use source adjustment tables for source facts that are absent from the CSVs, not for values already present in the source data.
- Avoid adding intermediate `active_` views just to filter version-scoped source adjustments. Prefer direct joins or filters against `source_file_versions` when that keeps the SQL simpler.
- Tie source adjustments to the applicable CSV/source version. When replacing a CSV with a newer version, reassess the relevant source adjustment rows instead of leaving old version-specific facts silently active.
- When upgrading a source CSV version, compare the previous and new CSV columns before changing ingestion. Keep currently ingested columns that still exist, remove columns that are truly absent from the new source instead of fabricating null source columns, and document any dropped or newly available columns in the README.
- For source adjustment SQL, keep table definitions in `03_create_source_adjustment_tables.sql` as `create table` statements and put row-loading logic in `04_insert_source_adjustments.sql`.
- Do not insert documentation-only rows into `source_file_versions`.
- Do not insert metadata-only, mapping-only, or side-only adjustment rows into source-shaped CSV ingestion tables. Keep source tables row-shaped: for example, `source_interstate_wars` rows should remain participant rows with a real `c_code`, and `source_interstate_war_dyads` rows should remain dyad rows with real participant codes.
- Keep absent cross-source facts in version-scoped source adjustment tables and have downstream transformations join the active adjustment rows through `source_file_versions`. This applies to MID `disno` to `war_num` mappings, synthetic MID-only war metadata, and participant-side overrides.
- For MID-to-war assignments, prefer explicit `disno`-level source facts over broad transformation fallbacks such as assigning every unmatched pre-1946 MID row to World War II.
- Keep source adjustment tables lean: store the keys and values needed by downstream joins, and document rationale/facts in the README instead of adding reason, citation, or narrative columns. Do not add generic `adjustment_id` columns unless a downstream relationship explicitly needs them.
- Keep open-ended lookup/reference data in `backend/manual/*.json` when it is expected to grow or change independently, such as participant name replacements and source metadata. Do not move participant name replacements back to inline SQL solely for consistency with smaller static reference tables.
- Name `backend/data` source subdirectories with the source key directly, such as `backend/data/interstate_mid_dyads/`, without a `source_` prefix. `source_` remains the table-name prefix, not the source-data directory prefix.
- Treat `backend/data` as pipeline-owned downloaded source state. The default data-preparation behavior should create or refresh only missing source subdirectories; use an explicit recreate option when deleting and rebuilding the entire data directory is intended.
- Keep small static reference data such as `war_types` inline in SQL, with table creation and row insertion in separate numbered reference files.
- For SQL `insert into ... values` statements, omit the target column list when inserting into tables created immediately nearby with an obvious column order.
- Collapse small inline `values` inserts to one row per tuple when that remains readable.
- Prefer compact SQL aliases such as `c_code_a c_code`; use `as` only when required by DuckDB or when it materially improves clarity.
- Prefer DuckDB postfix casts such as `value::integer` over `cast(value as integer)`.
- Remove unnecessary quoted SQL identifiers. Keep quotes only for source columns or aliases that require them.
- Do not use `select distinct`; write row deduplication as `group by` with numeric column positions, such as `group by 1` or `group by 1, 2`. Aggregate forms such as `count(distinct ...)` are acceptable when distinctness belongs inside the aggregate.
- Keep opposite columns adjacent, such as `c_code_a` immediately followed by `c_code_b` and `participant_a` immediately followed by `participant_b`.
- Keep battle-death columns at the end of source and transformed select lists. When both actual and estimated/estimate-flag battle-death columns are present, put the actual battle-death columns first and the estimated/estimate-flag columns immediately after them.
- In numbered pipeline-stage SQL union blocks, order branches by first-created primary table to last-created primary table. When a primary table contributes mirrored A/B branches, put the original non-flipped branch before the flipped branch for that same table.
- Choose `union all` for additive source stacking when later logic handles deduplication or duplicates are meaningful. Use plain `union` only when set semantics are required at that exact point. Do not write `union distinct`.
- For repeated date components, keep fields in source order by span and component: `start_day_1`, `start_month_1`, `start_year_1`, then `start_day_2`, `start_month_2`, `start_year_2`, and likewise for end dates.
- Compute transformed interstate `war_dyads.side_a` and `war_dyads.side_b` from participant-side source data rather than from directed dyad row position. Extra-state and intra-state dyads may use literal A/B sides when the source table's A/B columns are the side definition.
- Keep interstate participant rows sourced from participant-level interstate war data. Do not use directed interstate dyad rows as participant rows when those rows carry dyad-level dates or deaths.
- Keep `lagging_war` and `leading_war` nullable when source data does not provide a value; do not coalesce them to sentinel values.
- Final `dyads` should contain one row per unordered dyad. Use stable canonical dyad keys for deduplication, and derive `wars.total_dyads` from the resulting dyad row count rather than dividing a directed-row count by two.
- Avoid ordering tables unless deterministic output order is explicitly needed.
- Avoid CTEs when a direct query is clearer. Minimize CTEs when doing so does not duplicate substantial joins or unions. When the tradeoff is between a CTE and a subselect/derived table, prefer the CTE.
- Do not use derived-table subselects in `from` or `join` clauses.
- Prefer explicit `left join ... where matched_key is null` anti-joins over `not exists` filters in numbered pipeline-stage SQL.
- Keep anti-join null checks for `left join` patterns in the `where` clause, not inside the `on` clause. For inner joins, placing row filters in `on` is acceptable when it improves locality and does not obscure the join keys.
- Prefer `if(...)` over a `case` statement with only one `when`.
- Keep `if`, `least`, and `greatest` column calculations on one line, even when the line is long.
- Avoid nested `case` statements.
- When manual mappings can be expressed by a stable source identifier instead of enumerating country-code pairs, prefer the source identifier after validating that it yields the same rows.
- Keep synthetic or fake IDs explicit and deterministic. Prefer meaningful source identifiers when available, and document why the source data lacks the needed ID.
- Before removing filters, joins, or selected columns after the reference-table steps, compare the affected stage deliverables and downstream tables or otherwise verify that the pipeline keeps the intended behavior.
- Avoid subselect wrappers in test failure SQL unless they are necessary for the query shape.
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
- Do not remove, skip, or allowlist away known data-quality failures just to make the suite pass. Expected failures should remain visible until the source data or transformation logic is fixed.
- Tests for missing source relationships, such as unresolved MID war numbers, should fail in a way that requires an explicit source-data or source-adjustment change.
- Tests should protect data semantics and pipeline behavior rather than freezing metadata placeholders. For example, do not test that a source remains `unversioned`.
- Test semantic behavior at the layer that owns it. Do not test source row-position fields as if they were transformed participant-side semantics.
- Tests about original source columns should only cover columns that exist in the source files being ingested; do not add derived or convenience columns that were never present in the CSVs.
- For diagnostic failure SQL, select only the columns needed to identify and understand the failing rows.
- Pytest diagnostic failures should show what failed, the SQL queries that were run, and the detected rows. Avoid Python traceback/code-frame noise for intentional data-quality failures.
- Structure each diagnostic failure as its own self-contained block instead of grouping many failures under broad sections. Within a block, show the SQL query first, then a concise failure summary, then detected rows.
- Do not add Python helpers whose only purpose is to reformat SQL strings for logging. Logged SQL should generally appear as authored in the test query string, while preserving existing color handling.
- Do not include synthetic `table_name` columns in detected-row output when the table or check is already identified by the diagnostic label or failure summary.
- For scalar assertions, prefer assigning the SQL to a named variable and using a compact assertion such as `assert scalar(conn, query) == 0` over deeply nested multi-line `assert (scalar(...))` formatting.
- When logging source-row diagnostics, omit raw date-part columns after resolved `start_date` and `end_date` are shown. Keep `source_year` when it helps identify the source record.
- For broad source-data quality checks, prefer a simple `count(*)` filtered to the failure condition, then run a separate focused query to show flagged rows when the count is nonzero. Avoid large cross-table `union all` diagnostic queries when a loop over table/column checks is clearer.
- When symmetric A/B fields in the same source table represent one logical expectation, such as paired battle-death fields, prefer one combined diagnostic check over separate A and B failures.
- When a test is intended to catch unexpected rows, fetch and assert on the raw rows rather than only asserting on `count(*)`, so failures show the offending data.
- Add deterministic `order by` clauses to diagnostic failure queries that print or assert on raw rows. This is an exception to avoiding `order by` when row order is not itself behavior.
- Keep diagnostic failure output plain and concise. Avoid formatting choices that make pytest render large failure tables as visually noisy styled blocks.
- For tests that inspect comparable columns across many source tables, use one cross-table allowlist and include a column only when it exists in the inspected table.
- When tests generate repeated SQL fragments, prefer a named local helper or explicit loop over dense inline f-string comprehensions.
- Avoid `order by` in tests when row order is not part of the behavior being tested; compare unordered results in Python instead.
