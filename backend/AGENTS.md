# Backend Coding Preferences

## Backend Python

- Format Python from `backend` with the repository's Black and isort settings. Avoid non-functional trailing commas that make Black preserve unnecessary multiline layouts; when an existing tuple, list, call, or assertion would fit under the configured line length, remove the non-functional trailing comma so Black can collapse it. Keep commas that are semantically required or improve readability.
- Prefer guard-clause early exits with bare `return` when ending a no-op path improves readability. Do not write `return None`; use bare `return` for early no-value exits, and omit blank `return` statements at the natural end of a function.
- When backend code directly imports a runtime package, declare that package explicitly in `backend/pyproject.toml` rather than relying on transitive dependencies.

## Pipeline Structure

- Keep transformations as native DuckDB SQL in `.sql` files instead of embedding transformation SQL or Python transformation functions in pipeline code.
- Keep numbered pipeline-stage SQL filenames as a numeric sequence without letter suffixes. Use separate numbered files for tightly related create/insert pairs.
- When adding or splitting out a stage materialized table, add it as its own `create or replace table ... as` SQL file and update the stage SQL manifest, inspection SQL, README table lists/assumptions, and any tests that refer to the old table shape.
- If the pipeline pre-drops existing relations before `create or replace`, apply that behavior generically to all table/view targets detected from the SQL file. Do not add one-off Python special cases for individual SQL files or relation names.
- Before removing filters, joins, or selected columns after the reference-table steps, compare the affected stage deliverables and downstream tables or otherwise verify that the pipeline keeps the intended behavior.

## Data Modeling

- Keep table grain explicit and separated by purpose: base output tables hold base rows, yearly expansion tables hold one row per entity-year or dyad-year, and final dyad tables use stable canonical dyad keys with one row per unordered dyad.
- Do not add row identifiers unless a downstream transformation explicitly needs them.
- Store open-ended lookup/reference data that is expected to grow independently in `backend/manual/*.json`, such as participant name replacements and source metadata. Keep small static reference data such as `war_types` inline in SQL, with table creation and row insertion in separate numbered reference files.
- Keep related columns grouped consistently in source and transformed select lists. For dyadic A/B fields, put the same field for A and B adjacent before moving to the next field: for example, `c_code_a`, `c_code_b`, then `country_name_a`, `country_name_b`, rather than interleaving all A fields before all B fields. Keep repeated date components in source order by span and component. For resolved date-span fields, keep `start_date`, `end_date`, `start_year`, and `end_year` together in that order. Keep battle-death columns at the end with actual deaths before estimated values and estimate flags.
- Prefer stable source identifiers for manual mappings and generated values after validating that they identify the intended rows. Use synthetic/manual IDs only when needed, keep them explicit and deterministic, and document the source-data gap or deviation in the README.

## SQL Style

- For SQL `insert into ... values` statements, omit the target column list when inserting into tables created immediately nearby with an obvious column order, and collapse small inline `values` inserts to one row per tuple when that remains readable.
- Prefer compact DuckDB SQL idioms: concise aliases without `as` unless the grammar requires it, postfix casts, unquoted identifiers unless required, and `group by`-based row deduplication instead of `select distinct`. Quote aliases that are required by downstream graph semantics or reserved-word handling, such as `year`, `source`, and `target`, without `as` when DuckDB accepts that form, such as `clean_int(year) "year"`. Aggregate forms such as `count(distinct ...)` are acceptable when distinctness belongs inside the aggregate.
- Do not use a table alias in SQL queries that read from only one relation. Add aliases when the query joins multiple relations or otherwise needs them for disambiguation.
- Use sequential single-letter table aliases in SQL joins: `a`, `b`, `c`, `d`, and so on. Avoid mnemonic or suffix aliases such as `cc`, `dy`, `x`, `y`, or `z`.
- For multi-line join predicates, keep the first predicate on the join line and align each subsequent `and` under the
  `on` keyword:
  ```sql
  left join table_b b on a.id = b.id
                      and a.year = b.year
  ```
- In `where` and `having` boolean predicate lists, keep leading `and` or `or` on the same line as the predicate it introduces. Do not leave a boolean operator alone on its own line.
- In numbered pipeline-stage SQL union blocks, order branches by the stage's source/table construction order. When a source or table contributes mirrored A/B branches, put the original non-flipped branch before the flipped branch for that same source or table.
- Choose `union all` for additive source stacking when later logic handles deduplication or duplicates are meaningful. Use plain `union` only when set semantics are required at that exact point. Do not write `union distinct`.
- Avoid ordering tables or query results unless deterministic output order is explicitly needed. In tests, compare unordered results in Python unless the query prints or asserts on raw rows, where a deterministic `order by` makes diagnostics stable.
- Avoid CTEs when a direct query is clearer, but prefer a CTE over a derived-table subselect when one of those shapes is needed. Do not use derived-table subselects in `from` or `join` clauses.
- Prefer explicit `left join ... where matched_key is null` anti-joins over `not exists` filters in numbered pipeline-stage SQL. Keep anti-join `null` checks for `left join` patterns in the `where` clause, not inside the `on` clause; for inner joins, placing row filters in `on` is acceptable when it improves locality and does not obscure the join keys.
- Prefer simple conditional expressions: use `if(...)` instead of a `case` statement with only one `when`, avoid nested `case` statements, and keep `if`, `least`, and `greatest` column calculations on one line even when the line is long.

## Source Ingestion And Adjustments

- Keep raw source A/B, role, and row-position fields distinct from transformed side semantics. Preserve source role columns with clear aliases such as `role_a`, `role_b`, `dyad_role_a`, and `dyad_role_b`; derive transformed fields such as `side_a` and `side_b` only in downstream transformations that explicitly define side membership.
- When a source row has a COW `c_code` that resolves through `country_codes`, use `country_codes.state_name` as the participant name before applying participant name replacements. Use `participant_name_replacements` only for source names that do not resolve through a COW code, such as non-state participants or uncoded manual rows.
- Treat `backend/data` as pipeline-owned downloaded source state. Name source subdirectories with the source key directly, such as `backend/data/interstate_mid_dyads/`, without a `source_` prefix; the default data-preparation behavior should create or refresh only missing source subdirectories, and explicit recreate options should be required when deleting and rebuilding the entire data directory.
- Treat source subdirectory names as corresponding to source data table keys without the `source_` prefix; the raw data
  and PDF or JSON source documentation for that table belong in the matching folder.
- Source ingestion should mirror raw CSVs with explicit schemas and selections: write source table column definitions by hand, avoid `read_csv_auto` in `create table` statements, explicitly select loaded columns in insert files, and ingest all relevant CSV columns unless they are documented as calculated or intentionally excluded. Keep `source_` tables as direct CSV ingestion; cross-source enrichment, metadata, mappings, side overrides, and derived convenience fields belong in later transformation SQL or source adjustment tables.
- Keep source facts in the layer that owns them: documented data-entry fixes present in CSVs stay in source inserts, absent source facts belong in version-scoped source adjustment tables, and small tightly scoped rows can stay inline in SQL when they are part of transformation logic.
- Keep source adjustments version-aware and lean: tie them to the applicable CSV/source version, reassess them when replacing a CSV, store only keys and values needed by downstream joins, and document rationale/facts in the README instead of adding narrative columns.
- Do not add placeholder or convenience values to adjustment tables. Add an adjustment value only when it is used for a
  join, a source correction, or a downstream transformation; derive defaults in transformation SQL instead.
- When upgrading a source CSV version, compare the previous and new CSV columns before changing ingestion. Keep currently ingested columns that still exist, remove columns that are truly absent instead of fabricating `null` source columns, and document any dropped or newly available columns in the README.

## Documentation

- Keep explanatory comments out of SQL unless they are needed to understand non-obvious logic. Put assumptions, examples, and rationale in the README instead.

## Tests

- Keep test module names short and target-oriented while preserving pytest discovery, such as `test_pipeline.py`, `test_step_1.py`, and `test_step_2.py`. Keep stage data-quality and transformation expectations in stage-specific test files; reserve pipeline behavior tests for `test_pipeline.py`.
- Prefer simple, direct test code over broad abstractions or helper layers unless the duplication is clearly painful.
- In tests, define multi-line SQL in a named variable before passing it to `conn.execute` or helper functions. Do not embed triple-quoted SQL directly inside execute calls.
- Stage expectation tests should query created pipeline tables when checking ingested or transformed behavior. Read raw CSV files directly only for pre-ingestion checks, such as source metadata, download conversion, or validating source files before a table exists.
- Avoid CTEs in test SQL when a direct join or filtered query is clearer.
- Do not remove, skip, or allowlist away known data-quality failures just to make the suite pass. Expected failures, including missing source relationships such as unresolved MID war numbers, should remain visible until an explicit source-data, source-adjustment, or transformation fix resolves them.
- Tests should protect data semantics and pipeline behavior at the layer that owns them rather than freezing metadata placeholders or treating source row-position fields as transformed participant-side semantics.
- Diagnostic SQL should be focused and readable: select only columns needed to identify failing rows, avoid unnecessary subselect wrappers, use simple `count(*)` checks plus focused detail queries for broad source-data quality checks, and prefer loops or named helpers over dense inline repeated SQL fragments.
- For tests that expect no bad rows, query the unexpected rows with identifying columns instead of asserting on `count(*) = 0`; use the shared SQL-check failure helpers when the full SQL and formatted "Detected rows" table are useful, and use `assert rows == []` only for compact checks where pytest's abbreviated diff is enough. Keep scalar count assertions for aggregate totals, positive existence checks, or intentionally numeric invariants.
- Diagnostic failures and assertions should be self-contained and data-forward: show the SQL query, concise failure summary, and detected rows without Python traceback/code-frame noise. Avoid decorative or noisy styled formatting, but keep targeted problem-cell highlighting, such as existing `colorama` styling, when it makes detected rows easier to read. Fetch and assert on raw unexpected rows when useful, and prefer named SQL variables with compact scalar assertions.
