# The Networks of War

A study of networks by war using data from the Correlates of War (COW) project.

## Table Of Contents

- [The Networks of War](#the-networks-of-war)
  - [Table Of Contents](#table-of-contents)
  - [Quickstart](#quickstart)
  - [Current Architecture](#current-architecture)
    - [Backend](#backend)
    - [Frontend](#frontend)
  - [Data Layout](#data-layout)
  - [Commands](#commands)
    - [Pipeline Commands](#pipeline-commands)
    - [Test Commands](#test-commands)
    - [Frontend Commands](#frontend-commands)
  - [Source Tables](#source-tables)
    - [Step 1 Source Tables](#step-1-source-tables)
    - [Step 2 Source Tables](#step-2-source-tables)
  - [Materialized Tables](#materialized-tables)
  - [Final Outputs](#final-outputs)
  - [Ingestion Assumptions](#ingestion-assumptions)
    - [Source Ingestion Rules](#source-ingestion-rules)
    - [Excluded Calculated Columns](#excluded-calculated-columns)
    - [Date Values](#date-values)
    - [Encoding And Deduplication](#encoding-and-deduplication)
    - [Field Normalization](#field-normalization)
  - [Transformation Assumptions](#transformation-assumptions)
    - [Table Shape](#table-shape)
    - [Source War Dyads And Participants](#source-war-dyads-and-participants)
    - [Date Spans](#date-spans)
    - [Directed Dyads And MID Records](#directed-dyads-and-mid-records)
    - [Participant Inference](#participant-inference)
    - [Dyads](#dyads)
  - [Data-Entry Fixes And Assignment Rules](#data-entry-fixes-and-assignment-rules)
  - [Embedded Build Artifacts](#embedded-build-artifacts)
  - [Maintainer Notes](#maintainer-notes)

## Quickstart

Install the backend and build the DuckDB database first. From `the_networks_of_war/backend`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python src/pipeline.py
```

Then install and run the frontend. From `the_networks_of_war/frontend`:

```bash
npm install
npm run data:build
npm run dev
```

`npm run data:build` reruns the full backend pipeline through `../backend/.venv/bin/python`, so it expects the backend
virtual environment and source data to be available.

## Current Architecture

### Backend

The DuckDB backend rebuilds preprocessing steps with native SQL. Python resolves file paths, prepares downloaded source
files, normalizes configured CSV encodings, and runs the SQL files in order.

### Frontend

The Svelte frontend lives in `frontend/`. It provides a routed Svelte app and a usable war browser backed by the Step 3
graph export.

In Vite development, the menu is available at `/` and `/the_networks_of_war`. The browser itself is available at
`/tool` and `/the_networks_of_war/tool`.

The frontend consumes ignored generated data at `frontend/src/lib/static/graphData.json`. Do not commit this file. Step
3 writes it from `backend/sql/step_3/04_export_frontend_graph_data.sql` after the final Step 3 tables are built.
Generated graph rows include only descriptor fields that pass per-war availability checks, so the frontend does not
receive fields that cannot be selected.

## Data Layout

Source data is downloaded into `backend/data/`, which is ignored by git. Each external source table gets its own
subdirectory named after the source key without the `source_` table prefix, such as
`backend/data/interstate_mid_dyads/` for `source_interstate_mid_dyads`. The corresponding raw source data and source
documentation live in that folder. Source download metadata lives in `backend/manual/source_metadata.json`. Source CSVs
that need explicit encoding handling use `latin-1` by default; prepared copies are written to UTF-8 under ignored
`backend/.work/` before DuckDB reads them. The generated DuckDB database is ignored:

Prepared source subdirectories keep only durable source CSVs and PDF or JSON source documentation. Archive files,
original Excel/Stata workbooks, text exports, and temporary download caches are discarded after extraction or conversion;
`_downloads/` is not part of the expected `backend/data/` layout.

- `the_networks_of_war/backend/data/`
- `the_networks_of_war/backend/.work/`
- `the_networks_of_war/backend/the_networks_of_war.duckdb`

## Commands

### Pipeline Commands

From `the_networks_of_war/backend`:

```bash
python src/pipeline.py
```

Pipeline parameters:

| Parameter | Default | Demonstration |
| --- | --- | --- |
| `--data-dir PATH` | `backend/data/` | Source-data directory. Use `--data-dir data` for the default relative backend path. |
| `--db-path PATH` | `backend/the_networks_of_war.duckdb` | DuckDB database path. Use `--db-path the_networks_of_war.duckdb` for the default relative backend path. |
| `--build`, `--no-build` | `--build` | `--build` runs Steps 1, 2, and 3; `--no-build` skips preprocessing so commands can inspect or query an existing database. |
| `--inspect` | off | Print table row counts after build completes, or immediately with `--no-build`. |
| `--prepare-data` | off | Download and validate missing source-data folders before opening the database. |
| `--recreate-data` | off | Delete and recreate the full source-data directory before opening the database. |
| `--query SQL` | none | Execute an inline SQL query after build completes, or immediately with `--no-build`. |
| `--query-file PATH` | none | Execute SQL read from a local `.sql` file after build completes, or immediately with `--no-build`. Mutually exclusive with `--query`. |

Run or rebuild all pipeline steps:

```bash
python src/pipeline.py
```

Run the full build explicitly:

```bash
python src/pipeline.py --build
```

Print table row counts after running the full build:

```bash
python src/pipeline.py --inspect
```

Query the existing DuckDB database without rebuilding it:

```bash
python src/pipeline.py --no-build --query "select count(*) as row_count from dyads"
python src/pipeline.py --no-build --query "select * from wars limit 10"
```

Query from a local SQL file:

```bash
python src/pipeline.py --no-build --query-file queries/war_counts.sql
```

Run the full build, then query the freshly rebuilt tables:

```bash
python src/pipeline.py --query "select war_id, war_name from wars limit 10"
```

Use non-default input or database paths:

```bash
python src/pipeline.py --data-dir data --db-path the_networks_of_war.duckdb
```

Create missing source-data subdirectories without running the build:

```bash
python src/pipeline.py --prepare-data --no-build
```

Recreate the full ignored source-data directory:

```bash
python src/pipeline.py --recreate-data --no-build
```

### Test Commands

From `the_networks_of_war/backend`:

```bash
pytest
```

Run the Step 1 expectation tests:

```bash
pytest tests/test_step_1.py
pytest tests/test_step_1.py -q
```

Run the Step 3 final-output tests:

```bash
pytest tests/test_step_3.py
```

Run a single test or matching group of tests:

```bash
pytest tests/test_step_1.py -k "negative_date_sentinels"
pytest tests/test_step_1.py -k "date_macros or dyads"
```

Show verbose test names and failures:

```bash
pytest tests/test_step_1.py -vv
```

The Step 1 expectation tests rebuild Step 1 into a temporary DuckDB database. They skip automatically if the ignored
source files in `backend/data/` are not available locally.

### Frontend Commands

From `the_networks_of_war/frontend`, regenerate the frontend data snapshot from an already-built backend database:

```bash
npm run data:build
```

Run frontend checks:

```bash
npm run check
npm run build
```

Build the embedded bundle for the legacy Jekyll-rendered surface when needed:

```bash
npm run rollup
```

## Source Tables

The current backend ingests the following source files. Downloaded source subdirectories include the relevant PDFs and
supporting files from each source bundle when available.

### Step 1 Source Tables

| Table | Organization | Source CSV | Version | Download source |
| --- | --- | --- | --- | --- |
| `source_country_codes` | Correlates of War Project (COW) | `COW-country-codes.csv` | unversioned | [Data](https://correlatesofwar.org/wp-content/uploads/COW-country-codes.csv) |
| `source_extrastate_wars` | Correlates of War Project (COW) | `Extra-StateWarData_v4.0.csv` | 4.0 | [Data](https://correlatesofwar.org/wp-content/uploads/Extra-StateWarData_v4.0.csv)<br>[Doc](https://correlatesofwar.org/wp-content/uploads/Extra-StateWars_Codebook.pdf) |
| `source_interstate_mid_dyads` | Correlates of War Project (COW) | `dyadic_mid_4.03.csv` | 4.03 | [Release](https://correlatesofwar.org/wp-content/uploads/dyadic_mid_4.03_update.zip) |
| `source_interstate_war_dyads` | Correlates of War Project (COW) | `directed_dyadic_war.csv` | unversioned | [Release](https://correlatesofwar.org/wp-content/uploads/Dyadic-Interstate-War-Dataset.zip) |
| `source_interstate_wars` | Correlates of War Project (COW) | `Inter-StateWarData_v4.0.csv` | 4.0 | [Data](https://correlatesofwar.org/wp-content/uploads/Inter-StateWarData_v4.0.csv)<br>[Doc 1](https://correlatesofwar.org/wp-content/uploads/Inter-StateWars_Codebook.pdf)<br>[Doc 2](https://correlatesofwar.org/wp-content/uploads/Inter-StateWarsList.pdf) |
| `source_intrastate_wars` | Correlates of War Project (COW) | `INTRA-STATE_State_participants v5.1 CSV.csv` | 5.1 | [Release](https://correlatesofwar.org/wp-content/uploads/Intra-State-Wars-v5.1.zip) |

### Step 2 Source Tables

| Table | Organization | Source CSV | Version | Download source |
| --- | --- | --- | --- | --- |
| `source_global_terrorism_database` | START | `globalterrorismdb_0522dist.csv`<br>`globalterrorismdb_2021Jan-June_1222dist.csv` | 0522 + 2021 Jan-June 1222 | [Data 1](https://www.start.umd.edu/system/files/globalterrorismdb_0522dist.xlsx)<br>[Data 2](https://www.start.umd.edu/system/files/globalterrorismdb_2021Jan-June_1222dist.xlsx)<br>[Doc](https://www.start.umd.edu/sites/default/files/2024-10/Codebook.pdf) |
| `source_formal_alliances_directed_yearly` | Correlates of War Project (COW) | `alliance_v4.1_by_directed_yearly.csv` | 4.1 | [Release](https://correlatesofwar.org/wp-content/uploads/version4.1_csv.zip) |
| `source_territorial_changes` | Correlates of War Project (COW) | `tc2018.csv` | 6 | [Release](https://correlatesofwar.org/wp-content/uploads/terr-changes-v6.zip) |
| `source_forcibly_displaced_populations` | United States Committee for Refugees and Immigrants (USCRI) | `FDP2008a.csv` | 2008a | [Data](http://www.systemicpeace.org/inscr/FDP2008a.xls)<br>[Doc](http://www.systemicpeace.org/inscr/FDPCodebook2008.pdf) |
| `source_colonial_dependency_contiguity` | Correlates of War Project (COW) | `contcold.csv` | 3.1 | [Release](https://correlatesofwar.org/wp-content/uploads/ColonialContiguity310.zip) |
| `source_direct_contiguity` | Correlates of War Project (COW) | `contdird.csv` | 3.2 | [Release](https://correlatesofwar.org/wp-content/uploads/DirectContiguity320.zip) |
| `source_defense_cooperation_agreements` | Correlates of War Project (COW) | `DCAD-v1.0-dyadic.csv` | 1.0 | [Release](https://correlatesofwar.org/wp-content/uploads/kinne_dca.zip) |
| `source_intergovernmental_organizations_dyadic` | Correlates of War Project (COW) | `dyadic_formatv3.csv` | 3 | [Data](https://correlatesofwar.org/wp-content/uploads/dyadic_formatv3.zip)<br>[Doc](https://correlatesofwar.org/wp-content/uploads/IGO-Codebook_v3_short-copy.pdf) |
| `source_diplomatic_exchange` | Correlates of War Project (COW) | `Diplomatic_Exchange_2006v1.csv` | 2006.1 | [Release](https://correlatesofwar.org/wp-content/uploads/Diplomatic_Exchange_2006.1.zip) |
| `source_dd_revisited` | University of Illinois at Urbana‐Champain (UIUC), Emory University, Georgetown University | `ddrevisited_data_v1.csv` | 1 | [Data](https://github.com/cyaris/the_networks_of_war/releases/download/source-data-dd-revisited-v1/ddrevisited_data_v1.csv)<br>[Doc](https://rforpoliticalscience.com/wp-content/uploads/2022/04/ddrevisited-codebook.pdf) |
| `source_co_emissions_per_capita` | Our World in Data | `co-emissions-per-capita.csv` | 1 | [Data](https://ourworldindata.org/grapher/co-emissions-per-capita.csv?v=1&csvType=full&useColumnShortNames=true)<br>[Doc](https://ourworldindata.org/grapher/co-emissions-per-capita.metadata.json?v=1&csvType=full&useColumnShortNames=true&utm_source=chatgpt.com) |
| `source_arms_technology` | Correlates of War Project (COW) | `cow_arms_tech_long.csv` | 1.1 | [Release](https://correlatesofwar.org/wp-content/uploads/Arms-TechnologyV1.1.zip) |
| `source_atop_dyadic_years` | ATOP Project | `atop5_1ddyr.csv` | 5.1 | [Data](http://www.atopdata.org/uploads/6/9/1/3/69134503/atop_5.1__.csv_.zip)<br>[Doc](http://www.atopdata.org/uploads/6/9/1/3/69134503/atop_5_1_codebook.pdf) |
| `source_mtops_dyadic` | Issue Correlates of War Project (ICOW) | `mtopsd150.csv` | 1.5 | [Release](https://www.paulhensel.org/Data/mtops.zip) |
| `source_cow_trade_dyadic` | Correlates of War Project (COW) | `Dyadic_COW_4.0.csv` | 4.0 | [Release](https://correlatesofwar.org/wp-content/uploads/COW_Trade_4.0.zip) |
| `source_cow_trade_national` | Correlates of War Project (COW) | `National_COW_4.0.csv` | 4.0 | [Release](https://correlatesofwar.org/wp-content/uploads/COW_Trade_4.0.zip) |
| `source_national_material_capabilities` | Correlates of War Project (COW) | `NMC-70-wsupplementary.csv` | 7.0 | [Release](https://correlatesofwar.org/wp-content/uploads/NMCv7.zip) |

## Materialized Tables

Step 1 materializes reference tables:

- `country_codes`
- `war_types`

`war_types` is maintained as inline SQL reference data: `05_create_reference_tables.sql` creates the table and
`06_insert_reference_tables.sql` inserts the rows.

Step 1 also materializes transformed tables:

- `dyads_after_mid`
- `dyads_after_sources`
- `war_participants`

Step 1 also materializes base output tables:

- `dyads`
- `dyad_years`
- `participants`
- `wars`

Step 2 also materializes descriptive output tables:

- `country_year_descriptives`
- `participant_year_descriptives`
- `participant_descriptives`
- `dyad_year_descriptives`
- `dyadic_descriptives`

## Final Outputs

Step 3 materializes final merge and graph-export tables:

- `final_participants`
- `final_dyads`
- `final_wars`

`final_wars.graph_json` stores one graph payload per `war_id`, while `final_participants` and `final_dyads` keep the
normalized graph shape available for a Svelte app or API route. `pipeline.py` writes the single frontend payload after
Step 3 completes. Node and link descriptor values are stored in `descriptor_timeframes` JSON keyed by `first_year`,
`last_year`, and `all_years`; the frontend payload exposes those timeframe keys directly on each graph node or link.

## Ingestion Assumptions

### Source Ingestion Rules

- The primary source tables listed above come directly from their source CSV files, with only type coercion, column
  renaming, encoding normalization, and the data-entry fixes documented below applied during load.
- Source CSV headers are aliased to canonical pipeline names as early as possible. COW `WarNum`/`war_num` fields are
  loaded as `war_id`, numeric war-type fields are loaded as `war_type_id`, and the human-readable label comes from
  `war_types.war_type`. Ongoing-war markers and derived flags use `ongoing_war`; `ongoing_conflict` is not used as a
  table or frontend payload field.
- `source_global_terrorism_database` stacks two prepared GTD CSVs with `union all` after confirming the two files do
  not overlap on `eventid`.
- `dyadic_mid_4.03.csv` has no new columns relative to `dyadic_mid_4.02.csv` and no longer includes the 4.02 columns
  `dyad`, `abbreva`, `abbrevb`, `lastobs`, and `newar`.
- Version-scoped source adjustments live in `backend/sql/step_1/03_create_source_adjustment_tables.sql` and
  `backend/sql/step_1/04_insert_source_adjustments.sql`. The first file creates `source_file_versions` and adjustment
  tables; the second inserts adjustment rows for source facts that are not present in the source CSVs. Downstream
  transformations join adjustment tables to `source_file_versions` when an assignment is version-scoped. Adjustment
  rows should stay lean: store only values used for joins, source corrections, or downstream transformations. Data-entry
  fixes applied while reading source CSVs are documented below.
- Reference data that is not tied to an external source file, currently `war_types`, is created and inserted in
  `backend/sql/step_1/05_create_reference_tables.sql` and
  `backend/sql/step_1/06_insert_reference_tables.sql`.

### Excluded Calculated Columns

- Source columns that are documented as simple calculations from other source columns are not ingested. Currently
  excluded calculated fields are `batdths` and `durindx` from unversioned `directed_dyadic_war.csv`; `durindx`, `duration`, and
  `cumdurat` from `dyadic_mid_4.03.csv`; and `WDuratDays`, `WDuratMo`, and `TotalBDeaths` from
  `INTRA-STATE_State_participants v5.1 CSV.csv`. Duration and day-count fields are excluded because they should be
  calculated from the pipeline's resolved start and end dates, after applying the date assumptions below, such as using
  the last day of the year when only the end year is known.

### Date Values

- Blank strings are loaded as `null`. Text values `-7`, `-8`, and `-9` are also treated as `null` because the COW codebooks
  use negative values for ongoing, not applicable, or unknown values.
- Negative day, month, and start-year date fields are loaded as `null`. Negative end-year values are loaded as `null` except
  for `-7`, which the COW codebooks document as the ongoing-war marker.
- Missing, invalid, unknown, or not-applicable start months are interpreted as January, and start days are interpreted
  as day `1` of the resolved month.
- Missing, invalid, unknown, or not-applicable end months are interpreted as December, and end days are interpreted as
  the last day of the resolved month.
- Day values are capped to the last valid day of the resolved month, so an end date with year `2012`, month `10`, and
  missing day resolves to `2012-10-31`.
- End year `-7` is treated as ongoing and resolved to December 31 of the current year at pipeline runtime.
- A date is flagged as estimated when the year is an ongoing marker or when a positive year has a missing or invalid
  month or day.
- Raw source date components are expected to be in basic valid domains before cleaning: months `1-12`, days `1-31`,
  and years `1500-2100`, while COW sentinels `-7`, `-8`, and `-9` are allowed. Values outside these domains are treated
  as data-entry issues and documented below when accepted by the pipeline.

### Encoding And Deduplication

- `COW-country-codes.csv` is deduplicated by `c_code`; the first row per code is retained.
- Source CSVs that need explicit encoding handling use `latin-1` by default. The non-default source encoding is
  `Extra-StateWarData_v4.0.csv` as `cp1252`; prepared copies are written as UTF-8 under `backend/.work/` before DuckDB
  reads them.

### Field Normalization

- `dyadic_mid_4.03.csv` side-specific fatality levels are converted during ingestion to representative battle-death
  estimates as follows: `0 -> 0`, `1 -> 25`, `2 -> 100`, `3 -> 250`, `4 -> 500`, `5 -> 999`, and `6 -> 1000`.
- Participant names are normalized only for known display and matching issues: United States, Baron von
  Ungern-Sternberg's White army, Janissaries, Turkey/Ottoman Empire/Egypt, and a small set of lower-case rebel,
  resistance, sultanate, and tribe suffixes.

## Transformation Assumptions

### Table Shape

- Directed dyadic interstate war records get war name and war type metadata from `source_interstate_wars` by `war_id`;
  synthetic MID-only wars get metadata from source adjustment tables.
- Transformed tables do not carry source-only identifiers and outcome fields (`disno`, `dyindex`, `outcome_a`,
  `outcome_b`, and `outcome`) after they are no longer needed as table outputs. MID matching still uses `disno`
  internally where needed.
- After source date components are resolved, transformed tables carry `start_date`, `end_date`, and date-estimation
  flags instead of the original day/month/year component columns.
- Step 2 final descriptive tables use a `timeframe` column to distinguish the span summarized for each war participant
  or dyad: `First Year`, `Last Year`, and `All Years`.

### Source War Dyads And Participants

- Extra-state and intra-state war dyads are treated as side A versus side B rows, with side A assigned side `1` and
  side B assigned side `2`.
- Extra-state and intra-state participant rows are derived from both sides of the corresponding dyad rows.
- Directed dyadic interstate source rows do not materialize `side_a` or `side_b`; row position is already represented
  by `c_code_a` and `c_code_b`. The original directed dyadic role fields are retained as `role_a`, `role_b`,
  `dyad_role_a`, and `dyad_role_b`.
- In the transformed `war_dyads` view, interstate `side_a` and `side_b` are resolved back to substantive participant
  sides from `source_interstate_wars`; extra-state and intra-state dyads keep their source side A versus side B
  convention.

### Date Spans

- For extra-state and intra-state rows with multiple date spans, the pipeline uses the earliest start date and latest
  end date as the war dyad/participant span.
- Interstate participant dates use the earliest start date and latest end date across the two source date spans.
- Source rows with multiple date spans are validated by date pair before spans are collapsed. A bad pair such as
  `start_1 > end_1` should be corrected or explicitly accepted before relying on the row-level earliest-start/latest-end
  span.

### Directed Dyads And MID Records

- Participants that appear on both side 1 and side 2 in dyadic data are assigned side `3` programmatically.
- `dyads_after_sources` makes source war dyads directed by adding a reversed copy of each dyad.
- `dyads_after_mid` adds dyadic MID records to source war dyads.
- Only dyadic MID records with `war = 1` are incorporated.
- MID dyads are not incorporated when the same directed dyad in the same war overlaps an existing source war-dyad row.
- Existing battle-death values take precedence over MID fatality estimates for remaining merged rows. MID estimates are
  used when summed source battle deaths are `null` or zero and summed estimates are positive.
- MID dyads are assigned to known wars by `disno` from `source_interstate_war_dyads` and version-scoped rows in
  `source_interstate_mid_war_id_adjustments`.
- Missing MID `disno` to `war_id` relationships are stored in the Step 1 source adjustment tables when the current CSV
  version still needs them. If a future CSV version introduces a new unmatched MID war,
  `test_mid_dyads_resolve_all_mid_war_ids` should fail until the source adjustment file is updated or the new source
  data is accepted as authoritative.
- Manual interstate war-dyad additions that are missing from `directed_dyadic_war.csv` are stored in
  `source_interstate_war_dyad_adjustments` and merged after source and MID dyads.
- Synthetic war metadata, such as the Lebanon-Israel MID conflict (`disno = 4182`) named
  `Israeli–Hezbollah Conflict (South Lebanon)`, is stored in `source_interstate_war_metadata_adjustments` and joined
  during transformation without adding partial rows to `source_interstate_wars`.

### Participant Inference

- Participants found in dyadic data but missing from `war_participants` are added to `participants` from the
  dyadic side A records.
- Missing participant sides are inferred from the opposite participant in dyadic data when that inference is unambiguous.
- Remaining version-specific participant side assignments are stored in `source_participant_side_adjustments` and joined
  during participant creation. These adjustments are for source facts that cannot be calculated from participant or
  dyadic rows.
- Interstate war participant sides are taken from `source_interstate_wars`, either directly in `war_participants` or
  through semantic side values on `war_dyads`, because the directed dyadic source can include reciprocal rows where the
  same state appears as both `c_code_a` and `c_code_b` for the same war or dispute.
- Inferred dyads are created by choosing anchor participants for each war. An anchor is a participant that is treated as
  a known adversary for all overlapping participants on the opposite side when source dyadic records are incomplete.
- Anchor selection is independent by side and participant type. A participant is selected as an anchor when its side has
  exactly one total participant, exactly one named non-state participant, or exactly one state participant. More than one
  anchor can be selected for the same war, including anchors on both sides.
- Named non-state participants with COW code `-8` are retained in `dyads`. Unnamed or literal aggregate
  placeholders are excluded.

For example, in the Third Somalia War (`war_id = 940.8`), the source intra-state participant file lists six side 1
states and two side 2 participants. Side 2 has exactly one named non-state participant, ICU (`c_code = -8`), and exactly
one state participant, Eritrea (`c_code = 531`), so both become anchors.

| Side | Source participants | Anchor rule | Selected anchors |
| --- | --- | --- | --- |
| 1 | United States of America, Uganda, Kenya, Burundi, Somalia, Ethiopia | No single total, non-state, or state participant | None |
| 2 | ICU, Eritrea | One named non-state participant; one state participant | ICU, Eritrea |

Those anchors are then linked to every overlapping participant on the opposite side:

```mermaid
flowchart LR
    burundi["Burundi"] --- icu["ICU"]
    burundi --- eritrea["Eritrea"]
    ethiopia["Ethiopia"] --- icu
    ethiopia --- eritrea
    kenya["Kenya"] --- icu
    kenya --- eritrea
    somalia["Somalia"] --- icu
    somalia --- eritrea
    uganda["Uganda"] --- icu
    uganda --- eritrea
    usa["United States of America"] --- icu
    usa --- eritrea
```

### Dyads

- Source dyads with COW code `-8` on one side are expanded against every actual participant on that side. For example,
  if `c_code_a = -8`, side B is treated as having fought each source participant on side A for that conflict.
- Unnamed aggregate dyads are excluded from `dyads` after those rows are used for named-participant expansion.
- Inferred dyads are only created where the anchor and opposing participant date ranges overlap.
- Final dyads are deduplicated to one row per `war_id` and unordered participant pair. When duplicate spans exist, the
  final row keeps the earliest start date and latest end date from the unordered dyad pair.
- `dyad_years` expands `dyads` into one row per year for years in the range `1500` through `2099`.
- Step 2 and Step 3 preserve the semantic difference between unknown values and known zeros. Missing descriptor values
  stay `null` unless the source coverage or project derivation makes the value known to be zero, such as
  `concurrent_wars` when no overlapping participant war exists. Source unknown/not-applicable sentinels such as `-9`
  and `-8` become `null`, and the frontend displays null descriptor values as unknown rather than zero.
- Step 3 participant outputs convert notebook-era unit-scaled fields while building `descriptor_timeframes`: trade money
  flows to dollars, NMC military/population and displacement counts to people, and iron/steel and energy figures to
  documented base units.
- Step 3 prunes unavailable graph descriptor fields per war while building `final_participants` and `final_dyads`. Node
  descriptor fields are kept only when they have a positive maximum value, fewer than half null values, and more than
  one coalesced value after treating nulls as zero. Link descriptor fields are kept only when at least one dyad has a
  positive value.
- Step 3 does not write separate JSON files. `final_wars.graph_json` provides the per-war graph payload directly from
  DuckDB, and `pipeline.py` writes the single frontend payload from
  `backend/sql/step_3/04_export_frontend_graph_data.sql`.

## Data-Entry Fixes And Assignment Rules

- `directed_dyadic_war.csv`
  - Start month value `24` is treated as invalid and loaded as `null`; resolved start dates use the standard missing
    start-month default.
  - End year is corrected from original value `19118` to `1918`.
  - The World War I Japan dyads against Germany and Austria-Hungary are added as version-scoped source adjustments
    because Japan appears as a World War I participant in `Inter-StateWarData_v4.0.csv`, but the directed dyadic war
    source has no World War I dyads involving Japan. Japan is linked to Germany from `1914-08-23` through
    `1918-11-11` and to Austria-Hungary from `1914-08-23` through `1918-11-03`, using the overlapping participant
    date spans from `Inter-StateWarData_v4.0.csv`.
  - The World War II Thailand dyad (`war_id = 139`, `statea = 800`, `stateb = 710`) is loaded with Thailand battle
    deaths corrected from original blank `batdtha` to `5,569`. The Thailand death count comes from Wikipedia's summary
    of [Thailand in World War II](https://en.wikipedia.org/wiki/Thailand_in_World_War_II).
- `dyadic_mid_4.03.csv`
  - The source does not include COW war numbers, so rows are assigned to known wars by matching `disno` to
    `directed_dyadic_war.csv` where possible.
  - Unmatched MID disputes `3582`, `3583`, and `3585` are assigned from original missing `war_id` to World War II
    (`war_id = 139`) after manual review.
  - Unmatched MID dispute `4339` is assigned from original missing `war_id` to Africa's World War (`war_id = 905`)
    after manual review.
  - Unmatched MID dispute `4182` between Lebanon (`660`) and Israel (`666`) is assigned synthetic `war_id = 4182` and
    named `Israeli–Hezbollah Conflict (South Lebanon)`. This fake war id uses the MID `disno` because the conflict
    appears in the dyadic MID records with `war = 1`, but no corresponding `war_id` exists for it in the interstate
    war data. Lebanon is assigned participant side `1`, and Israel is assigned participant side `2`.
  - These assignments are implemented as version-scoped source adjustments, not as transformation-time fallback logic.
- `INTRA-STATE_State_participants v5.1 CSV.csv`
  - War number `977` is corrected to `979`. The intra-state war-level CSV and codebook identify the Syrian Arab
    Spring War as war `979`, and no war `977` exists there; the state-participant file has one `977` row for Iran with
    the same Syrian war name and start date.
  - War `976` has `StartYr1` corrected from original value `2001` to `2011`. The intra-state war-level CSV identifies
    the Libyan Civil War of 2011 as war `976` with `StartYr1 = 2011`; the affected state-participant rows have March
    2001 dates despite a 2011 war name and 2011 end year.
  - Wars `942`, `990.4`, `991`, `991.4`, and `992.5` are treated as ongoing because their source war names say
    `present` or `ongoing`; `EndYr1` is set to `-7` for these rows. The original source values include `-7`, `-8`, and
    `-9`, but only `-7` is treated as an ongoing end-year marker. Other negative end-year values are loaded as `null`
    because the codebooks use them for not applicable or unknown values.

## Embedded Build Artifacts

- `npm run rollup` builds `frontend/dist/bundle.js` and `frontend/dist/bundle.css` for the Jekyll-rendered embedded
  surface. Use the SvelteKit/Vite routes for normal local frontend development.

## Maintainer Notes

- Participant names for rows with COW codes come from `country_codes.state_name`. `participant_name_replacements.json`
  is reserved for formatting cleanup and uncoded participant consolidation, and replacement targets should not duplicate
  `country_codes.state_name` values.
