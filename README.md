# The Networks of War

A DuckDB and Svelte project for building and browsing war-participant networks from COW and other conflict, military,
economic, demographic, and displacement sources.

## Table Of Contents

- [The Networks of War](#the-networks-of-war)
  - [Table Of Contents](#table-of-contents)
  - [Quickstart](#quickstart)
  - [Current Architecture](#current-architecture)
    - [Backend](#backend)
    - [Frontend](#frontend)
      - [Embedded Build Artifacts](#embedded-build-artifacts)
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
    - [Graph Export And Descriptor Semantics](#graph-export-and-descriptor-semantics)
  - [Data-Entry Fixes And Assignment Rules](#data-entry-fixes-and-assignment-rules)
  - [Maintainer Notes](#maintainer-notes)

## Quickstart

Install the backend and build the DuckDB database first. From `the_networks_of_war/backend`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python src/pipeline.py
```

The backend `dev` extra includes `pdftotext`. The pipeline does not import it at runtime; it is installed so coding
agents and maintainers can extract and search PDF source documentation under `backend/data/` when validating source
assumptions.

Install and run the frontend. From `the_networks_of_war/frontend`:

```bash
npm install
npm run data:build
npm run dev
```

`npm run data:build` reruns the full backend pipeline through `../backend/.venv/bin/python` and expects the backend
virtual environment and source data to be available.

## Current Architecture

### Backend

- The DuckDB backend rebuilds preprocessing steps with native SQL.
- Python handles supporting orchestration:
  - Resolves file paths.
  - Prepares downloaded source files.
  - Normalizes configured CSV encodings.
  - Runs the SQL files in order.

### Frontend

- The Svelte frontend lives in `frontend/`.
- It provides a routed Svelte app and a usable war browser backed by the Step 3 graph export.

- In Vite development, the menu is available at `/` and `/the_networks_of_war`, while the browser itself is available
  at `/tool` and `/the_networks_of_war/tool`.

- The frontend consumes ignored generated data at `frontend/src/lib/static/graphData.json`; do not commit this file.
  Step 3 writes it from `backend/src/sql/step_3/04_export_frontend_graph_data.sql` after the final Step 3 tables are
  built.
- Generated graph rows keep two metric layers: top-level timeframe fields contain only descriptor fields that pass
  per-war availability checks for graph controls, while each node's `metrics` object contains all non-null participant
  metrics for the tooltip.

- The graph metric data dictionary lives at
  [`frontend/src/lib/static/metricDataDictionary.json`](frontend/src/lib/static/metricDataDictionary.json).
  - It is written for non-technical users.
  - It records each graph metric's source organization or study, high-level calculation, and display unit.
  - Keep this file aligned with backend metric changes and with any README metric summaries.

- Node-size descriptor behavior:
  - Known zero values render at the minimum node radius.
  - Unknown or `null` values also shrink to the minimum radius.
  - A small `?` marker is shown beside node labels only when there are a few unknown selected descriptor values.
  - If many nodes are unknown, per-node markers are suppressed and the tooltip still displays the selected descriptor as
    `Unknown`.
  - The no-descriptor default still uses equal fallback sizing so the graph remains readable before a size field is
    selected.

- Node tooltip behavior:
  - Tooltips show participant start date, end date, days at war, and every non-null participant metric available for the
    selected timeframe.
  - Estimated start dates, end dates, and battle deaths are labeled with `(estimated)`.
  - Ongoing-war participants show `Ongoing` as the end date so source-data caps are not mistaken for true conflict end
    dates.
  - Some count-style node metrics are yearly counts summarized across a selected timeframe.
  - Multi-year summaries can therefore be fractional averages, even though each yearly source count is a whole number.
    - Example: average concurrent wars per year.

- Tooltip number formatting:
  - Numbers are rounded to at most two decimal places.
  - Values of at least one million are shortened to readable million, billion, or trillion labels without showing the
    full underlying value.
  - Examples:
    - `1,400,000` displays as `1.4 million`.
    - `1,400,010` displays as `1.4 million`.
    - `56,546,000,000` displays as `56.55 billion`.
  - Smaller values continue to display in comma-separated form.

#### Embedded Build Artifacts

`npm run rollup` builds `frontend/dist/bundle.js` and `frontend/dist/bundle.css` for the Jekyll-rendered embedded
surface. Use the SvelteKit/Vite routes for normal local frontend development.

## Data Layout

- Source data is downloaded into `backend/data/`, which is ignored by git.
  - Each external source table gets its own subdirectory named after the source key without the `source_` table prefix.
  - Example: `backend/data/interstate_mid_dyads/` corresponds to `source_interstate_mid_dyads`.
  - The corresponding raw source data and source documentation live in that folder.
- Source download metadata lives in `backend/manual/source_metadata.json`, including Step 1 source release dates used
  for ongoing-war date caps.
- Source CSVs that need explicit encoding handling use `latin-1` by default. Prepared copies are written to UTF-8 under
  ignored `backend/.work/` before DuckDB reads them.
- Prepared source subdirectories keep only durable source CSVs and PDF or JSON source documentation. Archive files,
  original Excel/Stata workbooks, text exports, and temporary download caches are discarded after extraction or
  conversion; the expected `backend/data/` layout excludes `_downloads/`.
- Ignored generated paths:
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
pytest tests/test_step_1.py -k "negative_date_special_codes"
pytest tests/test_step_1.py -k "date_macros or dyads"
```

Show verbose test names and failures:

```bash
pytest tests/test_step_1.py -vv
```

The Step 1 expectation tests rebuild Step 1 into a temporary DuckDB database. They run when the ignored source files in
`backend/data/` are available locally and skip automatically otherwise.

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

| Table | Organization | Source CSV | Version | Release date | Download source |
| --- | --- | --- | --- | --- | --- |
| `source_country_codes` | Correlates of War Project (COW) | `COW-country-codes.csv` | unversioned | 2022-09-07 upload | [Data](https://correlatesofwar.org/wp-content/uploads/COW-country-codes.csv) |
| `source_extrastate_wars` | Correlates of War Project (COW) | `Extra-StateWarData_v4.0.csv` | 4.0 | 2011-12-08 release | [Data](https://correlatesofwar.org/wp-content/uploads/Extra-StateWarData_v4.0.csv)<br>[Doc](https://correlatesofwar.org/wp-content/uploads/Extra-StateWars_Codebook.pdf) |
| `source_interstate_mid_dyads` | Correlates of War Project (COW) | `dyadic_mid_4.03.csv` | 4.03 | 2025-04-06 upload | [Release](https://correlatesofwar.org/wp-content/uploads/dyadic_mid_4.03_update.zip) |
| `source_interstate_war_dyads` | Correlates of War Project (COW) | `directed_dyadic_war.csv` | unversioned | 2022-07-12 upload | [Release](https://correlatesofwar.org/wp-content/uploads/Dyadic-Interstate-War-Dataset.zip) |
| `source_interstate_wars` | Correlates of War Project (COW) | `Inter-StateWarData_v4.0.csv` | 4.0 | 2011-03-01 release | [Data](https://correlatesofwar.org/wp-content/uploads/Inter-StateWarData_v4.0.csv)<br>[Doc 1](https://correlatesofwar.org/wp-content/uploads/Inter-StateWars_Codebook.pdf)<br>[Doc 2](https://correlatesofwar.org/wp-content/uploads/Inter-StateWarsList.pdf) |
| `source_intrastate_wars` | Correlates of War Project (COW) | `INTRA-STATE_State_participants v5.1 CSV.csv` | 5.1 | 2020-04-06 release | [Release](https://correlatesofwar.org/wp-content/uploads/Intra-State-Wars-v5.1.zip) |

Release dates above use the COW war-data page when that page states the day a source became available. For Step 1 files
without a dated release note on the source page, the date is the COW WordPress media attachment date for the exact file
URL the pipeline downloads. Local PDF text and metadata were checked but are treated as documentation/build metadata
unless they explicitly identify the current source file's release date.

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

Step 1 materializes the reference tables `country_codes` and `war_types`.

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

Descriptor dictionary additions, new metrics: `shared_arms_technology` is a link-dash descriptor equal to `1` when both
countries in a dyad used at least one of the same COW arms technologies in the descriptor year.

Descriptor dictionary additions, recalculated source metrics:

- `arms_technologies_used`: node-size descriptor derived from the COW arms technology source's calculated `total_use`
  column. Step 2 recalculates it from individual technology rows in the descriptor year by counting rows with `use`
  codes `1` or `9` and excluding the aggregate `Adopted technologies` row.
- `cinc_score`: node-size descriptor derived in Step 2 from the six NMC component shares rather than ingested from the
  source CSV's calculated `cinc` column. For each year, Step 2 divides each state's component values by that year's
  system total for the same component, then averages the six shares:
  - Military expenditure.
  - Military personnel.
  - Iron and steel production.
  - Primary energy consumption.
  - Total population.
  - Urban population.

## Final Outputs

Step 3 materializes the final merge and graph-export tables `final_participants`, `final_dyads`, and `final_wars`.
`final_participants` and `final_dyads` keep the normalized graph shape available for a Svelte app or API route.
`final_wars.graph_json` stores one graph payload per `war_id`, and `pipeline.py` writes the single frontend payload
after Step 3 completes.

Node and link descriptor values are stored in `descriptor_timeframes` JSON keyed by:

- `first_year`
- `last_year`
- `all_years`

The frontend payload exposes those timeframe keys directly on each graph node or link.

## Ingestion Assumptions

### Source Ingestion Rules

- The primary source tables listed above come directly from their source CSV files. During load, the pipeline applies
  only:
  - Type coercion.
  - Column renaming.
  - Encoding normalization.
  - The data-entry fixes documented below.
- Source CSV headers are aliased to canonical pipeline names as early as possible:
  - COW `WarNum` and `war_num` fields are loaded as `war_id`.
  - Numeric war-type fields are loaded as `war_type_id`.
- Derived or lookup-backed fields are exposed under canonical names:
  - The human-readable label comes from `war_types.war_type`.
  - Ongoing-war markers and derived flags are exposed as `ongoing_war` table and frontend payload fields.
- `source_global_terrorism_database` stacks two prepared GTD CSVs with `union all` after confirming distinct `eventid`
  coverage across the files.
- Source CSV schemas are compared when source versions change. Ingestion keeps relevant columns that remain available,
  adds newly useful fields when downstream transformations need them, and removes truly absent fields instead of
  fabricating placeholder `null` source columns.
- Version-scoped source adjustment files live under `backend/src/sql/step_1/`:
  - `03_create_source_adjustment_tables.sql` creates `source_file_versions` and adjustment tables.
  - `04_insert_source_adjustments.sql` inserts release metadata and adjustment rows for source facts that are not
    present in the source CSVs.
- Downstream transformations join adjustment tables to `source_file_versions` when an assignment is version-scoped.
- Step 1 date-span transformations also use `source_file_versions.source_release_date` to cap ongoing source rows at
  the date the current source file was released or, when no explicit release note is published, uploaded.
- Adjustment rows should stay lean. Store only values used for joins, source corrections, or downstream transformations.
- Data-entry fixes applied while reading source CSVs are documented below.
- Reference data that is not tied to an external source file, currently `war_types`, is created and inserted in
  `backend/src/sql/step_1/05_create_reference_tables.sql` and
  `backend/src/sql/step_1/06_insert_reference_tables.sql`.

### Excluded Calculated Columns

The pipeline ingests raw source fields and recalculates simple derived columns from canonical inputs.

Currently excluded calculated fields:

| Source CSV | Excluded calculated fields |
| --- | --- |
| Unversioned `directed_dyadic_war.csv` | `batdths`, `durindx` |
| `dyadic_mid_4.03.csv` | `durindx`, `duration`, `cumdurat` |
| `INTRA-STATE_State_participants v5.1 CSV.csv` | `WDuratDays`, `WDuratMo`, `TotalBDeaths` |
| `cow_arms_tech_long.csv` | `total_use` |
| `NMC-70-wsupplementary.csv` | `cinc` |

Derived replacements:

- Duration and day-count fields are calculated from the pipeline's resolved start and end dates, after applying the date
  assumptions below.
  - Example: the pipeline uses the last day of the year when only the end year is known.
- `arms_technologies_used` is derived in Step 2 by recounting individual technology rows.
- `cinc_score` is derived in Step 2 by averaging each state's yearly shares of the six NMC components.

### Date Values

- Blank strings are loaded as `null`.
- These text values are also treated as `null` because the COW codebooks use negative values for ongoing, not
  applicable, or unknown values:
  - `-7`
  - `-8`
  - `-9`
- These negative date component fields are loaded as `null`:
  - Day fields.
  - Month fields.
  - Start-year fields.
- Negative end-year values are loaded as `null` except for `-7`, which the COW codebooks document as the ongoing-war
  marker.
- Missing, invalid, unknown, or not-applicable start months are interpreted as January, and start days are interpreted
  as day `1` of the resolved month.
- Missing, invalid, unknown, or not-applicable end months are interpreted as December, and end days are interpreted as
  the last day of the resolved month.
- Day values are capped to the last valid day of the resolved month, so an end date with year `2012`, month `10`, and
  missing day resolves to `2012-10-31`.
- End year `-7` is treated as ongoing and resolved to the source file's release date from
  `source_file_versions.source_release_date`. This keeps ongoing rows reproducible and prevents Step 2 `Last Year` and
  `All Years` descriptors from expanding beyond the years covered by the released source data.
- A date is flagged as estimated when the year is an ongoing marker or when a positive year has a missing or invalid
  month or day.
- Raw source date components are expected to be in basic valid domains before cleaning:
  - Months: `1-12`.
  - Days: `1-31`.
  - Years: `1500-2100`.
- COW special codes allowed before cleaning:
  - `-7`
  - `-8`
  - `-9`
- Values outside these domains are treated as data-entry issues and documented below when accepted by the pipeline.

### Encoding And Deduplication

- `COW-country-codes.csv` is deduplicated by `c_code`; the first row per code is retained.
- Source CSVs that need explicit encoding handling use `latin-1` by default.
- The non-default source encoding is `Extra-StateWarData_v4.0.csv` as `cp1252`.
- Prepared copies are written as UTF-8 under `backend/.work/` before DuckDB reads them.

### Field Normalization

- `dyadic_mid_4.03.csv` side-specific fatality levels are converted during ingestion to representative battle-death
  estimates:
  - `0 -> 0`
  - `1 -> 25`
  - `2 -> 100`
  - `3 -> 250`
  - `4 -> 500`
  - `5 -> 999`
  - `6 -> 1000`
- Participant names are normalized only for known display and matching issues. The full manual mapping lives in
  `backend/manual/participant_name_replacements.json`.
  - Examples:
    - `United States -> United States of America`
    - `Baron von Ungern-Sternbergs White army -> Baron von Ungern-Sternberg's White army`
    - `Turkey/Ottoman Empire/Egypt -> Turkey, Ottoman Empire & Egypt`
    - `al-Qaeda & Iraqi resistence -> al-Qaeda & Iraqi Resistance`
    - `Bharatpuran rebels -> Bharatpuran Rebels`
    - `Wadai sultanate -> Wadai Sultanate`
    - `Zulu tribe -> Zulu Tribe`
    - ` Janissaries -> Janissaries`

## Transformation Assumptions

### Table Shape

- Directed dyadic interstate war records get war name and war type metadata from `source_interstate_wars` by `war_id`;
  synthetic MID-only wars get metadata from source adjustment tables.
- Transformed tables carry pipeline-facing fields after source-only identifiers and outcome fields have served their
  matching and transformation roles:
  - `disno`
  - `dyindex`
  - `outcome_a`
  - `outcome_b`
  - `outcome`
- MID matching still uses `disno` internally where needed.
- After source date components are resolved, transformed tables carry:
  - `start_date`
  - `end_date`
  - `start_date_estimated` and `end_date_estimated` flags, which mark dates resolved from an ongoing marker or from a
    positive year with a missing or invalid month or day.
- Transformed tables do not carry the original day/month/year component columns.
- Step 2 final descriptive tables use a `timeframe` column to distinguish the span summarized for each war participant
  or dyad:
  - `First Year`
  - `Last Year`
  - `All Years`

### Source War Dyads And Participants

- Extra-state and intra-state war dyads are treated as side A versus side B rows, with side A assigned side `1` and
  side B assigned side `2`.
- Extra-state and intra-state participant rows are derived from both sides of the corresponding dyad rows.
- Directed dyadic interstate source rows represent row position with `c_code_a` and `c_code_b`.
- The original directed dyadic role fields are retained as:
  - `role_a`
  - `role_b`
  - `dyad_role_a`
  - `dyad_role_b`
- In the transformed `war_dyads` view, interstate side fields `side_a` and `side_b` are resolved back to substantive
  participant sides from `source_interstate_wars`.
- Extra-state and intra-state dyads keep their source side A versus side B convention.

### Date Spans

- For extra-state and intra-state rows with multiple date spans, the pipeline uses the earliest start date and latest
  end date as the war dyad/participant span.
- Interstate participant dates use the earliest start date and latest end date across the two source date spans.
- Source rows with multiple date spans are validated by date pair before spans are collapsed. Bad pairs should be
  corrected or explicitly accepted before relying on the row-level earliest-start/latest-end span.
  - Example: `start_1 > end_1`.

### Directed Dyads And MID Records

- Participants that appear on both side 1 and side 2 in dyadic data are assigned side `3` programmatically.
- `dyads_after_sources` makes source war dyads directed by adding a reversed copy of each dyad.
- `dyads_after_mid` adds dyadic MID records to source war dyads.
- Only dyadic MID records with `war = 1` are incorporated.
- MID dyads are incorporated only when the same directed dyad in the same war has no overlapping source war-dyad row.
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
- Synthetic war metadata is stored in `source_interstate_war_metadata_adjustments` and joined during transformation
  without adding partial rows to `source_interstate_wars`.
  - Example: the Lebanon-Israel MID conflict (`disno = 4182`) named
    `Israeli–Hezbollah Conflict (South Lebanon)`.

### Participant Inference

- Participants found in dyadic data but missing from `war_participants` are added to `participants` from the
  dyadic side A records.
- Missing participant sides are inferred from the opposite participant in dyadic data when that inference is unambiguous.
- Remaining version-specific participant side assignments are stored in `source_participant_side_adjustments` and joined
  during participant creation. These adjustments store source facts needed beyond participant and dyadic rows.
- Interstate war participant sides are taken from `source_interstate_wars`, either directly in `war_participants` or
  through semantic side values on `war_dyads`.
- The directed dyadic source can include reciprocal rows where the same state appears as both `c_code_a` and `c_code_b`
  for the same war or dispute.
- Inferred dyads are created by choosing anchor participants for each war. An anchor is a participant that is treated as
  a known adversary for all overlapping participants on the opposite side when source dyadic records are incomplete.
- Anchor selection is independent by side and participant type. A participant is selected as an anchor when its side has:
  - Exactly one total participant.
  - Exactly one named non-state participant.
  - Exactly one state participant.
- More than one anchor can be selected for the same war, including anchors on both sides.
  - Example: in the Third Somalia War (`war_id = 940.8`), the source intra-state participant file lists six side 1
    states and two side 2 participants. Side 2 has exactly one named non-state participant, ICU (`c_code = -8`), and
    exactly one state participant, Eritrea (`c_code = 531`), so both become anchors.
- Named non-state participants with COW code `-8` are retained in `dyads`. Unnamed or literal aggregate
  placeholders are excluded.

| Side | Source participants | Anchor rule | Selected anchors |
| --- | --- | --- | --- |
| 1 | United States of America, Uganda, Kenya, Burundi, Somalia, Ethiopia | No single total, non-state, or state participant | None |
| 2 | ICU, Eritrea | One named non-state participant; one state participant | ICU, Eritrea |

Those anchors are then linked to every overlapping participant on the opposite side:

| Anchor | Linked opposite-side participants |
| --- | --- |
| ICU | Burundi, Ethiopia, Kenya, Somalia, Uganda, United States of America |
| Eritrea | Burundi, Ethiopia, Kenya, Somalia, Uganda, United States of America |

### Dyads

- Source dyads with COW code `-8` on one side are expanded against every actual participant on that side.
  - Example: if `c_code_a = -8`, side B is treated as having fought each source participant on side A for that conflict.
- Unnamed aggregate dyads are excluded from `dyads` after those rows are used for named-participant expansion.
- Inferred dyads are only created where the anchor and opposing participant date ranges overlap.
- Final dyads are deduplicated to one row per `war_id` and unordered participant pair. When duplicate spans exist, the
  final row keeps the earliest start date and latest end date from the unordered dyad pair.
- `dyad_years` expands `dyads` into one row per year for years in the range `1500` through `2099`.

### Graph Export And Descriptor Semantics

- Step 2 and Step 3 preserve the semantic difference between unknown values and known zeros. Missing descriptor values
  stay `null` unless the source coverage or project derivation makes the value known to be zero.
  - Example: `concurrent_wars` is known to be zero when no overlapping participant war exists.
- Source unknown/not-applicable codes `-9` and `-8` become `null`.
- The frontend displays `null` descriptor values as unknown rather than zero.
- Step 3 participant outputs convert notebook-era unit-scaled fields before graph export:
  - COW trade currency values: from millions to dollars.
  - NMC military expenditure, military personnel, population, iron/steel, and energy values: from thousands to base
    units.
  - Displacement counts: from thousands to people.
- Step 3 keeps graph-control descriptors and tooltip metrics separate.
  - Node tooltip metrics are stored under each node's `metrics` object and include all non-null participant metrics for
    the timeframe.
  - The tooltip displays:
    - Non-zero metrics.
    - Selected zero-value metrics.
    - Zero values for metrics that are always useful to show.
  - Top-level node descriptor fields are kept for node-size dropdown options only when they have:
    - At least one positive known value.
    - Fewer than half `null` values.
    - Either more than one known value or a useful known/unknown distinction.
  - Link descriptor fields are kept only when at least one dyad has a positive value.
- Step 3 stores the per-war graph payload directly in `final_wars.graph_json`, and `pipeline.py` writes the single
  frontend payload from
  `backend/src/sql/step_3/04_export_frontend_graph_data.sql`.

## Data-Entry Fixes And Assignment Rules

- `directed_dyadic_war.csv`
  - Start month value `24` is treated as invalid and loaded as `null`; resolved start dates use the standard missing
    start-month default.
  - End year is corrected from original value `19118` to `1918`.
  - The World War I Japan dyads against Germany and Austria-Hungary are added as version-scoped source adjustments
    because Japan appears as a World War I participant in `Inter-StateWarData_v4.0.csv`, but the directed dyadic war
    source has no World War I dyads involving Japan.
    - Added links:
      - Germany: `1914-08-23` through `1918-11-11`.
      - Austria-Hungary: `1914-08-23` through `1918-11-03`.
    - Date spans use the overlapping participant date spans from `Inter-StateWarData_v4.0.csv`.
  - The World War II Thailand dyad is loaded with Thailand battle deaths corrected from original blank `batdtha` to
    `5,569`:
    - Source row:
      - `war_id = 139`
      - `statea = 800`
      - `stateb = 710`
    - The Thailand death count comes from Wikipedia's summary of
      [Thailand in World War II](https://en.wikipedia.org/wiki/Thailand_in_World_War_II).
- `dyadic_mid_4.03.csv`
  - Rows are assigned to known COW wars by matching `disno` to `directed_dyadic_war.csv` where possible.
  - These unmatched MID disputes are assigned from original missing `war_id` to World War II (`war_id = 139`) after
    manual review:
    - `3582`
    - `3583`
    - `3585`
  - Unmatched MID dispute `4339` is assigned from original missing `war_id` to Africa's World War (`war_id = 905`)
    after manual review.
  - Unmatched MID dispute `4182` is assigned synthetic `war_id = 4182` and named
    `Israeli–Hezbollah Conflict (South Lebanon)`:
    - Lebanon: `660`, participant side `1`.
    - Israel: `666`, participant side `2`.
    - This fake war id uses the MID `disno` because the conflict appears in the dyadic MID records with `war = 1`, but
      no corresponding `war_id` exists for it in the interstate war data.
  - These assignments are implemented as version-scoped source adjustments that transformations join explicitly.
- `INTRA-STATE_State_participants v5.1 CSV.csv`
  - War number `977` is corrected to `979`:
    - The intra-state war-level CSV and codebook identify the Syrian Arab Spring War as war `979`.
    - No war `977` exists there.
    - The state-participant file has one `977` row for Iran with the same Syrian war name and start date.
  - War `976` has `StartYr1` corrected from original value `2001` to `2011`. The intra-state war-level CSV identifies
    the Libyan Civil War of 2011 as war `976` with `StartYr1 = 2011`; the affected state-participant rows have March
    2001 dates despite a 2011 war name and 2011 end year.
  - End-year handling:
    - Original source values include `-7`, `-8`, and `-9`.
    - Only `-7` is treated as an ongoing end-year marker.
    - Other negative end-year values are loaded as `null` because the codebooks use them for not applicable or unknown
      values.
    - The pipeline sets `EndYr1` to `-7` for source rows whose war names say `present` or `ongoing`:
      - `942`
      - `990.4`
      - `991`
      - `991.4`
      - `992.5`

## Maintainer Notes

- Documentation style:
  - Prefer bullets and subbullets over inline listed-out prose when they make concrete technical lists easier to scan,
    especially files, paths, source values/codes, table names, column names, commands, and metrics.
  - Keep short phrase lists in prose when bullets would make the text feel fragmented.
  - Do not use bullets solely to separate command examples or other code-block sections.
  - Do not place separate bullet groups directly next to each other when they document different concepts.
  - Keep each bullet list focused on one kind of item; move standout metadata, source notes, examples, row identifiers,
    or downstream behavior notes into prose, a table, a new subsection, or a labeled subbullet group.
  - Use prose instead of a bullet list when a section would contain only one bullet.
  - Prefer prose over subbullets when a nested list would have only two items, unless the pair needs extra visual
    separation to avoid ambiguity.
  - Use `Example:` for one example and `Examples:` for multiple examples under an existing bullet.
  - Let table-of-contents nesting reflect the document structure even when a section has only two children.
  - Keep tables when they make dense reference data easier to compare.
- Participant names for rows with COW codes come from `country_codes.state_name`. Use
  `participant_name_replacements.json` only when the source name cannot resolve through a COW code:
  - Non-state participants.
  - Uncoded manual rows.
  - Source tables that do not carry `c_code` values.
- Replacement targets may match `country_codes.state_name` only for no-code source inputs.
  - Example: CO2 country names.
