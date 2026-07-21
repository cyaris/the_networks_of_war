# The Networks of War

A DuckDB and Svelte project for building a war-participant network analysis tool from COW and other conflict,
military, economic, demographic, and displacement sources.

## Table Of Contents

- [The Networks of War](#the-networks-of-war)
  - [Table Of Contents](#table-of-contents)
  - [Quickstart](#quickstart)
  - [Pipeline Overview](#pipeline-overview)
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
  - [Metric And Descriptor Notes](#metric-and-descriptor-notes)
  - [Graph Display Behavior](#graph-display-behavior)
  - [Ingestion Assumptions](#ingestion-assumptions)
    - [Source Ingestion Rules](#source-ingestion-rules)
    - [Excluded Calculated Columns](#excluded-calculated-columns)
    - [Date Values](#date-values)
      - [General Date Cleaning](#general-date-cleaning)
      - [Start Dates](#start-dates)
      - [End Dates](#end-dates)
      - [Date-Estimation Flags](#date-estimation-flags)
    - [Encoding And Deduplication](#encoding-and-deduplication)
    - [Field Normalization](#field-normalization)
      - [Battle-Death Estimate Flags](#battle-death-estimate-flags)
      - [Participant Name Normalization](#participant-name-normalization)
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

The backend `dev` extra includes `pdftotext`. The pipeline does not import `pdftotext` at runtime; the package is
installed so coding agents and maintainers can extract and search PDF source documentation under `backend/data/` when
validating source assumptions.

Install the frontend dependencies. From `the_networks_of_war/frontend`:

```bash
npm install
```

Regenerate the frontend data snapshot. This reruns the full backend pipeline through `../backend/.venv/bin/python` and
expects the backend virtual environment and source data to be available:

```bash
npm run data:build
```

Run the Vite development server for normal local frontend development:

```bash
npm run dev
```

Build the bundle for the Jekyll-rendered embedded surface. Use the SvelteKit/Vite routes for normal local frontend
development:

```bash
npm run rollup
```

## Pipeline Overview

The backend build runs three ordered steps:

| Step | Purpose |
| --- | --- |
| 1 | Ingest source files, apply source-level normalization and adjustments, and create the base war, participant, dyad, and dyad-year tables. |
| 2 | Add country-year, participant, and dyad descriptors from military, economic, demographic, displacement, terrorism, and related sources. |
| 3 | Merge the final participant, dyad, and war outputs, then produce the graph payload used by the frontend. |

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
- The frontend provides a routed Svelte network analysis tool backed by the Step 3 graph export.
- In Vite development, routes are mirrored for the local root and the simulated GitHub Pages base:
  - `/` and `/the_networks_of_war` render the menu.
  - `/tool` and `/the_networks_of_war/tool` render the network analysis tool.
- Generated frontend data is written to `frontend/src/lib/static/graphData.json`, which is not committed.
- Generated graph rows keep two metric layers:
  - Top-level timeframe fields provide the selectable node-size and link-style descriptors.
  - Each node's `metrics` object provides tooltip-only participant metrics, including fields that are not available as
    node-size choices.
- The graph metric data dictionary lives at
  [`frontend/src/lib/static/metricDataDictionary.json`](frontend/src/lib/static/metricDataDictionary.json).
  - The dictionary is written for non-technical users.
  - The dictionary records each graph metric's source organization or study, high-level calculation, and display unit.
  - Keep the dictionary aligned with backend metric changes and with any README metric summaries.

## Data Layout

- Source download metadata lives in `backend/manual/source_metadata.json`, including Step 1 source release dates used
  for ongoing-war date caps.
- Source data is downloaded programmatically into `backend/data/`.
  - Each external source table gets its own subdirectory named after the source key without the `source_` table prefix.
    The corresponding raw source data lives in that folder, along with downloaded PDF or JSON source documentation when
    available.
    - Example: `backend/data/interstate_mid_dyads/` corresponds to `source_interstate_mid_dyads`.
  - Downloaded documentation supports source validation but is not ingested as a source table.
  - Source subdirectories keep durable CSVs and PDF or JSON documentation only. Archives, original Excel/Stata
    workbooks, text exports, and temporary download caches are discarded, so `backend/data/` does not include
    `_downloads/`.
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

Recreate the full source-data directory:

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

The Step 1 expectation tests rebuild Step 1 into a temporary DuckDB database. They run when the source files in
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

## Source Tables

The current backend ingests the following source files.

### Step 1 Source Tables

Release date note: local PDF text and metadata were checked but are treated as documentation/build metadata unless they
explicitly identify the current source file's release date.

| Table | Organization | Source CSV | Version | Release Date | Release Date Source | Download source |
| --- | --- | --- | --- | --- | --- | --- |
| `source_country_codes` | Correlates of War Project (COW) | `COW-country-codes.csv` | unversioned | 2022-09-07 | WordPress media attachment date | [Data](https://correlatesofwar.org/wp-content/uploads/COW-country-codes.csv) |
| `source_extrastate_wars` | Correlates of War Project (COW) | `Extra-StateWarData_v4.0.csv` | 4.0 | 2011-12-08 | COW war-data release note | [Data](https://correlatesofwar.org/wp-content/uploads/Extra-StateWarData_v4.0.csv)<br>[Doc](https://correlatesofwar.org/wp-content/uploads/Extra-StateWars_Codebook.pdf) |
| `source_interstate_mid_dyads` | Correlates of War Project (COW) | `dyadic_mid_4.03.csv` | 4.03 | 2025-04-06 | WordPress media attachment date | [Release](https://correlatesofwar.org/wp-content/uploads/dyadic_mid_4.03_update.zip) |
| `source_interstate_war_dyads` | Correlates of War Project (COW) | `directed_dyadic_war.csv` | unversioned | 2022-07-12 | WordPress media attachment date | [Release](https://correlatesofwar.org/wp-content/uploads/Dyadic-Interstate-War-Dataset.zip) |
| `source_interstate_wars` | Correlates of War Project (COW) | `Inter-StateWarData_v4.0.csv` | 4.0 | 2011-03-01 | COW war-data release note | [Data](https://correlatesofwar.org/wp-content/uploads/Inter-StateWarData_v4.0.csv)<br>[Doc 1](https://correlatesofwar.org/wp-content/uploads/Inter-StateWars_Codebook.pdf)<br>[Doc 2](https://correlatesofwar.org/wp-content/uploads/Inter-StateWarsList.pdf) |
| `source_intrastate_wars` | Correlates of War Project (COW) | `INTRA-STATE_State_participants v5.1 CSV.csv` | 5.1 | 2020-04-06 | COW war-data release note | [Release](https://correlatesofwar.org/wp-content/uploads/Intra-State-Wars-v5.1.zip) |

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

Step 1 also materializes transformed staging tables:

- `war_participants`: participant rows derived from source war and dyad records.
- `dyads_after_sources`: source war dyads made directed by adding a reversed copy of each dyad.
- `dyads_after_mid`: directed source war dyads plus eligible dyadic MID records.

Step 1 also materializes base output tables:

- `wars`: normalized war-level rows.
- `participants`: normalized war-participant rows.
- `dyads`: normalized unordered participant-pair rows.
- `dyad_years`: one row per dyad-year for years in the range `1500` through `2099`.

Step 2 materializes descriptive output tables:

- `country_year_descriptives`
- `participant_year_descriptives`
- `participant_descriptives`
- `dyad_year_descriptives`
- `dyadic_descriptives`

The Step 2 final descriptive tables use a `timeframe` column to distinguish the span summarized for each war
participant or dyad:

- `First Year`
- `Last Year`
- `All Years`

## Final Outputs

Step 3 materializes the final merge and graph-export tables:

- `final_participants` and `final_dyads`: normalized graph rows for a Svelte app or API route.
- `final_wars`: war-level rows, including one `graph_json` payload per `war_id`.

Node and link descriptor values are stored in `descriptor_timeframes` JSON; in the frontend payload, the same timeframe
keys appear directly on each node or link:

- `first_year`
- `last_year`
- `all_years`

## Metric And Descriptor Notes

Link descriptor:

- `shared_arms_technology`: link-dash descriptor equal to `1` when both countries in a dyad used at least one of the
  same COW arms technologies in the descriptor year.

Recalculated source metrics:

- `arms_technologies_used`: node-size descriptor derived from the COW arms technology source's calculated `total_use`
  column. Step 2 recalculates `arms_technologies_used` from individual technology rows in the descriptor year by
  counting rows with `use` codes `1` or `9` and excluding the aggregate `Adopted technologies` row.
- `cinc_score`: node-size descriptor derived in Step 2 from the six NMC component shares rather than ingested from the
  source CSV's calculated `cinc` column. For each year, Step 2 divides each state's component values by that year's
  system total for the same component, then averages the six shares:
  - Military expenditure.
  - Military personnel.
  - Iron and steel production.
  - Primary energy consumption.
  - Total population.
  - Urban population.

## Graph Display Behavior

Node-size descriptor behavior:

- Known zero values render at the minimum node radius.
- Unknown or `null` values also shrink to the minimum node radius.
- A small `?` marker is shown beside node labels only when there are a few unknown selected descriptor values.
- If many nodes are unknown, per-node markers are suppressed and the tooltip still displays the selected descriptor as
  `Unknown`.
- The no-descriptor default still uses equal fallback sizing so the graph remains readable before a size field is
  selected.

Node tooltip behavior:

- Tooltips show participant start date, end date, days at war, and every non-null participant metric available for the
  selected timeframe.
- Estimated values are labeled with `(estimated)`:
  - Start dates use `start_date_estimated`.
  - End dates use `end_date_estimated`.
  - Battle deaths use `battle_deaths_estimated`.
- Ongoing-war participants show `Ongoing` as the end date so source-data caps are not mistaken for true conflict end
  dates.
- Some count-style node metrics start as yearly counts and are then summarized for the selected timeframe.
  - Example: `concurrent_wars` counts overlapping wars separately for each participant-year. `First Year` and
    `Last Year` use the count from one year, while `All Years` averages the yearly counts across the participant's war
    span.

Tooltip number formatting:

- Most numbers are rounded to at most two decimal places.
  - Exception: `cinc_score` is formatted with fixed decimal places because the CINC index needs more precision than
    whole-number style metrics.
- Values of at least one million are shortened to readable million, billion, or trillion labels without showing the
  full underlying value. Smaller values continue to display in comma-separated form.
  - Examples:
    - `1,400,000` displays as `1.4 million`.
    - `1,400,010` displays as `1.4 million`.
    - `56,546,000,000` displays as `56.55 billion`.

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
  - The adjustment-table SQL creates `source_file_versions` and the source adjustment tables.
  - The adjustment-row SQL inserts release metadata and adjustment rows for source facts that are not present in the
    source CSVs.
- Downstream transformations join adjustment tables to `source_file_versions` when an assignment is version-scoped.
- Adjustment rows should stay lean. Store only values used for joins, source corrections, or downstream transformations.
- Data-entry fixes applied while reading source CSVs are documented below.

### Excluded Calculated Columns

The pipeline ingests raw source fields and recalculates simple derived columns from canonical inputs.

Excluded calculated fields:

| Source CSV | Excluded calculated fields |
| --- | --- |
| `directed_dyadic_war.csv` | `batdths`, `durindx` |
| `dyadic_mid_4.03.csv` | `durindx`, `duration`, `cumdurat` |
| `INTRA-STATE_State_participants v5.1 CSV.csv` | `WDuratDays`, `WDuratMo`, `TotalBDeaths` |
| `cow_arms_tech_long.csv` | `total_use` |
| `NMC-70-wsupplementary.csv` | `cinc` |

Derived replacements:

- Duration and day-count fields are calculated from the pipeline's resolved start and end dates, after applying the date
  assumptions below.
  - Example: the pipeline uses the last day of the year when only the end year is known.
- The source's calculated `total_use` column is replaced by `arms_technologies_used`; the calculation is described in
  the [Metric And Descriptor Notes](#metric-and-descriptor-notes) section.
- The source's calculated `cinc` column is replaced by `cinc_score`; the calculation is described in the
  [Metric And Descriptor Notes](#metric-and-descriptor-notes) section.

### Date Values

#### General Date Cleaning

- Blank strings are loaded as `null`.
- COW special codes allowed before cleaning are `-7`, `-8`, and `-9`.
- Month fields, day fields, and start-year fields load those special codes as `null` because the COW codebooks use
  negative values for ongoing, not applicable, or unknown values.
- Raw source date components are expected to be in basic valid domains before cleaning:
  - Months: `1-12`.
  - Days: `1-31`.
  - Years: `1500-2100`.
- Values outside these domains are treated as data-entry issues and documented below when accepted by the pipeline.

#### Start Dates

- Negative start-year values are loaded as `null`.
- Missing, invalid, unknown, or not-applicable start months are interpreted as January.
- Missing, invalid, unknown, or not-applicable start days are interpreted as day `1` of the resolved month.

#### End Dates

- Negative end-year values are loaded as `null` except for `-7`, which the COW codebooks document as the ongoing-war
  marker.
- End year `-7` is resolved to the source file's release date from `source_file_versions.source_release_date`. The
  source-release cap keeps ongoing rows reproducible and prevents Step 2 `Last Year` and `All Years` descriptors from
  expanding beyond the years covered by the released source data.
- Missing, invalid, unknown, or not-applicable end months are interpreted as December.
- Missing, invalid, unknown, or not-applicable end days are interpreted as the last day of the resolved month.
- End day values are capped to the last valid day of the resolved month.
  - Example: end date components with year `2012`, month `10`, and missing day resolve to `2012-10-31`.

#### Date-Estimation Flags

- `start_date_estimated` is set when a positive start year has a missing or invalid start month or day.
- `end_date_estimated` is set when the end year is the ongoing-war marker or when a positive end year has a missing or
  invalid end month or day.

### Encoding And Deduplication

- `COW-country-codes.csv` is deduplicated by `c_code`; the first row per code is retained.
- Source CSVs that need explicit encoding handling use `latin-1` by default; `Extra-StateWarData_v4.0.csv` is the
  exception and uses `cp1252`.
- Prepared copies are written as UTF-8 under `backend/.work/` before DuckDB reads them.

### Field Normalization

#### Battle-Death Estimate Flags

- `dyadic_mid_4.03.csv` side-specific fatality levels are converted during ingestion to representative battle-death
  estimate values:
  - `0 -> 0`
  - `1 -> 25`
  - `2 -> 100`
  - `3 -> 250`
  - `4 -> 500`
  - `5 -> 999`
  - `6 -> 1000`
- The source MID table stores these representative estimate values in `battle_deaths_estimated_a` and
  `battle_deaths_estimated_b`.
- After MID rows are merged into `dyads_after_mid`, `battle_deaths_estimated_a` and `battle_deaths_estimated_b` become
  binary flags:
  - `1`: the row's battle-death value comes from MID fatality-level estimates because summed source battle deaths were
    missing or zero.
  - `0`: the row's battle-death value comes from source battle-death fields or remains zero.
- Participant tables carry the merged flag as `battle_deaths_estimated`.

#### Participant Name Normalization

- Participant names are normalized only for known display and matching issues.
- The full manual mapping lives in `backend/manual/participant_name_replacements.json`.
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
  - `disno`, retained for MID matching.
  - `dyindex`
  - `outcome_a`
  - `outcome_b`
  - `outcome`
- After source date components are resolved, transformed tables carry:
  - `start_date`
  - `end_date`
  - `start_date_estimated` and `end_date_estimated` flags, which mark dates resolved from an ongoing marker or from a
    positive year with a missing or invalid month or day.
- Transformed tables do not carry the original day/month/year component columns.

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
- Before source rows with multiple date spans are collapsed, each date pair must have `start_date <= end_date`; rows
  with `start_date > end_date` should be corrected or explicitly accepted before relying on the row-level
  earliest-start/latest-end span.

### Directed Dyads And MID Records

- Participants that appear on both side 1 and side 2 in dyadic data are assigned side `3` programmatically.
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
- Anchor selection is independent by side and participant type. A participant is selected as an anchor when any one of
  these conditions is true for its side:
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

The selected anchors are then linked to every overlapping participant on the opposite side:

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
      - Example: if `concurrent_wars` is the selected node-size descriptor, a participant with `concurrent_wars = 0`
        still shows that value in the tooltip.
    - Zero values for metrics that are always useful to show.
      - Example: `battle_deaths = 0` and `battle_deaths_per_day = 0` still display when available.
  - Top-level node descriptor fields are kept for node-size dropdown options only when they have:
    - At least one positive known value.
    - Fewer than half `null` values.
    - Sizing variation across known values or between positive known values and unknown values.
      - Example: a descriptor with one positive known value and some `null` values can still be offered because it
        distinguishes known participants from unknown participants.
  - Link descriptor fields are kept only when at least one dyad has a positive value.

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
  - Rows with a `disno` found in `directed_dyadic_war.csv` inherit that COW `war_id`.
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
    - The synthetic war id uses the MID `disno` because the conflict appears in the dyadic MID records with `war = 1`,
      but no corresponding `war_id` exists for that conflict in the interstate war data.
  - The MID war-id assignments above are implemented as version-scoped source adjustments that transformations join
    explicitly.
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

- Participant names for rows with COW codes come from `country_codes.state_name`. Use the manual participant-name
  mapping only when the source name cannot resolve through a COW code:
  - Non-state participants.
  - Uncoded manual rows.
  - Source tables that do not carry `c_code` values.
- Replacement targets may match `country_codes.state_name` only for no-code source inputs.
  - Example: CO2 country names.
