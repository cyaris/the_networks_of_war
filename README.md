# The Networks of War

A study of networks by war using data from the Correlates of War (COW) project.

## Backend

The DuckDB backend rebuilds the first preprocessing notebook with native SQL. Python only resolves file paths,
normalizes the extra-state CSV encoding, and runs the SQL files in order.

## Setup

From `the_networks_of_war/backend`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Data Layout

Source data is downloaded into `backend/data/`, which is ignored by git. Each external source table gets its own
subdirectory named after the source key, such as `backend/data/interstate_mid_dyads/`. The manual
`war_types.csv` helper lives at `backend/manual/war_types.csv` because it is not tied to an external data version.
The extra-state CSV is copied to UTF-8 under ignored `backend/.work/` before DuckDB reads it. The generated DuckDB
database is also ignored:

- `the_networks_of_war/backend/data/`
- `the_networks_of_war/backend/.work/`
- `the_networks_of_war/backend/the_networks_of_war.duckdb`

## Pipeline Commands

From `the_networks_of_war/backend`:

```bash
python src/pipeline.py
```

Run or rebuild Step 1:

```bash
python src/pipeline.py --step 1
python src/pipeline.py --step all
```

Print table row counts after running the selected step:

```bash
python src/pipeline.py --inspect
python src/pipeline.py --step 1 --inspect
```

Query the existing DuckDB database without rebuilding it:

```bash
python src/pipeline.py --step none --query "select count(*) as row_count from dyads"
python src/pipeline.py --step none --query "select * from wars limit 10"
```

Run Step 1, then query the freshly rebuilt tables:

```bash
python src/pipeline.py --step 1 --query "select war_num, war_name from wars limit 10"
```

Use non-default input or database paths:

```bash
python src/pipeline.py --data-dir data --db-path the_networks_of_war.duckdb --step 1
```

Create missing source-data subdirectories without running a preprocessing step:

```bash
python src/pipeline.py --prepare-data --step none
```

Recreate the full ignored source-data directory:

```bash
python src/pipeline.py --recreate-data --step none
```

## Test Commands

From `the_networks_of_war/backend`:

```bash
pytest
```

Run the Step 1 expectation tests:

```bash
pytest tests/test_step_1_expectations.py
pytest tests/test_step_1_expectations.py -q
```

Run a single test or matching group of tests:

```bash
pytest tests/test_step_1_expectations.py -k "negative_date_sentinels"
pytest tests/test_step_1_expectations.py -k "date_macros or dyads"
```

Show verbose test names and failures:

```bash
pytest tests/test_step_1_expectations.py -vv
```

The Step 1 expectation tests rebuild Step 1 into a temporary DuckDB database. They skip automatically if the ignored
source files in `backend/data/` are not available locally.

## Step 1 Sources And Tables

The current backend ingests the following source files. Downloaded source subdirectories include the relevant PDFs and
supporting files from each source bundle when available.

| Table | Source CSV | Version | Download source |
| --- | --- | --- | --- |
| `source_country_codes` | `COW-country-codes.csv` | unversioned | <a href="https://correlatesofwar.org/wp-content/uploads/COW-country-codes.csv" target="_blank" rel="noopener noreferrer">COW country codes CSV</a> |
| `source_extrastate_wars` | `Extra-StateWarData_v4.0.csv` | 4.0 | <a href="https://correlatesofwar.org/wp-content/uploads/Extra-StateWarData_v4.0.csv" target="_blank" rel="noopener noreferrer">Extra-state wars CSV</a>; <a href="https://correlatesofwar.org/wp-content/uploads/Extra-StateWars_Codebook.pdf" target="_blank" rel="noopener noreferrer">extra-state wars codebook</a> |
| `source_interstate_mid_dyads` | `dyadic_mid_4.03.csv` | 4.03 | <a href="https://correlatesofwar.org/wp-content/uploads/dyadic_mid_4.03_update.zip" target="_blank" rel="noopener noreferrer">dyadic MID 4.03 update ZIP</a> |
| `source_interstate_war_dyads` | `directed_dyadic_war.csv` | unversioned | <a href="https://correlatesofwar.org/wp-content/uploads/Dyadic-Interstate-War-Dataset.zip" target="_blank" rel="noopener noreferrer">dyadic interstate war dataset ZIP</a> |
| `source_interstate_wars` | `Inter-StateWarData_v4.0.csv` | 4.0 | <a href="https://correlatesofwar.org/wp-content/uploads/Inter-StateWarData_v4.0.csv" target="_blank" rel="noopener noreferrer">inter-state wars CSV</a>; <a href="https://correlatesofwar.org/wp-content/uploads/Inter-StateWarsList.pdf" target="_blank" rel="noopener noreferrer">inter-state wars list</a>; <a href="https://correlatesofwar.org/wp-content/uploads/Inter-StateWars_Codebook.pdf" target="_blank" rel="noopener noreferrer">inter-state wars codebook</a> |
| `source_intrastate_wars` | `INTRA-STATE_State_participants v5.1 CSV.csv` | 5.1 | <a href="https://correlatesofwar.org/wp-content/uploads/Intra-State-Wars-v5.1.zip" target="_blank" rel="noopener noreferrer">intra-state wars 5.1 ZIP</a> |
| `source_war_types` | `backend/manual/war_types.csv` | local | local helper file with no external codebook |

Other files in the legacy ignored `documentation/` directory correspond to datasets that have not yet been incorporated
and were not used for the current Step 1 assumptions.

Step 1 also materializes transformed tables:

- `dyads_after_mid`
- `dyads_after_sources`
- `war_participants`

Step 1 also materializes compatibility tables:

- `dyads`
- `dyad_years`
- `participants`
- `wars`

## Ingestion Assumptions

### Source Tables

- The primary source tables listed above come directly from one CSV file, with only type coercion, column renaming,
  encoding normalization, and the data-entry fixes documented below applied during load.
- `dyadic_mid_4.03.csv` has no new columns relative to `dyadic_mid_4.02.csv` and no longer includes the 4.02 columns
  `dyad`, `abbreva`, `abbrevb`, `lastobs`, and `newar`.
- Version-scoped source adjustments live in `backend/sql/step_1/02a_create_source_adjustment_tables.sql` and
  `backend/sql/step_1/02b_apply_source_adjustments.sql`. The first file creates `source_file_versions` and adjustment
  tables; the second inserts adjustment rows for source facts that are not present in the source CSVs. Downstream
  transformations join adjustment tables to `source_file_versions` when an assignment is version-scoped. Data-entry
  fixes applied while reading source CSVs are documented below.

### Excluded Calculated Columns

- Source columns that are documented as simple calculations from other source columns are not ingested. Currently
  excluded calculated fields are `batdths` and `durindx` from unversioned `directed_dyadic_war.csv`; `durindx`, `duration`, and
  `cumdurat` from `dyadic_mid_4.03.csv`; and `WDuratDays`, `WDuratMo`, and `TotalBDeaths` from
  `INTRA-STATE_State_participants v5.1.csv`. Duration and day-count fields are excluded because they should be
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

- `COW country codes.csv` is deduplicated by `c_code`; the first row per code is retained.
- `Extra-StateWarData_v4.0.csv` is read as `cp1252` and copied to UTF-8 in `backend/.work/` before DuckDB reads it.
- `directed_dyadic_war.csv`, `dyadic_mid_4.03.csv`, `Inter-StateWarData_v4.0.csv`, and
  `INTRA-STATE_State_participants v5.1.csv` are read with `latin-1` encoding.

### Field Normalization

- `dyadic_mid_4.03.csv` side-specific fatality levels are converted during ingestion to representative battle-death
  estimates as follows: `0 -> 0`, `1 -> 25`, `2 -> 100`, `3 -> 250`, `4 -> 500`, `5 -> 999`, and `6 -> 1000`.
- Participant names are normalized only for known display and matching issues: United States, Baron von
  Ungern-Sternberg's White army, Janissaries, Turkey/Ottoman Empire/Egypt, and a small set of lower-case rebel,
  resistance, sultanate, and tribe suffixes.

## Transformation Assumptions

### Table Shape

- Directed dyadic interstate war records get war name and war type metadata from `source_interstate_wars` by `war_num`;
  synthetic MID-only wars get metadata from source adjustment tables.
- Transformed tables do not carry source-only identifiers and outcome fields (`disno`, `dyindex`, `outcome_a`,
  `outcome_b`, and `outcome`) after they are no longer needed as table outputs. MID matching still uses `disno`
  internally where needed.
- After source date components are resolved, transformed tables carry `start_date`, `end_date`, and date-estimation
  flags instead of the original day/month/year component columns.

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
  `source_interstate_mid_war_num_adjustments`.
- Missing MID `disno` to `war_num` relationships are stored in the Step 1 source adjustment tables when the current CSV
  version still needs them. If a future CSV version introduces a new unmatched MID war,
  `test_mid_dyads_resolve_all_mid_war_numbers` should fail until the source adjustment file is updated or the new source
  data is accepted as authoritative.
- Synthetic war metadata, such as the Lebanon-Israel MID conflict (`disno = 4182`) named
  `Israeli–Hezbollah Conflict (South Lebanon)`, is stored in `source_interstate_war_metadata_adjustments` and joined
  during transformation without adding partial rows to `source_interstate_wars`.

### Participant Inference

- Participants found in dyadic data but missing from `war_participants` are added to `participants` from the
  dyadic side A records.
- Missing participant sides are inferred from the opposite participant in dyadic data when that inference is unambiguous.
- Remaining version-specific participant side assignments are stored in `source_participant_side_adjustments` and joined
  during participant creation.
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

For example, in the Third Somalia War (`war_num = 940.8`), the source intra-state participant file lists six side 1
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
- Final dyads are deduplicated to one row per `war_num` and unordered participant pair. When duplicate spans exist, the
  final row keeps the earliest start date and latest end date from the unordered dyad pair.
- `dyad_years` expands `dyads` into one row per year for years in the range `1500` through `2099`.

## Data-Entry Fixes And Assignment Rules

- `directed_dyadic_war.csv`
  - Start month value `24` is treated as invalid and loaded as `null`; resolved start dates use the standard missing
    start-month default.
  - End year is corrected from original value `19118` to `1918`.
  - The World War II Thailand dyad (`war_num = 139`, `statea = 800`, `stateb = 710`) is loaded with Thailand battle
    deaths corrected from original blank `batdtha` to `5,569`. The Thailand death count comes from Wikipedia's summary
    of <a href="https://en.wikipedia.org/wiki/Thailand_in_World_War_II" target="_blank" rel="noopener noreferrer">Thailand in World War II</a>.
- `dyadic_mid_4.03.csv`
  - The source does not include COW war numbers, so rows are assigned to known wars by matching `disno` to
    `directed_dyadic_war.csv` where possible.
  - Unmatched MID disputes `3582`, `3583`, and `3585` are assigned from original missing `war_num` to World War II
    (`war_num = 139`) after manual review.
  - Unmatched MID dispute `4339` is assigned from original missing `war_num` to Africa's World War (`war_num = 905`)
    after manual review.
  - Unmatched MID dispute `4182` between Lebanon (`660`) and Israel (`666`) is assigned synthetic `war_num = 4182` and
    named `Israeli–Hezbollah Conflict (South Lebanon)`. This fake war id uses the MID `disno` because the conflict
    appears in the dyadic MID records with `war = 1`, but no corresponding `war_num` exists for it in the interstate
    war data. Lebanon is assigned participant side `1`, and Israel is assigned participant side `2`.
  - These assignments are implemented as version-scoped source adjustments, not as transformation-time fallback logic.
- `INTRA-STATE_State_participants v5.1.csv`
  - War number is corrected from original value `977` to `979`.
  - War `976` has `StartYr1` corrected from original value `2001` to `2011`.
  - Wars `942`, `990.4`, `991`, `991.4`, and `992.5` are treated as ongoing because their source war names say
    `present` or `ongoing`; `EndYr1` is set to `-7` for these rows. The original source values include `-7`, `-8`, and
    `-9`, but only `-7` is treated as an ongoing end-year marker. Other negative end-year values are loaded as `null`
    because the codebooks use them for not applicable or unknown values.
