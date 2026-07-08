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

Raw CSV files are read from `csvs/`, which is ignored by git. The extra-state CSV is copied to UTF-8 under
ignored `backend/.work/` before DuckDB reads it. The generated DuckDB database is also ignored:

- `the_networks_of_war/backend/.work/`
- `the_networks_of_war/backend/the_networks_of_war.duckdb`

## Run

From `the_networks_of_war/backend`:

```bash
python src/pipeline.py
```

Useful options:

```bash
python src/pipeline.py --step 1
python src/pipeline.py --inspect
```

## Documentation Consulted

The current backend ingests the following source files and uses the matching documentation in `documentation/` where available:

- `COW country codes.csv`: `Entities.pdf`
- `directed_dyadic_war.csv`: `The Directed Dyadic Interstate War Dataset Codebook.pdf`
- `dyadic_mid_4.02.csv`: `Dyadic MID Codebook V4.0.pdf`
- `Extra-StateWarData_v4.0.csv`: `Extra-StateWars_Codebook.pdf`
- `Inter-StateWarData_v4.0.csv`: `MII_v4.0_Codebook.pdf`
- `INTRA-STATE_State_participants v5.1.csv`: `Codebook for Intra-state v5.1 2.9.20.pdf` and `Description of Intra-state v5.1.pdf`
- `war_types.csv`: local helper file with no external codebook

Other files in `documentation/` correspond to datasets that have not yet been incorporated and were not used for
the current Step 1 assumptions.

## Step 1 Tables

Step 1 creates typed source tables loaded from CSV files:

- `source_country_codes`
- `source_directed_dyadic_war`
- `source_dyadic_mid`
- `source_extrastate_wars`
- `source_interstate_wars`
- `source_intrastate_wars`
- `source_war_types`

It also materializes transformed Step 1 tables:

- `war_dyads`
- `war_participants`
- `dyads_after_mid`

It also materializes compatibility tables:

- `initial_dyads`
- `initial_participants`
- `initial_wars`

## Ingestion Assumptions

- Source table names beginning with `source_` mean the table comes directly from one CSV file, with only type coercion,
  column renaming, encoding normalization, and explicit data-entry fixes applied during load.
- Blank strings are loaded as null. Text values `-7`, `-8`, and `-9` are also treated as null because the COW codebooks
  use negative values for ongoing, not applicable, or unknown values.
- Numeric date fields keep their negative sentinel values during load so date macros can distinguish ongoing wars from
  unknown or not-applicable dates.
- Unknown or not-applicable start month/day values are interpreted as January 1 for date construction.
- Unknown or not-applicable end month/day values are interpreted as December 31 for date construction.
- Day values are capped to the last valid day of the resolved month.
- End year `-7` is treated as ongoing and resolved to December 31 of the current year at pipeline runtime.
- A date is flagged as estimated when the year is ongoing (`-7`) or when a positive year has an unknown month or day
  marker (`-9`).
- `COW country codes.csv` is deduplicated by `c_code`; the first row per code is retained.
- `Extra-StateWarData_v4.0.csv` is read as `cp1252` and copied to UTF-8 in `backend/.work/` before DuckDB reads it.
- `directed_dyadic_war.csv`, `dyadic_mid_4.02.csv`, `Inter-StateWarData_v4.0.csv`, and
  `INTRA-STATE_State_participants v5.1.csv` are read with `latin-1` encoding.
- Participant names are normalized only for known display and matching issues: United States, Baron von
  Ungern-Sternberg's White army, Janissaries, Turkey/Ottoman Empire/Egypt, and a small set of lower-case rebel,
  resistance, sultanate, and tribe suffixes.

## Transformation Assumptions

- Directed dyadic interstate war records get war name and war type metadata from `source_interstate_wars` by `war_num`.
- Extra-state and intra-state war dyads are treated as side A versus side B rows, with side A assigned side `1` and
  side B assigned side `2`.
- Extra-state and intra-state participant rows are derived from both sides of the corresponding dyad rows.
- For extra-state wars, the battle-death total is calculated as side A battle deaths plus non-state deaths.
- For extra-state and intra-state rows with multiple date spans, the pipeline uses the earliest start date and latest
  end date as the war dyad/participant span.
- Interstate participant dates use the earliest start date and latest end date across the two source date spans.
- Participants that appear on both side 1 and side 2 in dyadic data are assigned side `3` programmatically.
- `dyads_after_mid` makes source war dyads directed by adding a reversed copy of each dyad.
- Only dyadic MID records with `war = 1` are incorporated.
- Dyadic MID fatality levels are converted to representative values as follows: `0 -> 0`, `1 -> 25`, `2 -> 100`,
  `3 -> 250`, `4 -> 500`, `5 -> 999`, and `6 -> 1000`.
- Existing battle-death values take precedence over MID fatality estimates. MID estimates are used when summed source
  battle deaths are null or zero and summed estimates are positive.
- MID dyads are assigned to known wars by `disno` when possible.
- MID dyads that cannot be assigned by `disno` are assigned to World War II (`war_num = 139`) when their start year is
  1945 or earlier.
- A small set of unmatched MID dyads among COW codes `483`, `490`, `500`, `517`, `540`, `552`, and `565` are assigned
  to war `905`.
- An additional directed pair for World War II dispute `2581` between COW codes `220` and `255` is preserved as a
  special case.
- Participants found in dyadic data but missing from `war_participants` are added to `initial_participants` from the
  dyadic side A records.
- Missing participant sides are inferred from the opposite participant in dyadic data when that inference is unambiguous.
- Inferred dyads are created by choosing anchor participants for each war. The anchor selection prefers a single
  participant on a side, then a single non-state participant, then a single state participant.
- War `820` additionally treats France and the Democratic Republic of the Congo as anchor participants.
- Inferred dyads are only created where the anchor and opposing participant date ranges overlap.
- `initial_dyads` expands dyads into one row per year for years in the range `1500` through `2099`.

## Data-Entry Fixes

- `directed_dyadic_war.csv`
  - Start month `24` is corrected to `12`.
  - End year `19118` is corrected to `1918`.
  - The World War II Thailand dyad (`war_num = 139`, `statea = 800`, `stateb = 710`) is loaded with Thailand battle
    deaths set to `5,569` and total battle deaths set to `6,569`.
- `INTRA-STATE_State_participants v5.1.csv`
  - War number `977` is corrected to `979`.
  - War `976` has `StartYr1` corrected to `2011`.
  - Wars `942`, `990.4`, `991`, `991.4`, and `992.5` are treated as ongoing by setting `EndYr1` to `-7`.
