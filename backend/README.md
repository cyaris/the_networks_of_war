# The Networks of War Backend

DuckDB ETL backend for rebuilding the preprocessing pipeline that previously lived in the three notebooks.

The current backend rebuilds Step 1 with native DuckDB SQL. Python only resolves file paths and runs the SQL files
in order.

## Setup

From `the_networks_of_war/backend`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Data Layout

Raw CSV files are read from `../csvs/`, which is already ignored by git. The extra-state CSV is copied to UTF-8 under
ignored `backend/.work/` before DuckDB reads it. The generated DuckDB database is also ignored:

- `the_networks_of_war/backend/.work/`
- `the_networks_of_war/backend/the_networks_of_war.duckdb`

## Run

```bash
python src/pipeline.py
```

Useful options:

```bash
python src/pipeline.py --step 1
python src/pipeline.py --inspect
```

## Tables

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

It also materializes compatibility tables:

- `initial_dyads`
- `initial_participants`
- `initial_wars`
