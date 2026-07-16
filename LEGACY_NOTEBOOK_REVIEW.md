# Legacy Notebook Review

This note records the review of the three legacy preprocessing notebooks before deletion:

- `Preprocessing Step 1 - Participant and Dyadic Setup.ipynb`
- `Preprocessing Step 2 - Descriptive Data.ipynb`
- `Preprocessing Step 3 - Final Merges and D3.js Export.ipynb`

## SQL Coverage Map

The backend SQL appears to cover the production work from the notebooks.

Step 1 notebook coverage:

- Country-code setup and participant-name normalization are represented by `backend/sql/step_1/00_setup.sql`, `05_create_reference_tables.sql`, and the `participant_name_replacements` manual JSON.
- Source ingestion is represented by `01_create_source_tables.sql` and `02_insert_source_tables.sql`.
- Manual source corrections are represented by `03_create_source_adjustment_tables.sql` and `04_insert_source_adjustments.sql`, including MID war-id assignments, source war metadata adjustments, and participant-side adjustments.
- Initial participant merging is represented by `08_create_war_participants.sql`.
- Initial dyadic source merging is represented by `07_create_war_dyads.sql`.
- MID war dyad additions are represented by `09_create_dyads_after_mid.sql`.
- Missing participant additions and side inference are represented by `10_create_participants.sql`.
- Missing/floating dyad inference and canonical dyad construction are represented by `11_create_dyads.sql`.
- Year expansion is represented by `12_create_dyad_years.sql`.
- Final war-level aggregation is represented by `13_create_wars.sql`.

Step 2 notebook coverage:

- Country-year descriptors are represented by `03_create_country_year_descriptives.sql`.
- Participant-year expansion and descriptor joining are represented by `04_create_participant_year_descriptives.sql`.
- Participant first/last/all-year descriptor rollups are represented by `05_create_participant_descriptives.sql`.
- Dyad-year descriptor joining is represented by `06_create_dyad_year_descriptives.sql`.
- Dyad first/last/all-year descriptor rollups are represented by `07_create_dyadic_descriptives.sql`.

Step 3 notebook coverage:

- Final participant descriptor fill/conversion rules are represented by `01_create_final_participants.sql` and covered by `backend/tests/test_step_3.py`.
- Final dyad descriptor fill/conversion rules and canonical source/target IDs are represented by `02_create_final_dyads.sql`.
- Final war records and per-war graph JSON are represented by `03_create_final_wars.sql`.
- The frontend export is represented by `04_export_frontend_graph_data.sql`, which is executed by `Pipeline.export_frontend_data()` rather than listed in `STEP_3_SQL`.

## Logging Parity

The backend runner now emits notebook-style progress checkpoints after the corresponding SQL stages. These logs are data-driven and use current backend table counts rather than hard-coded legacy notebook counts.

The runtime logs intentionally focus on progress milestones:

- manual adjustment loading;
- participant, dyad, dyad-year, and war totals;
- MID-added dyad counts;
- inferred dyad counts;
- descriptor table totals;
- Step 3 final participant/dyad/war totals;
- frontend graph export start, count, and completion.

Data-quality assertions and semantic checks are intentionally left to pytest rather than duplicated as runtime logs.

## Uncertainty

- The notebooks used older source versions in several places, including MID `4.02`, GTD `0221`, and NMC `6.0`; the backend currently uses newer configured source files such as MID `4.03`, GTD `0522` plus 2021 H1, and NMC `7.0`. Count differences from notebook outputs are therefore expected and are not by themselves evidence of missed steps.
- The notebook helper `print_new_fields()` printed field-by-field dyadic descriptor coverage. The backend now logs table-level descriptor checkpoints, while detailed descriptor behavior is covered by tests and SQL output inspection. Add a dedicated descriptor-coverage logger if exact field-by-field runtime output is still useful.
- The Step 3 notebook wrote one JSON file per war plus `war_file_list.csv`. The backend exports a single frontend `graphData.json` payload. This appears intentional for the Svelte frontend, but it is not a one-for-one file-output replacement.
- Some notebook diagnostics, such as no-participant/no-dyad warnings, same-side dyad warnings, and floating-node warnings, are represented as data-quality expectations in tests rather than as runtime warnings.
- Several late Step 2 notebook cells were commented-out experiments or exploratory reads. I did not treat those as required production steps.
- The full backend pytest suite currently reports three Step 1 raw/source data-quality failures unrelated to the logging changes: invalid raw date components in `directed_dyadic_war.csv`, one resolved intrastate source date pair where start exceeds end, and null battle-death fields in `source_interstate_war_dyads`. The project guidance says these should remain visible until a source-data or transformation fix resolves them.
