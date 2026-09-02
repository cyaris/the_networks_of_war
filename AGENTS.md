# Repository Guidance

## Shared Conventions

- Inherit README and Markdown style, GitHub Actions, reusable workflow wrapper, release policy, dispatch, pull-request
  review, workflow failure, commit, and release-management rules from `../shared-automation/AGENTS.md`.

## Documentation And Data Semantics

- Keep README pipeline commands aligned with `backend/src/pipeline.py` CLI behavior. Keep frontend build-artifact notes
  near frontend setup or commands rather than in a standalone top-level section.
- When adding, removing, renaming, or changing the calculation or formula of a backend metric, update that metric's
  definition in `frontend/src/lib/static/metricDataDictionary.json` in the same change. Align README source,
  calculation, unit, and metric summaries with that file.
- Split long README assumption sections into focused subsections. Show normalization and replacement direction with
  `source -> replacement` example subbullets.
- Consult relevant source documentation under `backend/data/` before deciding whether a source value, adjustment, or
  transformation is correct. Use the backend `pdftotext` development dependency to extract searchable PDF text, and
  treat PDF metadata dates as build metadata unless the text identifies a source release date.
- Preserve the difference between `null` and zero in transformations and displays. Coalesce missing values to `0` only
  when coverage or derivation proves the value is zero; otherwise keep `null` so the frontend can show unknown values.
- Treat `backend/data/<source_key>/` as the folder for the matching `source_<source_key>` table's raw data and PDF or
  JSON documentation.
- Do not preserve removed or renamed interfaces unless the user requests a migration bridge. Delete stale parameters,
  aliases, scripts, and docs so old callers fail clearly.

## Notebooks

- Do not edit `.ipynb` files unless the task explicitly includes notebook changes.

## Rollup Delivery

- Project-specific Rollup inputs include the S3 prefix and bundle file list.
