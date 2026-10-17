# Repository Guidance

## Shared Conventions

- Inherit README and Markdown style, link verification, GitHub Actions, reusable workflow, release policy, dispatch,
  pull-request review and merge authorization, workflow failure, commit, and release-management rules from the
  shared topics in `../shared-automation/agents/`, which `CLAUDE.md` imports:
  - `documentation.md`
  - `git-and-pull-requests.md`
  - `workflows.md`

## Data Generation

- Regenerate and commit `frontend/src/lib/static/graphData.json` after changing the backend pipeline, its SQL, or
  source data under `backend/data/`.

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
- Treat `backend/data/<source_key>/` as the folder for the matching `source_<source_key>` table's raw data and PDF or
  JSON documentation.
- Do not preserve removed or renamed interfaces unless the user requests a migration bridge. Delete stale parameters,
  aliases, scripts, and docs so old callers fail clearly.
