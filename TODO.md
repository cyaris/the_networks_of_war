# TODO

Findings from a retrospective engineering audit (2026-08-04). Each item lists the evidence and a proposed next step;
none of these have been implemented yet.

## Critical correctness or security issues

- **Cote d'Ivoire War (`war_id = 925.0`) has a resolved date pair where `start_date > end_date`.**
  `source_intrastate_wars`, France(220)/null(-8) dyad: `start_date = 2002-12-21`, `end_date = 2002-10-17`. This
  violates the invariant documented in `AGENTS.md`/README ("rows with `start_date > end_date` require correction or
  explicit acceptance before the row-level earliest-start/latest-end span is reliable") and has no corresponding entry
  in the README's "Data-Entry Fixes And Assignment Rules" section or in any `source_*_adjustments` table. Needs source
  review to determine the correct dates, then a documented data-entry fix.
  - Validate with `pytest tests/test_step_1.py::test_source_resolved_date_pairs_do_not_start_after_they_end`.
- **15 rows in `source_interstate_war_dyads` have `null` battle-death fields with no documented acceptance.**
  Affected `disno` values: `1694`, `3587`, `3826`, `1293`, `1441` (wars `139`, `184`, `186`). None appear in the
  README or in `backend/src/sql/step_1/04_insert_source_adjustments.sql`. Needs a decision: recover values from source
  documentation and add a data-entry fix, or explicitly document these as known-unknown.
  - Validate with `pytest tests/test_step_1.py::test_required_source_battle_death_fields_are_not_null`.

## Reliability and operational issues

- **The backend pytest suite (153 tests) is never run in CI.** No workflow under `.github/workflows/` references
  `pytest` or `backend/`; `rollup.yml` only builds/lints the frontend. This means the two data-quality regressions
  above (and any future one) can persist indefinitely without any automated signal. Needs a backend CI workflow, plus
  a decision on how it obtains `backend/data/` (gitignored, normally downloaded via `--prepare-data`) — fresh download
  each run vs. a cached data directory.
- **`test_raw_source_date_components_use_valid_domains` is a permanent false positive.** It reads
  `directed_dyadic_war.csv` directly (pre-correction) and flags `warstrtmnth = 24` and `warendyr = 19118` for war
  `106`/`disno 257` — both are the exact two cases the README documents as already-applied, intentional data-entry
  fixes. Because the raw CSV is never modified, this test can never pass as currently written, which trains reviewers
  to ignore red results in this file and would mask a genuinely new raw-domain violation introduced alongside it.
  Needs a named allowlist for the two known-corrected raw values so the test only fails on new/undocumented issues.
  Should land before or alongside the CI workflow above, or CI starts red on day one for a non-issue.

## Maintainability and simplification opportunities

- **`frontend/jsconfig.json` conflicts with SvelteKit's auto-generated tsconfig.** Its `baseUrl`/`paths` settings
  duplicate what SvelteKit already generates, and every `npm run check`/`npm run build` prints a warning recommending
  removal in favor of `kit.alias`. Low priority; safe, no-behavior-change cleanup.

## Already tracked, not re-flagged

- `frontend/src/lib/static/graphData.json` is an 8.4MB generated artifact committed to git; the README already
  tracks this as a TODO to replace with a cleaner generated-data path (release asset, S3/R2 download, Git LFS, or a
  CI prebuild step). Noting only that `.git` is 23MB and will keep growing with each regeneration commit until this
  is addressed.
