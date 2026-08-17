# Repository Guidance

## Documentation

- Use `../shared-automation/AGENTS.md` as the source of truth for README and Markdown documentation-style conventions.
- Keep GitHub Actions deployment notes aligned with the root `Rollup` workflow. Project-specific S3 and branch
  behavior should be documented in the README; maintainer-facing workflow conventions belong here.
- Keep README pipeline command documentation in sync with `backend/src/pipeline.py` CLI parser behavior.
- When backend metrics are added, removed, renamed, or recalculated, update
  `frontend/src/lib/static/metricDataDictionary.json` in the same change. Keep README references and any README metric
  summaries aligned with that JSON so users never see conflicting source, calculation, or unit descriptions.
- Split long README assumption sections into small, scannable subsections rather than maintaining one long bullet list.
- When documenting normalization or replacement rules, show the direction of the change with `source -> replacement`
  example subbullets instead of listing only the affected source values or categories.
- Keep frontend-specific build artifact notes near frontend setup or frontend commands instead of standalone top-level
  README sections.
- For data-related questions, consult the relevant source documentation in `backend/data/` before concluding whether a
  source value, adjustment, or transformation is correct.
- Use the backend dev dependency `pdftotext` to extract and search PDF documentation in `backend/data/` when source
  documentation is relevant. Treat PDF metadata dates as document/build metadata unless the PDF text explicitly states a
  source release date.
- Preserve the semantic difference between `null` and zero in data transformations and frontend displays. Coalesce
  missing values to `0` only when the source coverage or derivation makes the overall value known to be zero; keep
  unknown values as `null` so the frontend can show them as unknown instead of silently displaying zero.
- Put SQL select-list columns on separate lines when a `select` returns more than one column. One-line `select count(*)`
  and other single-expression selects are fine.
- In SQL `where` and `having` clauses, put multiple `and`-joined predicates on separate lines. A single `or` inside one
  predicate may stay on one line when it remains readable.
- Treat `backend/data/<source_key>/` folder names as matching source-data table keys without the `source_` prefix; the
  corresponding raw source data and PDF or JSON source documentation live inside each folder.
- Keep source adjustments minimal and only add values that downstream joins, corrections, or transformations actually
  need.
- For participant names, prefer `country_codes.state_name` for rows with COW codes. Use
  `backend/manual/participant_name_replacements.json` only when the source name cannot resolve through a COW code, such
  as non-state participants, uncoded manual rows, or source tables that do not carry `c_code` values. Replacement targets
  may match `country_codes.state_name` only for no-code source inputs.
- Do not include source columns documented as calculated in `source_` tables; derive those values in downstream
  transformation SQL when the project still needs them.
- Do not preserve backward compatibility for removed or renamed project interfaces unless the user explicitly asks for a
  migration bridge. Prefer deleting stale parameters, aliases, scripts, and docs so old callers fail clearly.

## Notebooks

Do not update `.ipynb` files while working on the backend replacement unless explicitly requested.

## GitHub Actions

- Use `../shared-automation/AGENTS.md` as the source of truth for shared GitHub Actions, reusable workflow wrapper,
  release-policy, dispatch, and automation documentation conventions.
- Before merging any pull request, explicitly inspect CodeRabbit comments and reviews and assess every still-applicable
  finding; do not merge solely because checks are green.
- Workflows must fail clearly when a requested feature requires credentials, secrets, repository variables, external
  permissions, or paid services that are not configured. Apply this to dry-run modes too unless the feature is
  explicitly documented as credential-optional.
- Project-specific rollup upload inputs include the S3 prefix and bundle file list. The shared Rollup workflow uses the
  latest `svelte-lib` `main` commit by default and resolves that branch to an exact commit SHA during each run.
- Project release naming and milestone overrides belong in `.github/release-policy.yml`.

## Release Management

- While working in this repository, evaluate whether the accumulated changes represent a meaningful release milestone.
- A release may be appropriate when the work includes a substantial user-facing feature, a major redesign or workflow change, a meaningful new integration, an important architecture change, a backward-incompatible change, a stable initial public version, a significant performance, reliability, security, accessibility, or compatibility improvement, or a coherent group of changes that materially changes how the project is used.
- Do not recommend a release for routine maintenance, formatting, minor refactoring, isolated dependency updates, or small bug fixes unless their combined impact is significant.
- Write clear, specific commit subjects that describe the actual change. Prefer plain language over release-tool syntax,
  and do not exaggerate routine maintenance as user-facing work.
- Treat upstream automation, shared workflow reference, dependency-pin, Renovate, release-policy, and local dependency ref
  maintenance as non-release work unless it changes user-facing behavior, runtime behavior, or a published API.
- When the current work appears to justify a release, state that a release may be warranted, explain the milestone in plain language, suggest a release title, suggest a tag consistent with this repository's existing convention, summarize release-note content, identify breaking changes or migration concerns, and recommend full release, prerelease, or draft status.
- Prefer app-style tags such as `v2`, `v2.1`, and `v2.2` with release titles in the form `vX.Y - Plain-English Milestone`; do not rename historical tags solely for cosmetic consistency.
- Treat work on PR or development branches as a release candidate. The final tag should normally point to the merge commit on `main` or `master`, unless the user explicitly approves releasing from another branch.
- Do not create, rename, move, or delete tags or publish a GitHub release unless the user explicitly requests it.
