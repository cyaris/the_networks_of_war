# Repository Guidance

## Documentation

- Keep README link behavior intentional and consistent. Use standard Markdown links by default, and use HTML anchors with `target="_blank"` and `rel="noopener noreferrer"` only when links should explicitly open in a new tab.
- Keep README pipeline command documentation in sync with `backend/src/pipeline.py` CLI parser behavior.
- When backend metrics are added, removed, renamed, or recalculated, update
  `frontend/src/lib/static/metricDataDictionary.json` in the same change. Keep README references and any README metric
  summaries aligned with that JSON so users never see conflicting source, calculation, or unit descriptions.
- Split long README assumption sections into small, scannable subsections rather than maintaining one long bullet list.
- Prefer bullets and subbullets over inline listed-out prose in README and Markdown documentation when they make
  concrete technical lists easier to scan, especially files, paths, source values/codes, table names, column names,
  commands, and metrics. Keep short phrase lists in prose when bullets would make the text feel fragmented, and keep
  tables when they make dense reference data easier to compare.
- Do not use bullets solely to separate README command examples or other code-block sections. Introduce each code block
  with a short prose sentence instead.
- Do not place separate bullet groups directly next to each other when they document different concepts, because
  Markdown can render them as one list. Use prose, a table, or an explicit subsection label to separate the concepts.
- Use prose instead of a bullet list when a section would contain only one bullet. Prefer prose over subbullets when a
  nested list would have only two items, unless the pair needs extra visual separation to avoid ambiguity.
- When an example supports an existing README bullet, make the example a subbullet under that point even when there is
  only one example. Use `Example:` for one example and `Examples:` for multiple examples.
- Let table-of-contents nesting reflect the document structure even when a section has only two children.
- When documenting normalization or replacement rules, show the direction of the change with `source -> replacement`
  example subbullets instead of listing only the affected source values or categories.
- Keep frontend-specific build artifact notes under the README frontend section instead of as standalone top-level
  sections.
- In Markdown files, always format the literal as `null`.
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
- Do not include source columns documented as calculated in `source_` tables; derive those values in downstream
  transformation SQL when the project still needs them.
- Do not preserve backward compatibility for removed or renamed project interfaces unless the user explicitly asks for a
  migration bridge. Prefer deleting stale parameters, aliases, scripts, and docs so old callers fail clearly.

## Notebooks

Do not update `.ipynb` files while working on the backend replacement unless explicitly requested.
