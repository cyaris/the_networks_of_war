# Repository Guidance

## Documentation

- Keep README link behavior intentional and consistent. Use standard Markdown links by default, and use HTML anchors with `target="_blank"` and `rel="noopener noreferrer"` only when links should explicitly open in a new tab.
- Keep README pipeline command documentation in sync with `backend/src/pipeline.py` CLI parser behavior.
- Split long README assumption sections into small, scannable subsections rather than maintaining one long bullet list.
- For data-related questions, consult the relevant source documentation in `backend/data/` before concluding whether a
  source value, adjustment, or transformation is correct.
- Preserve the semantic difference between `null` and zero in data transformations and frontend displays. Coalesce
  missing values to `0` only when the source coverage or derivation makes the overall value known to be zero; keep
  unknown values as `null` so the frontend can show them as unknown instead of silently displaying zero.
- Treat `backend/data/<source_key>/` folder names as matching source-data table keys without the `source_` prefix; the
  corresponding raw source data and PDF or JSON source documentation live inside each folder.
- Keep source adjustments minimal and only add values that downstream joins, corrections, or transformations actually
  need.
- Do not preserve backward compatibility for removed or renamed project interfaces unless the user explicitly asks for a
  migration bridge. Prefer deleting stale parameters, aliases, scripts, and docs so old callers fail clearly.

## Notebooks

- Do not update `.ipynb` files while working on the backend replacement unless explicitly requested.
