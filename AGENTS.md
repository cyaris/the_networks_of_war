# Repository Guidance

## Documentation

- Keep README link behavior intentional and consistent. Use standard Markdown links by default, and use HTML anchors with `target="_blank"` and `rel="noopener noreferrer"` only when links should explicitly open in a new tab.
- Keep README pipeline command documentation in sync with `backend/src/pipeline.py` CLI parser behavior.
- Split long README assumption sections into small, scannable subsections rather than maintaining one long bullet list.

## Notebooks

- Do not update `.ipynb` files while working on the backend replacement unless explicitly requested.
