from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = BACKEND_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import Pipeline  # noqa: E402


def test_run_executes_query_from_local_sql_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    query_file = tmp_path / "query.sql"
    query_file.write_text("select 42 as answer\n")
    executed_queries = []

    def record_query(self, conn, sql: str) -> None:
        executed_queries.append(sql)

    monkeypatch.setattr(Pipeline, "query", record_query)

    Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).run(step="none", query_file=query_file)

    assert executed_queries == ["select 42 as answer\n"]


def test_run_rejects_query_text_and_query_file_together(tmp_path: Path) -> None:
    query_file = tmp_path / "query.sql"
    query_file.write_text("select 42 as answer\n")

    with pytest.raises(ValueError, match="Use either query or query_file"):
        Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).run(
            step="none", query="select 1", query_file=query_file
        )
