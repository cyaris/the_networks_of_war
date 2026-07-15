from __future__ import annotations

import sys
import zipfile
from pathlib import Path

import pandas as pd
import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = BACKEND_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

import pipeline as pipeline_module  # noqa: E402
from pipeline import Pipeline, format_query_results  # noqa: E402


def test_run_executes_query_from_local_sql_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    query_file = tmp_path / "query.sql"
    query_file.write_text("select 42 as answer\n")
    executed_queries = []

    def record_query(self, conn, sql: str) -> None:
        executed_queries.append(sql)

    monkeypatch.setattr(Pipeline, "query", record_query)

    Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).run(step="none", query_file=query_file)

    assert executed_queries == ["select 42 as answer\n"]


def test_format_query_results_can_show_and_style_problem_cells() -> None:
    def style_problem_cell(row_index: int, column: str, value: object, text: str) -> str:
        if row_index == 0 and column == "value" and value is None:
            return f"<red>{text}</red>"

        return text

    formatted = format_query_results(["id", "value"], [(1, None)], null_text="null", cell_style=style_problem_cell)

    assert "<red>null </red>" in formatted


def test_run_rejects_query_text_and_query_file_together(tmp_path: Path) -> None:
    query_file = tmp_path / "query.sql"
    query_file.write_text("select 42 as answer\n")

    with pytest.raises(ValueError, match="Use either query or query_file"):
        Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).run(
            step="none", query="select 1", query_file=query_file
        )


def test_convert_excel_to_csv_writes_first_sheet_as_csv(tmp_path: Path) -> None:
    source_path = tmp_path / "source.xlsx"
    destination_path = tmp_path / "converted.csv"
    pd.DataFrame([{"eventid": "202101010001", "iyear": 2021}]).to_excel(source_path, index=False)

    Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).convert_excel_to_csv(source_path, destination_path)

    assert destination_path.read_text() == "eventid,iyear\n202101010001,2021\n"


def test_convert_excel_to_csv_uses_xlrd_for_xls(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_path = tmp_path / "source.xls"
    destination_path = tmp_path / "converted.csv"
    source_path.write_bytes(b"placeholder")
    captured = {}

    def read_excel(source, *, sheet_name, engine):
        captured["source"] = source
        captured["sheet_name"] = sheet_name
        captured["engine"] = engine
        return pd.DataFrame([{"ccode": 2, "year": 2008}])

    monkeypatch.setattr(pd, "read_excel", read_excel)

    Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).convert_excel_to_csv(source_path, destination_path)

    assert captured == {"source": source_path, "sheet_name": 0, "engine": "xlrd"}
    assert destination_path.read_text() == "ccode,year\n2,2008\n"


def test_convert_excel_to_csv_rejects_html_download(tmp_path: Path) -> None:
    source_path = tmp_path / "source.xls"
    destination_path = tmp_path / "converted.csv"
    source_path.write_text("<!DOCTYPE html><html></html>")

    with pytest.raises(RuntimeError, match="looks like HTML"):
        Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).convert_excel_to_csv(source_path, destination_path)


def test_nested_zip_can_be_extracted_from_source_directory(tmp_path: Path) -> None:
    inner_archive_path = tmp_path / "NMC-v7-supplemental.zip"
    outer_archive_path = tmp_path / "NMCv7.zip"
    source_dir = tmp_path / "source"

    with zipfile.ZipFile(inner_archive_path, "w") as archive:
        archive.writestr("NMC-70-wsupplementary.csv", "ccode,year\n2,1816\n")

    with zipfile.ZipFile(outer_archive_path, "w") as archive:
        archive.writestr("readme.txt", "outer archive")
        archive.write(inner_archive_path, "supplemental/NMC-v7-supplemental.zip")

    pipeline = Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path)
    pipeline.extract_zip(outer_archive_path, source_dir)
    pipeline.extract_zip(pipeline.extracted_file_path(source_dir, "NMC-v7-supplemental.zip"), source_dir)

    assert (source_dir / "NMC-70-wsupplementary.csv").read_text() == "ccode,year\n2,1816\n"


def test_prepared_path_for_missing_encoded_source_returns_missing_source_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(pipeline_module.SOURCE_FILES, "encoded_missing", "missing.csv")
    monkeypatch.setitem(pipeline_module.SOURCE_METADATA_BY_KEY, "encoded_missing", {"encoding": "latin-1"})

    actual = Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).prepared_path_for("encoded_missing")

    assert actual == (tmp_path / "encoded_missing" / "missing.csv").resolve()


def test_require_inputs_checks_all_prepared_files(tmp_path: Path) -> None:
    source_dir = tmp_path / "global_terrorism_database"
    source_dir.mkdir()
    (source_dir / "globalterrorismdb_0522dist.csv").write_text("eventid\n1\n")

    with pytest.raises(FileNotFoundError, match="globalterrorismdb_2021Jan-June_1222dist.csv"):
        Pipeline(db_path=tmp_path / "test.duckdb", csv_dir=tmp_path).require_inputs(["global_terrorism_database"])
