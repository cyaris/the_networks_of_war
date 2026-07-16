from __future__ import annotations

import argparse
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


def test_source_data_arguments_reject_csv_dir_alias() -> None:
    parser = argparse.ArgumentParser()
    pipeline_module.add_source_data_arguments(parser)

    with pytest.raises(SystemExit):
        parser.parse_args(["--csv-dir", "data"])


def test_duckdb_arguments_default_to_build_and_reject_step_alias() -> None:
    parser = argparse.ArgumentParser()
    pipeline_module.add_duckdb_arguments(parser)

    assert parser.parse_args([]).build is True
    assert parser.parse_args(["--no-build"]).build is False
    with pytest.raises(SystemExit):
        parser.parse_args(["--step", "3"])


def test_run_builds_all_steps_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    executed_steps = []

    def record_step_1(self, conn) -> None:
        executed_steps.append("1")

    def record_step_2(self, conn) -> None:
        executed_steps.append("2")

    def record_step_3(self, conn) -> None:
        executed_steps.append("3")

    monkeypatch.setattr(Pipeline, "run_step_1", record_step_1)
    monkeypatch.setattr(Pipeline, "run_step_2", record_step_2)
    monkeypatch.setattr(Pipeline, "run_step_3", record_step_3)

    Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).run()

    assert executed_steps == ["1", "2", "3"]


def test_run_executes_query_from_local_sql_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    query_file = tmp_path / "query.sql"
    query_file.write_text("select 42 as answer\n")
    executed_queries = []

    def record_query(self, conn, sql: str) -> None:
        executed_queries.append(sql)

    monkeypatch.setattr(Pipeline, "query", record_query)

    Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).run(build=False, query_file=query_file)

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
        Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).run(
            build=False, query="select 1", query_file=query_file
        )


def test_convert_excel_to_csv_writes_first_sheet_as_csv(tmp_path: Path) -> None:
    source_path = tmp_path / "source.xlsx"
    destination_path = tmp_path / "converted.csv"
    pd.DataFrame([{"eventid": "202101010001", "iyear": 2021}]).to_excel(source_path, index=False)

    Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).convert_excel_to_csv(source_path, destination_path)

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

    Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).convert_excel_to_csv(source_path, destination_path)

    assert captured == {"source": source_path, "sheet_name": 0, "engine": "xlrd"}
    assert destination_path.read_text() == "ccode,year\n2,2008\n"


def test_convert_excel_to_csv_rejects_html_download(tmp_path: Path) -> None:
    source_path = tmp_path / "source.xls"
    destination_path = tmp_path / "converted.csv"
    source_path.write_text("<!DOCTYPE html><html></html>")

    with pytest.raises(RuntimeError, match="looks like HTML"):
        Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).convert_excel_to_csv(
            source_path, destination_path
        )


def test_nested_zip_can_be_extracted_from_source_directory(tmp_path: Path) -> None:
    inner_archive_path = tmp_path / "NMC-v7-supplemental.zip"
    outer_archive_path = tmp_path / "NMCv7.zip"
    source_dir = tmp_path / "source"

    with zipfile.ZipFile(inner_archive_path, "w") as archive:
        archive.writestr("NMC-70-wsupplementary.csv", "ccode,year\n2,1816\n")

    with zipfile.ZipFile(outer_archive_path, "w") as archive:
        archive.writestr("readme.txt", "outer archive")
        archive.write(inner_archive_path, "supplemental/NMC-v7-supplemental.zip")

    pipeline = Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path)
    pipeline.extract_zip(outer_archive_path, source_dir)
    pipeline.extract_zip(pipeline.extracted_file_path(source_dir, "NMC-v7-supplemental.zip"), source_dir)

    assert (source_dir / "NMC-70-wsupplementary.csv").read_text() == "ccode,year\n2,1816\n"


def test_populate_source_dir_keeps_only_csv_and_pdf_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    metadata = {
        "key": "sample_source",
        "file": "sample.csv",
        "version": "1",
        "downloads": [
            {"url": "https://example.test/source.zip", "kind": "zip", "filename": "source.zip"},
            {"url": "https://example.test/codebook.pdf", "kind": "file", "filename": "codebook.pdf"},
            {
                "url": "https://example.test/raw.xlsx",
                "kind": "file",
                "filename": "raw.xlsx",
                "converted_filename": "raw.csv",
            },
        ],
    }
    monkeypatch.setitem(pipeline_module.SOURCE_METADATA_BY_KEY, "sample_source", metadata)
    monkeypatch.setitem(pipeline_module.SOURCE_PREPARED_FILES, "sample_source", ["sample.csv", "raw.csv"])

    def download_file(url: str, destination: Path) -> None:
        if destination.suffix == ".zip":
            with zipfile.ZipFile(destination, "w") as archive:
                archive.writestr("sample.csv", "id\n1\n")
                archive.writestr("sample.dta", "not retained")
                archive.writestr("nested/readme.txt", "not retained")
        elif destination.suffix == ".pdf":
            destination.write_bytes(b"%PDF-1.7")
        else:
            destination.write_bytes(b"workbook")

    def convert_excel_to_csv(source_path: Path, destination_path: Path) -> None:
        destination_path.write_text("id\n2\n")

    pipeline = Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path)
    monkeypatch.setattr(pipeline, "download_file", download_file)
    monkeypatch.setattr(pipeline, "convert_excel_to_csv", convert_excel_to_csv)

    pipeline.populate_source_dir("sample_source")

    source_files = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file())

    assert source_files == [
        Path("sample_source/codebook.pdf"),
        Path("sample_source/raw.csv"),
        Path("sample_source/sample.csv"),
    ]
    assert not (tmp_path / "_downloads").exists()
    assert pipeline.validate_source_dir("sample_source") == []


def test_prepare_data_prunes_existing_complete_source_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    metadata = {
        "key": "sample_source",
        "file": "sample.csv",
        "version": "1",
        "downloads": [{"url": "https://example.test/sample.csv", "kind": "file", "filename": "sample.csv"}],
    }
    source_dir = tmp_path / "sample_source"
    source_dir.mkdir()
    (source_dir / "sample.csv").write_text("id\n1\n")
    (source_dir / "sample.dta").write_text("not retained")

    monkeypatch.setitem(pipeline_module.SOURCE_METADATA_BY_KEY, "sample_source", metadata)
    monkeypatch.setitem(pipeline_module.SOURCE_PREPARED_FILES, "sample_source", ["sample.csv"])

    Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).prepare_data(source_keys=["sample_source"])

    assert sorted(path.name for path in source_dir.iterdir()) == ["sample.csv"]


def test_prepared_path_for_missing_encoded_source_returns_missing_source_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(pipeline_module.SOURCE_FILES, "encoded_missing", "missing.csv")
    monkeypatch.setitem(pipeline_module.SOURCE_METADATA_BY_KEY, "encoded_missing", {"encoding": "latin-1"})

    actual = Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).prepared_path_for("encoded_missing")

    assert actual == (tmp_path / "encoded_missing" / "missing.csv").resolve()


def test_require_inputs_checks_all_prepared_files(tmp_path: Path) -> None:
    source_dir = tmp_path / "global_terrorism_database"
    source_dir.mkdir()
    (source_dir / "globalterrorismdb_0522dist.csv").write_text("eventid\n1\n")

    with pytest.raises(FileNotFoundError, match="globalterrorismdb_2021Jan-June_1222dist.csv"):
        Pipeline(db_path=tmp_path / "test.duckdb", data_dir=tmp_path).require_inputs(["global_terrorism_database"])
