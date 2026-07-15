"""DuckDB preprocessing backend for The Networks of War."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import ssl
import subprocess
import tempfile
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from utils import initialize_logger

logger = initialize_logger(__name__)

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SQL_ROOT = BACKEND_ROOT / "sql"
WORK_CSV_DIR = BACKEND_ROOT / ".work" / "csv"

DEFAULT_DATA_DIR = BACKEND_ROOT / "data"
DEFAULT_CSV_DIR = DEFAULT_DATA_DIR
DEFAULT_DB_PATH = BACKEND_ROOT / "the_networks_of_war.duckdb"
PARTICIPANT_NAME_REPLACEMENTS_PATH = BACKEND_ROOT / "manual" / "participant_name_replacements.json"
SOURCE_METADATA_PATH = BACKEND_ROOT / "manual" / "source_metadata.json"
INSPECT_SQL = SQL_ROOT / "inspect_tables.sql"
SYSTEM_CA_FILE = Path("/etc/ssl/cert.pem")
CREATE_RELATION_PATTERN = re.compile(
    r"^\s*create\s+or\s+replace\s+(?:table|view)\s+([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE | re.MULTILINE
)


def load_source_metadata() -> list[dict]:
    return json.loads(SOURCE_METADATA_PATH.read_text())


SOURCE_METADATA = load_source_metadata()


def source_prepared_files(metadata: dict) -> list[str]:
    return list(dict.fromkeys([metadata["file"], *metadata.get("prepared_files", [])]))


SOURCE_METADATA_BY_KEY = {metadata["key"]: metadata for metadata in SOURCE_METADATA}
SOURCE_FILES = {metadata["key"]: metadata["file"] for metadata in SOURCE_METADATA}
SOURCE_PREPARED_FILES = {metadata["key"]: source_prepared_files(metadata) for metadata in SOURCE_METADATA}

STEP_1_SQL = [
    "00_setup.sql",
    "step_1/00_setup.sql",
    "step_1/01_create_source_tables.sql",
    "step_1/02_insert_source_tables.sql",
    "step_1/03_create_source_adjustment_tables.sql",
    "step_1/04_insert_source_adjustments.sql",
    "step_1/05_create_reference_tables.sql",
    "step_1/06_insert_reference_tables.sql",
    "step_1/07_create_war_dyads.sql",
    "step_1/08_create_war_participants.sql",
    "step_1/09_create_dyads_after_mid.sql",
    "step_1/10_create_participants.sql",
    "step_1/11_create_dyads.sql",
    "step_1/12_create_dyad_years.sql",
    "step_1/13_create_wars.sql",
]

STEP_2_SQL = ["00_setup.sql", "step_2/01_create_source_tables.sql", "step_2/02_insert_source_tables.sql"]

STEP_1_SOURCE_KEYS = [
    "country_codes",
    "extrastate_wars",
    "interstate_mid_dyads",
    "interstate_war_dyads",
    "interstate_wars",
    "intrastate_wars",
]

STEP_2_SOURCE_KEYS = [
    "global_terrorism_database",
    "formal_alliances_directed_yearly",
    "territorial_changes",
    "forcibly_displaced_populations",
    "colonial_dependency_contiguity",
    "direct_contiguity",
    "defense_cooperation_agreements",
    "intergovernmental_organizations_dyadic",
    "diplomatic_exchange",
    "dd_revisited",
    "co_emissions_per_capita",
    "arms_technology",
    "atop_dyadic_years",
    "mtops_dyadic",
    "cow_trade_dyadic",
    "cow_trade_national",
    "national_material_capabilities",
]


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def render_sql(name: str, context: dict[str, str]) -> str:
    return (SQL_ROOT / name).read_text().format(**context)


def read_query_file(path: Path) -> str:
    return path.expanduser().read_text()


def created_relation_names(sql: str) -> list[str]:
    return list(dict.fromkeys(match.group(1) for match in CREATE_RELATION_PATTERN.finditer(sql)))


def format_query_results(
    columns: list[str],
    rows: list[tuple],
    *,
    null_text: str = "",
    cell_style: Callable[[int, str, object, str], str] | None = None,
) -> str:
    if not columns:
        return "Query completed; no tabular result."

    values = [[str(value) if value is not None else null_text for value in row] for row in rows]
    widths = [
        max(len(column), *(len(row[index]) for row in values)) if values else len(column)
        for index, column in enumerate(columns)
    ]
    header = " | ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    divider = "-+-".join("-" * width for width in widths)

    if not values:
        return "\n".join([header, divider, "(no rows)"])

    def format_cell(row_index: int, column: str, value: object, text: str, width: int) -> str:
        padded_text = text.ljust(width)

        return cell_style(row_index, column, value, padded_text) if cell_style else padded_text

    return "\n".join(
        [header, divider]
        + [
            " | ".join(
                format_cell(row_index, columns[index], raw_value, values[row_index][index], widths[index])
                for index, raw_value in enumerate(row)
            )
            for row_index, row in enumerate(rows)
        ]
    )


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


@dataclass(frozen=True)
class SourceDataIssue:
    source_key: str
    message: str

    def __str__(self) -> str:
        return f"{self.source_key}: {self.message}"


class Pipeline:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH, csv_dir: Path = DEFAULT_DATA_DIR) -> None:
        self.data_dir = csv_dir
        self.db_path = db_path

    def source_dir_for(self, source_key: str) -> Path:
        return self.data_dir / source_key

    def path_for(self, source_key: str) -> Path:
        return self.path_for_file(source_key, SOURCE_FILES[source_key])

    def path_for_file(self, source_key: str, relative: str) -> Path:
        return (self.source_dir_for(source_key) / relative).resolve()

    def paths_for(self, source_key: str) -> list[Path]:
        return [self.path_for_file(source_key, relative) for relative in SOURCE_PREPARED_FILES[source_key]]

    def download_file(self, url: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        request = urllib.request.Request(url, headers={"User-Agent": "the-networks-of-war-backend/0.1"})
        context = ssl.create_default_context(cafile=str(SYSTEM_CA_FILE)) if SYSTEM_CA_FILE.exists() else None

        try:
            with urllib.request.urlopen(request, timeout=60, context=context) as response:
                destination.write_bytes(response.read())
        except urllib.error.URLError as exc:
            if not isinstance(exc.reason, ssl.SSLCertVerificationError) or shutil.which("curl") is None:
                raise

            logger.warning("Python SSL verification failed for %s; retrying with curl", url)
            subprocess.run(
                ["curl", "--location", "--fail", "--silent", "--show-error", "--output", str(destination), url],
                check=True,
            )

    def extract_zip(self, archive_path: Path, destination: Path) -> None:
        with tempfile.TemporaryDirectory(prefix="networks-of-war-") as temp_name:
            temp_dir = Path(temp_name)

            with zipfile.ZipFile(archive_path) as archive:
                archive.extractall(temp_dir)

            children = [path for path in temp_dir.iterdir() if path.name != "__MACOSX"]
            extracted_root = children[0] if len(children) == 1 and children[0].is_dir() else temp_dir
            destination.mkdir(parents=True, exist_ok=True)

            for path in extracted_root.iterdir():
                target = destination / path.name
                if target.exists():
                    if target.is_dir():
                        shutil.rmtree(target)
                    else:
                        target.unlink()
                shutil.move(str(path), target)

    def extracted_file_path(self, source_dir: Path, filename: str) -> Path:
        direct_path = source_dir / filename
        if direct_path.exists():
            return direct_path

        matches = sorted(path for path in source_dir.rglob(filename) if path.is_file())
        if not matches:
            raise FileNotFoundError(f"Expected extracted file {filename!r} in {source_dir}")

        return matches[0]

    def convert_excel_to_csv(self, source_path: Path, destination_path: Path) -> None:
        excel_engines = {".xls": "xlrd", ".xlsx": "openpyxl"}
        engine = excel_engines.get(source_path.suffix.lower())

        if engine is None:
            raise ValueError(f"Excel conversion is only supported for .xls/.xlsx files: {source_path}")

        try:
            import pandas as pd
        except ImportError as exc:
            raise RuntimeError("Excel source conversion requires pandas, openpyxl, and xlrd to be installed.") from exc

        logger.info("Converting %s to %s", source_path, destination_path)
        prefix = source_path.read_bytes()[:256].lstrip().lower()
        if prefix.startswith(b"<!doctype html") or prefix.startswith(b"<html"):
            raise RuntimeError(f"Downloaded file looks like HTML, not Excel: {source_path}")

        destination_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe = pd.read_excel(source_path, sheet_name=0, engine=engine)
        except ImportError as exc:
            raise RuntimeError("Excel source conversion requires pandas, openpyxl, and xlrd to be installed.") from exc
        dataframe.to_csv(destination_path, index=False)

    def populate_source_dir(self, source_key: str) -> None:
        metadata = SOURCE_METADATA_BY_KEY[source_key]

        source_dir = self.source_dir_for(source_key)
        source_dir.mkdir(parents=True, exist_ok=True)
        downloads_dir = self.data_dir / "_downloads"

        for download in metadata["downloads"]:
            if download["kind"] == "zip":
                download_path = downloads_dir / download["filename"]
                logger.info("Downloading %s", download["url"])
                self.download_file(download["url"], download_path)
                self.extract_zip(download_path, source_dir)

                for nested_zip in download.get("nested_zips", []):
                    self.extract_zip(self.extracted_file_path(source_dir, nested_zip), source_dir)
            elif download["kind"] == "file":
                download_path = downloads_dir / download["filename"]
                logger.info("Downloading %s", download["url"])
                self.download_file(download["url"], download_path)
                source_path = source_dir / download["filename"]
                shutil.copy2(download_path, source_path)

                if converted_filename := download.get("converted_filename"):
                    self.convert_excel_to_csv(source_path, source_dir / converted_filename)
            else:
                raise ValueError(f"Unsupported download kind: {download['kind']}")

    def validate_source_dir(self, source_key: str) -> list[SourceDataIssue]:
        metadata = SOURCE_METADATA_BY_KEY[source_key]

        source_dir = self.source_dir_for(source_key)
        found = sorted(path.name for path in source_dir.rglob("*") if path.is_file())
        expected_files = set(SOURCE_PREPARED_FILES[source_key])
        expected_files.update(
            download["filename"] for download in metadata.get("downloads", []) if download["kind"] == "file"
        )
        issues = []

        for expected_file in sorted(expected_files):
            if not any(path.name == expected_file for path in source_dir.rglob("*") if path.is_file()):
                issues.append(
                    SourceDataIssue(
                        source_key,
                        f"expected {expected_file!r} but found: {', '.join(found) if found else '(no files)'}",
                    )
                )

        return issues

    def missing_download_url_issues(self) -> list[SourceDataIssue]:
        return [
            SourceDataIssue(metadata["key"], "source table has no configured download URL")
            for metadata in SOURCE_METADATA
            if not metadata.get("downloads")
        ]

    def prepare_data(self, recreate: bool = False, source_keys: Iterable[str] | None = None) -> None:
        if recreate and self.data_dir.exists():
            logger.info("Recreating data directory: %s", self.data_dir)
            shutil.rmtree(self.data_dir)

        self.data_dir.mkdir(parents=True, exist_ok=True)
        issues = self.missing_download_url_issues()
        source_keys = list(source_keys) if source_keys is not None else list(SOURCE_FILES)

        for source_key in source_keys:
            source_dir = self.source_dir_for(source_key)
            try:
                if recreate or not source_dir.exists():
                    self.populate_source_dir(source_key)
                else:
                    logger.info("Source directory already exists: %s", source_dir)

                source_issues = self.validate_source_dir(source_key)
                if source_issues and not recreate:
                    logger.info("Source directory is incomplete; refreshing: %s", source_dir)
                    self.populate_source_dir(source_key)
                    source_issues = self.validate_source_dir(source_key)
            except Exception as exc:
                issues.append(SourceDataIssue(source_key, str(exc)))
                logger.error("Failed to prepare source directory %s: %s", source_key, exc)
                continue

            issues.extend(source_issues)

        if issues:
            raise RuntimeError("Source data preparation issues:\n" + "\n".join(str(issue) for issue in issues))

    def prepared_path_for(self, source_key: str) -> Path:
        return self.prepared_path_for_file(source_key, SOURCE_FILES[source_key])

    def prepared_path_for_file(self, source_key: str, relative: str) -> Path:
        encoding = SOURCE_METADATA_BY_KEY[source_key].get("encoding")
        source_path = self.path_for_file(source_key, relative)

        if encoding is None or not source_path.exists():
            return source_path

        work_source_dir = WORK_CSV_DIR / source_key
        work_source_dir.mkdir(parents=True, exist_ok=True)
        path = work_source_dir / Path(relative).name
        path.write_text(source_path.read_text(encoding=encoding), encoding="utf-8")

        return path.resolve()

    def sql_context(self) -> dict[str, str]:
        context = {
            "participant_name_replacements_path": sql_literal(PARTICIPANT_NAME_REPLACEMENTS_PATH),
            **{f"{key}_path": sql_literal(self.prepared_path_for(key)) for key in SOURCE_FILES},
        }

        for metadata in SOURCE_METADATA:
            key = metadata["key"]
            context[f"{key}_source_file"] = sql_literal(Path(metadata["file"]).name)
            context[f"{key}_source_version"] = sql_literal(metadata["version"])
            for index, relative in enumerate(SOURCE_PREPARED_FILES[key], start=1):
                context[f"{key}_path_{index}"] = sql_literal(self.prepared_path_for_file(key, relative))

        return context

    def require_inputs(self, source_keys: Iterable[str] | None = None) -> None:
        source_keys = list(source_keys) if source_keys is not None else list(SOURCE_FILES)
        missing = [str(path) for key in source_keys for path in self.paths_for(key) if not path.exists()]

        if missing:
            raise FileNotFoundError("Missing required CSV inputs:\n" + "\n".join(missing))

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        return duckdb.connect(str(self.db_path))

    def drop_relation_if_exists(self, conn: duckdb.DuckDBPyConnection, relation_name: str) -> None:
        query = """
        select table_type
        from information_schema.tables
        where
            table_schema = current_schema()
            and table_name = ?
        """
        row = conn.execute(query, [relation_name]).fetchone()

        if row is None:
            return

        relation_type = "view" if row[0] == "VIEW" else "table"
        conn.execute(f"drop {relation_type} {sql_identifier(relation_name)}")

    def drop_created_relations(self, conn: duckdb.DuckDBPyConnection, sql: str) -> None:
        for relation_name in created_relation_names(sql):
            self.drop_relation_if_exists(conn, relation_name)

    def execute_sql(self, conn: duckdb.DuckDBPyConnection, name: str) -> None:
        sql = render_sql(name, self.sql_context())
        self.drop_created_relations(conn, sql)
        conn.execute(sql)

    def run_step_1(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.prepare_data(recreate=False, source_keys=STEP_1_SOURCE_KEYS)
        self.require_inputs(STEP_1_SOURCE_KEYS)

        for name in STEP_1_SQL:
            logger.info("Running %s", name)
            self.execute_sql(conn, name)

    def run_step_2(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.prepare_data(recreate=False, source_keys=STEP_2_SOURCE_KEYS)
        self.require_inputs(STEP_2_SOURCE_KEYS)

        for name in STEP_2_SQL:
            logger.info("Running %s", name)
            self.execute_sql(conn, name)

    def inspect(self, conn: duckdb.DuckDBPyConnection) -> None:
        query = """
        select 1
        from information_schema.tables
        where
            table_schema = current_schema()
            and table_name = ?
        """
        table_names = [table_name for (table_name,) in conn.execute(INSPECT_SQL.read_text()).fetchall()]

        for table_name in table_names:
            if conn.execute(query, [table_name]).fetchone() is None:
                continue

            row_count = conn.execute(f"select count(*) from {sql_identifier(table_name)}").fetchone()[0]
            logger.info(f"{table_name}: {row_count:,d}")

    def query(self, conn: duckdb.DuckDBPyConnection, sql: str) -> None:
        result = conn.execute(sql)
        columns = [column[0] for column in result.description] if result.description else []
        rows = result.fetchall() if columns else []
        logger.info("\n%s", format_query_results(columns, rows))

    def run(
        self,
        step: str = "all",
        inspect: bool = False,
        query: str | None = None,
        prepare_data: bool = False,
        recreate_data: bool = False,
        query_file: Path | None = None,
    ) -> None:
        if query is not None and query_file is not None:
            raise ValueError("Use either query or query_file, not both.")

        if prepare_data or recreate_data:
            self.prepare_data(recreate=recreate_data)

        with self.connect() as conn:
            if step in {"all", "1"}:
                self.run_step_1(conn)

            if step in {"all", "2"}:
                self.run_step_2(conn)

            if step == "3":
                raise NotImplementedError("Only preprocessing Steps 1 and 2 have been rebuilt in native SQL so far.")

            if inspect:
                self.inspect(conn)

            if query:
                self.query(conn, query)

            if query_file:
                self.query(conn, read_query_file(query_file))

        logger.info("DuckDB database: %s", self.db_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DuckDB backend for The Networks of War preprocessing.")

    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, type=Path)
    parser.add_argument("--csv-dir", dest="data_dir", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, type=Path)
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument(
        "--prepare-data", action="store_true", help="Download and validate missing source data folders."
    )
    parser.add_argument("--recreate-data", action="store_true", help="Delete and recreate the entire data directory.")
    query_group = parser.add_mutually_exclusive_group()
    query_group.add_argument(
        "--query", help="SQL query to execute against the DuckDB database after the selected step runs."
    )
    query_group.add_argument(
        "--query-file",
        type=Path,
        help="Path to a local .sql file to execute against the DuckDB database after the selected step runs.",
    )
    parser.add_argument("--step", choices=["none", "all", "1", "2", "3"], default="all")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    Pipeline(csv_dir=args.data_dir, db_path=args.db_path).run(
        step=args.step,
        inspect=args.inspect,
        query=args.query,
        query_file=args.query_file,
        prepare_data=args.prepare_data,
        recreate_data=args.recreate_data,
    )


if __name__ == "__main__":
    main()
