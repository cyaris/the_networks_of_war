"""DuckDB preprocessing backend for The Networks of War."""

from __future__ import annotations

import argparse
import re
import shutil
import ssl
import subprocess
import tempfile
import urllib.error
import urllib.request
import zipfile
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
INSPECT_SQL = SQL_ROOT / "inspect_tables.sql"
SYSTEM_CA_FILE = Path("/etc/ssl/cert.pem")
COW_UPLOADS_URL = "https://correlatesofwar.org/wp-content/uploads"
CREATE_RELATION_PATTERN = re.compile(
    r"^\s*create\s+or\s+replace\s+(?:table|view)\s+([A-Za-z_][A-Za-z0-9_]*)\b",
    re.IGNORECASE | re.MULTILINE,
)

SOURCE_METADATA = [
    {
        "key": "country_codes",
        "file": "COW-country-codes.csv",
        "version": "unversioned",
        "downloads": [
            {"url": f"{COW_UPLOADS_URL}/COW-country-codes.csv", "kind": "file", "filename": "COW-country-codes.csv"}
        ],
    },
    {
        "key": "extrastate_wars",
        "file": "Extra-StateWarData_v4.0.csv",
        "version": "4.0",
        "downloads": [
            {
                "url": f"{COW_UPLOADS_URL}/Extra-StateWarData_v4.0.csv",
                "kind": "file",
                "filename": "Extra-StateWarData_v4.0.csv",
            },
            {
                "url": f"{COW_UPLOADS_URL}/Extra-StateWars_Codebook.pdf",
                "kind": "file",
                "filename": "Extra-StateWars_Codebook.pdf",
            },
        ],
    },
    {
        "key": "interstate_mid_dyads",
        "file": "dyadic_mid_4.03.csv",
        "version": "4.03",
        "downloads": [
            {
                "url": f"{COW_UPLOADS_URL}/dyadic_mid_4.03_update.zip",
                "kind": "zip",
                "filename": "dyadic_mid_4.03_update.zip",
            }
        ],
    },
    {
        "key": "interstate_war_dyads",
        "file": "directed_dyadic_war.csv",
        "version": "unversioned",
        "downloads": [
            {
                "url": f"{COW_UPLOADS_URL}/Dyadic-Interstate-War-Dataset.zip",
                "kind": "zip",
                "filename": "Dyadic-Interstate-War-Dataset.zip",
            }
        ],
    },
    {
        "key": "interstate_wars",
        "file": "Inter-StateWarData_v4.0.csv",
        "version": "4.0",
        "downloads": [
            {
                "url": f"{COW_UPLOADS_URL}/Inter-StateWarData_v4.0.csv",
                "kind": "file",
                "filename": "Inter-StateWarData_v4.0.csv",
            },
            {
                "url": f"{COW_UPLOADS_URL}/Inter-StateWarsList.pdf",
                "kind": "file",
                "filename": "Inter-StateWarsList.pdf",
            },
            {
                "url": f"{COW_UPLOADS_URL}/Inter-StateWars_Codebook.pdf",
                "kind": "file",
                "filename": "Inter-StateWars_Codebook.pdf",
            },
        ],
    },
    {
        "key": "intrastate_wars",
        "file": "INTRA-STATE_State_participants v5.1 CSV.csv",
        "version": "5.1",
        "downloads": [
            {
                "url": f"{COW_UPLOADS_URL}/Intra-State-Wars-v5.1.zip",
                "kind": "zip",
                "filename": "Intra-State-Wars-v5.1.zip",
            }
        ],
    },
    {"key": "war_types", "file": "manual/war_types.csv", "version": "local", "local": True},
]

SOURCE_METADATA_BY_KEY = {metadata["key"]: metadata for metadata in SOURCE_METADATA}
SOURCE_FILES = {metadata["key"]: metadata["file"] for metadata in SOURCE_METADATA}

SOURCE_DEFAULT_ENCODING = None
SOURCE_ENCODING_OVERRIDES = {"extrastate_wars": "cp1252"}

STEP_1_SQL = [
    "step_1/00_setup.sql",
    "step_1/01_create_source_tables.sql",
    "step_1/02_insert_source_tables.sql",
    "step_1/02a_create_source_adjustment_tables.sql",
    "step_1/02b_apply_source_adjustments.sql",
    "step_1/03_create_reference_tables.sql",
    "step_1/04_create_war_dyads.sql",
    "step_1/05_create_war_participants.sql",
    "step_1/06_create_dyads_after_mid.sql",
    "step_1/07_create_participants.sql",
    "step_1/08_create_dyads.sql",
    "step_1/09_create_dyad_years.sql",
    "step_1/10_create_wars.sql",
]


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def render_sql(name: str, context: dict[str, str]) -> str:
    return (SQL_ROOT / name).read_text().format(**context)


def read_query_file(path: Path) -> str:
    return path.expanduser().read_text()


def created_relation_names(sql: str) -> list[str]:
    return list(dict.fromkeys(match.group(1) for match in CREATE_RELATION_PATTERN.finditer(sql)))


def format_query_results(columns: list[str], rows: list[tuple]) -> str:
    if not columns:
        return "Query completed; no tabular result."

    values = [[str(value) if value is not None else "" for value in row] for row in rows]
    widths = [
        max(len(column), *(len(row[index]) for row in values)) if values else len(column)
        for index, column in enumerate(columns)
    ]
    header = " | ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    divider = "-+-".join("-" * width for width in widths)

    if not values:
        return "\n".join([header, divider, "(no rows)"])

    return "\n".join(
        [header, divider]
        + [" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)) for row in values]
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
        relative = SOURCE_FILES[source_key]

        if SOURCE_METADATA_BY_KEY[source_key].get("local"):
            return (BACKEND_ROOT / relative).resolve()

        return (self.source_dir_for(source_key) / relative).resolve()

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

    def populate_source_dir(self, source_key: str) -> None:
        metadata = SOURCE_METADATA_BY_KEY[source_key]

        if metadata.get("local"):
            return

        source_dir = self.source_dir_for(source_key)
        source_dir.mkdir(parents=True, exist_ok=True)
        downloads_dir = self.data_dir / "_downloads"

        for download in metadata["downloads"]:
            download_path = downloads_dir / download["filename"]
            logger.info("Downloading %s", download["url"])
            self.download_file(download["url"], download_path)

            if download["kind"] == "zip":
                self.extract_zip(download_path, source_dir)
            elif download["kind"] == "file":
                shutil.copy2(download_path, source_dir / download["filename"])
            else:
                raise ValueError(f"Unsupported download kind: {download['kind']}")

    def validate_source_dir(self, source_key: str) -> list[SourceDataIssue]:
        metadata = SOURCE_METADATA_BY_KEY[source_key]

        if metadata.get("local"):
            return []

        source_dir = self.source_dir_for(source_key)
        found = sorted(path.name for path in source_dir.rglob("*") if path.is_file())
        expected_files = {metadata["file"]}
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
            if not metadata.get("local") and not metadata.get("downloads")
        ]

    def prepare_data(self, recreate: bool = False) -> None:
        if recreate and self.data_dir.exists():
            logger.info("Recreating data directory: %s", self.data_dir)
            shutil.rmtree(self.data_dir)

        self.data_dir.mkdir(parents=True, exist_ok=True)
        issues = self.missing_download_url_issues()

        for metadata in SOURCE_METADATA:
            source_key = metadata["key"]

            if metadata.get("local"):
                continue

            source_dir = self.source_dir_for(source_key)
            if recreate or not source_dir.exists():
                self.populate_source_dir(source_key)
            else:
                logger.info("Source directory already exists: %s", source_dir)

            issues.extend(self.validate_source_dir(source_key))

        if issues:
            raise RuntimeError("Source data preparation issues:\n" + "\n".join(str(issue) for issue in issues))

    def prepared_path_for(self, source_key: str) -> Path:
        encoding = SOURCE_ENCODING_OVERRIDES.get(source_key, SOURCE_DEFAULT_ENCODING)

        if encoding is None:
            return self.path_for(source_key)

        WORK_CSV_DIR.mkdir(parents=True, exist_ok=True)
        path = WORK_CSV_DIR / self.path_for(source_key).name
        path.write_text(self.path_for(source_key).read_text(encoding=encoding), encoding="utf-8")

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

        return context

    def require_inputs(self) -> None:
        missing = [str(self.path_for(key)) for key in SOURCE_FILES if not self.path_for(key).exists()]

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
        self.prepare_data(recreate=False)
        self.require_inputs()

        for name in STEP_1_SQL:
            logger.info("Running %s", name)
            self.execute_sql(conn, name)

    def inspect(self, conn: duckdb.DuckDBPyConnection) -> None:
        for table_name, row_count in conn.execute(INSPECT_SQL.read_text()).fetchall():
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

            if step in {"2", "3"}:
                raise NotImplementedError("Only preprocessing Step 1 has been rebuilt in native SQL so far.")

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
