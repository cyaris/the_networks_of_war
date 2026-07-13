"""DuckDB preprocessing backend for The Networks of War."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from utils import initialize_logger

logger = initialize_logger(__name__)

BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = BACKEND_ROOT.parent
SQL_ROOT = BACKEND_ROOT / "sql"
WORK_CSV_DIR = BACKEND_ROOT / ".work" / "csv"

DEFAULT_CSV_DIR = PROJECT_ROOT / "csvs"
DEFAULT_DB_PATH = BACKEND_ROOT / "the_networks_of_war.duckdb"
INSPECT_SQL = SQL_ROOT / "inspect_tables.sql"

SOURCE_METADATA = {
    "country_codes": {"file": "COW country codes.csv", "version": "COW country codes", "documentation": "Entities.pdf"},
    "extrastate_wars": {
        "file": "Extra-StateWarData_v4.0.csv",
        "version": "4.0",
        "documentation": "Extra-StateWars_Codebook.pdf",
    },
    "interstate_mid_dyads": {
        "file": "dyadic_mid_4.02.csv",
        "version": "4.02",
        "documentation": "Dyadic MID Codebook V4.0.pdf",
    },
    "interstate_war_dyads": {
        "file": "directed_dyadic_war.csv",
        "version": "directed_dyadic_war.csv",
        "documentation": "The Directed Dyadic Interstate War Dataset Codebook.pdf",
    },
    "interstate_wars": {
        "file": "Inter-StateWarData_v4.0.csv",
        "version": "4.0",
        "documentation": "MII_v4.0_Codebook.pdf",
    },
    "intrastate_wars": {
        "file": "INTRA-STATE_State_participants v5.1.csv",
        "version": "5.1",
        "documentation": "Codebook for Intra-state v5.1 2.9.20.pdf; Description of Intra-state v5.1.pdf",
    },
    "war_types": {"file": "../war_types.csv", "version": "local", "documentation": "local helper file"},
}

SOURCE_FILES = {key: metadata["file"] for key, metadata in SOURCE_METADATA.items()}

SOURCE_ENCODINGS = {"extrastate_wars": "cp1252"}

STEP_1_SQL = [
    "step_1/00_setup.sql",
    "step_1/01_create_source_tables.sql",
    "step_1/02_insert_source_tables.sql",
    "step_1/02a_apply_source_adjustments.sql",
    "step_1/02b_insert_source_adjustments.sql",
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


class Pipeline:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH, csv_dir: Path = DEFAULT_CSV_DIR) -> None:
        self.csv_dir = csv_dir
        self.db_path = db_path

    def path_for(self, source_key: str) -> Path:
        relative = SOURCE_FILES[source_key]

        if relative.startswith("../"):
            return (PROJECT_ROOT / relative.removeprefix("../")).resolve()

        return (self.csv_dir / relative).resolve()

    def prepared_path_for(self, source_key: str) -> Path:
        if source_key not in SOURCE_ENCODINGS:
            return self.path_for(source_key)

        WORK_CSV_DIR.mkdir(parents=True, exist_ok=True)
        path = WORK_CSV_DIR / self.path_for(source_key).name
        path.write_text(self.path_for(source_key).read_text(encoding=SOURCE_ENCODINGS[source_key]), encoding="utf-8")

        return path.resolve()

    def sql_context(self) -> dict[str, str]:
        context = {f"{key}_path": sql_literal(self.prepared_path_for(key)) for key in SOURCE_FILES}

        for key, metadata in SOURCE_METADATA.items():
            context[f"{key}_source_file"] = sql_literal(Path(metadata["file"]).name)
            context[f"{key}_source_version"] = sql_literal(metadata["version"])
            context[f"{key}_documentation"] = sql_literal(metadata["documentation"])

        return context

    def require_inputs(self) -> None:
        missing = [str(self.path_for(key)) for key in SOURCE_FILES if not self.path_for(key).exists()]

        if missing:
            raise FileNotFoundError("Missing required CSV inputs:\n" + "\n".join(missing))

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        return duckdb.connect(str(self.db_path))

    def execute_sql(self, conn: duckdb.DuckDBPyConnection, name: str) -> None:
        conn.execute(render_sql(name, self.sql_context()))

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

    def run_step_1(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.require_inputs()

        for name in STEP_1_SQL:
            logger.info("Running %s", name)
            if name == "step_1/04_create_war_dyads.sql":
                self.drop_relation_if_exists(conn, "war_dyads")
            self.execute_sql(conn, name)

    def inspect(self, conn: duckdb.DuckDBPyConnection) -> None:
        for table_name, row_count in conn.execute(INSPECT_SQL.read_text()).fetchall():
            logger.info(f"{table_name}: {row_count:,d}")

    def query(self, conn: duckdb.DuckDBPyConnection, sql: str) -> None:
        result = conn.execute(sql)
        columns = [column[0] for column in result.description] if result.description else []
        rows = result.fetchall() if columns else []
        logger.info("\n%s", format_query_results(columns, rows))

    def run(self, step: str = "all", inspect: bool = False, query: str | None = None) -> None:
        with self.connect() as conn:
            if step in {"all", "1"}:
                self.run_step_1(conn)

            if step in {"2", "3"}:
                raise NotImplementedError("Only preprocessing Step 1 has been rebuilt in native SQL so far.")

            if inspect:
                self.inspect(conn)

            if query:
                self.query(conn, query)

        logger.info("DuckDB database: %s", self.db_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DuckDB backend for The Networks of War preprocessing.")

    parser.add_argument("--csv-dir", default=DEFAULT_CSV_DIR, type=Path)
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, type=Path)
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument(
        "--query", help="SQL query to execute against the DuckDB database after the selected step runs."
    )
    parser.add_argument("--step", choices=["none", "all", "1", "2", "3"], default="all")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    Pipeline(csv_dir=args.csv_dir, db_path=args.db_path).run(args.step, args.inspect, args.query)


if __name__ == "__main__":
    main()
