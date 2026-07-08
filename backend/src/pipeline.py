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

SOURCE_FILES = {
    "country_codes": "COW country codes.csv",
    "directed_dyadic_war": "directed_dyadic_war.csv",
    "dyadic_mid": "dyadic_mid_4.02.csv",
    "extrastate_wars": "Extra-StateWarData_v4.0.csv",
    "interstate_wars": "Inter-StateWarData_v4.0.csv",
    "intrastate_wars": "INTRA-STATE_State_participants v5.1.csv",
    "war_types": "../war_types.csv",
}

SOURCE_ENCODINGS = {
    "extrastate_wars": "cp1252",
}

STEP_1_SQL = [
    "step_1/00_setup.sql",
    "step_1/01_create_source_tables.sql",
    "step_1/02_insert_source_tables.sql",
    "step_1/03_create_reference_tables.sql",
    "step_1/04_create_war_dyads.sql",
    "step_1/05_create_war_participants.sql",
    "step_1/06_process_war_dyads.sql",
    "step_1/07_add_mid_dyads.sql",
    "step_1/08_add_missing_participants.sql",
    "step_1/09_add_inferred_dyads.sql",
    "step_1/10_create_initial_tables.sql",
]


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def render_sql(name: str, context: dict[str, str]) -> str:
    return (SQL_ROOT / name).read_text().format(**context)


class Pipeline:
    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        csv_dir: Path = DEFAULT_CSV_DIR,
    ) -> None:
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
        return {f"{key}_path": sql_literal(self.prepared_path_for(key)) for key in SOURCE_FILES}

    def require_inputs(self) -> None:
        missing = [str(self.path_for(key)) for key in SOURCE_FILES if not self.path_for(key).exists()]

        if missing:
            raise FileNotFoundError("Missing required CSV inputs:\n" + "\n".join(missing))

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        return duckdb.connect(str(self.db_path))

    def execute_sql(self, conn: duckdb.DuckDBPyConnection, name: str) -> None:
        conn.execute(render_sql(name, self.sql_context()))

    def run_step_1(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.require_inputs()

        for name in STEP_1_SQL:
            logger.info("Running %s", name)
            self.execute_sql(conn, name)

    def inspect(self, conn: duckdb.DuckDBPyConnection) -> None:
        logger.info(
            "\n%s",
            "\n".join(
                f"{table_name}: {row_count:,d}"
                for table_name, row_count in conn.execute(INSPECT_SQL.read_text()).fetchall()
            ),
        )

    def run(self, step: str = "all", inspect: bool = False) -> None:
        with self.connect() as conn:
            if step in {"all", "1"}:
                self.run_step_1(conn)

            if step in {"2", "3"}:
                raise NotImplementedError("Only preprocessing Step 1 has been rebuilt in native SQL so far.")

            if inspect:
                self.inspect(conn)

        logger.info("DuckDB database: %s", self.db_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DuckDB backend for The Networks of War preprocessing.")

    parser.add_argument("--csv-dir", default=DEFAULT_CSV_DIR, type=Path)
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, type=Path)
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--step", choices=["all", "1", "2", "3"], default="all")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    Pipeline(
        csv_dir=args.csv_dir,
        db_path=args.db_path,
    ).run(args.step, args.inspect)


if __name__ == "__main__":
    main()
