"""DuckDB preprocessing backend for The Networks of War."""

from __future__ import annotations

import argparse
from pathlib import Path

from duckdb_backend import (
    DEFAULT_DB_PATH,
    SQL_ROOT,
    STEP_1_SOURCE_KEYS,
    STEP_1_SQL,
    STEP_2_SOURCE_KEYS,
    STEP_2_SQL,
    DuckDBProcessesMixin,
    add_duckdb_arguments,
    created_relation_names,
    format_query_results,
    read_query_file,
    render_sql,
    sql_identifier,
)
from source import (
    DEFAULT_DATA_DIR,
    PARTICIPANT_NAME_REPLACEMENTS_PATH,
    SOURCE_FILES,
    SOURCE_METADATA,
    SOURCE_METADATA_BY_KEY,
    SOURCE_PREPARED_FILES,
    SourceDataIssue,
    SourceDataPreparationMixin,
    add_source_data_arguments,
    load_source_metadata,
    source_prepared_files,
    sql_literal,
)
from utils import initialize_logger

logger = initialize_logger(__name__)

DEFAULT_FRONTEND_DATA_PATH = (
    Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "static" / "graphData.json"
)

STEP_3_SQL = [
    "00_setup.sql",
    "step_3/01_create_final_participants.sql",
    "step_3/02_create_final_dyads.sql",
    "step_3/03_create_final_wars.sql",
]


class Pipeline(SourceDataPreparationMixin, DuckDBProcessesMixin):
    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        data_dir: Path = DEFAULT_DATA_DIR,
        frontend_data_path: Path | None = DEFAULT_FRONTEND_DATA_PATH,
    ) -> None:
        self.data_dir = data_dir
        self.db_path = db_path
        self.frontend_data_path = frontend_data_path

    def run_step_3(self, conn) -> None:
        for name in STEP_3_SQL:
            logger.info("Running %s", name)
            self.execute_sql(conn, name)

        self.export_frontend_data(conn)

    def export_frontend_data(self, conn) -> None:
        if self.frontend_data_path is None:
            return

        logger.info("Updating/recreating frontend graph data.")
        query = render_sql("step_3/04_export_frontend_graph_data.sql", self.sql_context())
        _, war_count, graph_data_json = conn.execute(query).fetchone()
        logger.info("Graphs to be rewritten: %s", f"{war_count:,d}")

        self.frontend_data_path.parent.mkdir(parents=True, exist_ok=True)
        self.frontend_data_path.write_text(f"{graph_data_json}\n")
        logger.info("Completed frontend graph data update: %s (%s wars)", self.frontend_data_path, f"{war_count:,d}")

    def run(
        self,
        build: bool = True,
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
            if build:
                self.run_step_1(conn)
                self.run_step_2(conn)
                self.run_step_3(conn)

            if inspect:
                self.inspect(conn)

            if query:
                self.query(conn, query)

            if query_file:
                self.query(conn, read_query_file(query_file))

        logger.info("DuckDB database: %s", self.db_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DuckDB backend for The Networks of War preprocessing.")
    add_source_data_arguments(parser)
    add_duckdb_arguments(parser)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    Pipeline(data_dir=args.data_dir, db_path=args.db_path).run(
        build=args.build,
        inspect=args.inspect,
        query=args.query,
        query_file=args.query_file,
        prepare_data=args.prepare_data,
        recreate_data=args.recreate_data,
    )


if __name__ == "__main__":
    main()
