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
    DEFAULT_CSV_DIR,
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


class Pipeline(SourceDataPreparationMixin, DuckDBProcessesMixin):
    def __init__(self, db_path: Path = DEFAULT_DB_PATH, csv_dir: Path = DEFAULT_DATA_DIR) -> None:
        self.data_dir = csv_dir
        self.db_path = db_path

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
    add_source_data_arguments(parser)
    add_duckdb_arguments(parser)

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
