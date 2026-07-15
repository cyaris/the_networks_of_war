"""DuckDB execution processes for The Networks of War preprocessing."""

from __future__ import annotations

import argparse
import re
from collections.abc import Callable
from pathlib import Path

import duckdb

from source import BACKEND_ROOT
from utils import initialize_logger

logger = initialize_logger(__name__)

SQL_ROOT = BACKEND_ROOT / "sql"

DEFAULT_DB_PATH = BACKEND_ROOT / "the_networks_of_war.duckdb"
INSPECT_SQL = SQL_ROOT / "inspect_tables.sql"
CREATE_RELATION_PATTERN = re.compile(
    r"^\s*create\s+or\s+replace\s+(?:table|view)\s+([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE | re.MULTILINE
)

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

STEP_2_SQL = [
    "00_setup.sql",
    "step_1/00_setup.sql",
    "step_2/01_create_source_tables.sql",
    "step_2/02_insert_source_tables.sql",
    "step_2/03_create_country_year_descriptives.sql",
    "step_2/04_create_participant_year_descriptives.sql",
    "step_2/05_create_participant_descriptives.sql",
    "step_2/06_create_dyad_year_descriptives.sql",
    "step_2/07_create_dyadic_descriptives.sql",
]

STEP_3_SQL = [
    "00_setup.sql",
    "step_3/01_create_final_participants.sql",
    "step_3/02_create_final_dyads.sql",
    "step_3/03_create_final_wars.sql",
    "step_3/04_create_d3_war_nodes.sql",
    "step_3/05_create_d3_war_links.sql",
    "step_3/06_create_d3_war_json.sql",
]

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


def add_duckdb_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, type=Path)
    parser.add_argument("--inspect", action="store_true")
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


class DuckDBProcessesMixin:
    db_path: Path

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

    def run_step_3(self, conn: duckdb.DuckDBPyConnection) -> None:
        for name in STEP_3_SQL:
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


__all__ = [
    "DEFAULT_DB_PATH",
    "SQL_ROOT",
    "STEP_1_SQL",
    "STEP_2_SQL",
    "STEP_3_SQL",
    "STEP_1_SOURCE_KEYS",
    "STEP_2_SOURCE_KEYS",
    "DuckDBProcessesMixin",
    "add_duckdb_arguments",
    "created_relation_names",
    "format_query_results",
    "read_query_file",
    "render_sql",
    "sql_identifier",
]
