from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import DEFAULT_CSV_DIR, STEP_1_SOURCE_KEYS, STEP_2_SOURCE_KEYS, STEP_3_SQL, Pipeline, sql_identifier

STEP_3_TRANSFORMED_TABLES = [
    "final_participants",
    "final_dyads",
    "final_wars",
    "d3_war_nodes",
    "d3_war_links",
    "d3_war_json",
]


@pytest.fixture(scope="session")
def step_3_db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    db_path = tmp_path_factory.mktemp("duckdb") / "step_3.duckdb"
    pipeline = Pipeline(db_path=db_path, csv_dir=DEFAULT_CSV_DIR)
    missing = [
        str(path)
        for source_key in [*STEP_1_SOURCE_KEYS, *STEP_2_SOURCE_KEYS]
        for path in pipeline.paths_for(source_key)
        if not path.exists()
    ]

    if missing:
        pytest.skip("Step 1 or Step 2 source CSVs are not available:\n" + "\n".join(missing))

    with pipeline.connect() as conn:
        pipeline.run_step_1(conn)
        pipeline.run_step_2(conn)
        pipeline.run_step_3(conn)

    return db_path


@pytest.fixture()
def conn(step_3_db_path: Path):
    connection = duckdb.connect(str(step_3_db_path), read_only=True)
    try:
        yield connection
    finally:
        connection.close()


def table_columns(conn, table_name: str) -> list[str]:
    query = """
    select column_name
    from information_schema.columns
    where
        table_schema = current_schema()
        and table_name = ?
    order by ordinal_position
    """

    return [column_name for (column_name,) in conn.execute(query, [table_name]).fetchall()]


def scalar(conn, sql: str):
    return conn.execute(sql).fetchone()[0]


def test_step_3_manifest_runs_final_export_transformations(conn):
    actual_tables_query = """
    select table_name
    from information_schema.tables
    where table_schema = current_schema()
    """
    actual_tables = {table_name for (table_name,) in conn.execute(actual_tables_query).fetchall()}
    row_counts = {}

    for table_name in STEP_3_TRANSFORMED_TABLES:
        row_count_query = f"select count(*) from {sql_identifier(table_name)}"
        row_counts[table_name] = conn.execute(row_count_query).fetchone()[0]

    assert STEP_3_SQL == [
        "00_setup.sql",
        "step_3/01_create_final_participants.sql",
        "step_3/02_create_final_dyads.sql",
        "step_3/03_create_final_wars.sql",
        "step_3/04_create_d3_war_nodes.sql",
        "step_3/05_create_d3_war_links.sql",
        "step_3/06_create_d3_war_json.sql",
    ]
    assert set(STEP_3_TRANSFORMED_TABLES).issubset(actual_tables)
    assert all(row_count > 0 for row_count in row_counts.values())
    assert row_counts["final_participants"] == scalar(conn, "select count(*) from participant_descriptives")
    assert row_counts["final_dyads"] == scalar(conn, "select count(*) from dyadic_descriptives")
    assert row_counts["final_wars"] == scalar(conn, "select count(*) from wars")
    assert row_counts["d3_war_nodes"] == row_counts["final_participants"]
    assert row_counts["d3_war_links"] == row_counts["final_dyads"]
    assert row_counts["d3_war_json"] == row_counts["final_wars"]

    assert "file_name" in table_columns(conn, "final_wars")
    assert "total_days_in_war" in table_columns(conn, "final_wars")
    assert {"id", "node_key"}.issubset(table_columns(conn, "d3_war_nodes"))
    assert {"source", "target"}.issubset(table_columns(conn, "d3_war_links"))


def test_step_3_applies_legacy_participant_fill_and_conversion_rules(conn):
    state_null_fill_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_num = b.war_num
                              and a.c_code = b.c_code
                              and a.participant = b.participant
    where
        a.c_code > 0
        and a.money_flow_in_x is null
        and b.money_flow_in_x = 0
    """
    population_conversion_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_num = b.war_num
                              and a.c_code = b.c_code
                              and a.participant = b.participant
    where
        a.c_code > 0
        and a.population_x is not null
        and a.population_x not in (-9, -8)
        and b.population_x = a.population_x * 1000
    """
    non_state_null_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_num = b.war_num
                              and a.c_code = b.c_code
                              and a.participant = b.participant
    where
        a.c_code <= 0
        and a.terrorism_deaths_x is null
        and b.terrorism_deaths_x is null
    """

    assert scalar(conn, state_null_fill_sql) > 0
    assert scalar(conn, population_conversion_sql) > 0
    assert scalar(conn, non_state_null_sql) > 0


def test_step_3_d3_links_resolve_all_final_dyads(conn):
    missing_nodes_sql = """
    select count(*)
    from final_dyads a
    left join d3_war_nodes b on a.war_num = b.war_num
                              and if(a.c_code_a > 0, a.c_code_a::varchar, a.participant_a) = b.node_key
    left join d3_war_nodes c on a.war_num = c.war_num
                              and if(a.c_code_b > 0, a.c_code_b::varchar, a.participant_b) = c.node_key
    where
        b.id is null
        or c.id is null
    """
    assert scalar(conn, missing_nodes_sql) == 0


def test_step_3_materializes_valid_per_war_graph_json(conn):
    query = """
    select graph_json
    from d3_war_json
    where war_num = 419
    """
    graph_json = conn.execute(query).fetchone()[0]
    graph = json.loads(graph_json)

    assert set(graph) == {"war", "nodes", "links"}
    assert graph["war"][0]["file_name"] == "war_num_419_0.json"
    assert len(graph["war"]) == 1
    assert len(graph["nodes"]) == scalar(conn, "select count(*) from d3_war_nodes where war_num = 419")
    assert len(graph["links"]) == scalar(conn, "select count(*) from d3_war_links where war_num = 419")
