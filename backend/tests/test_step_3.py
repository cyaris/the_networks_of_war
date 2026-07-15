from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import DEFAULT_CSV_DIR, STEP_1_SOURCE_KEYS, STEP_2_SOURCE_KEYS, STEP_3_SQL, Pipeline, sql_identifier

STEP_3_TRANSFORMED_TABLES = ["final_participants", "final_dyads", "final_wars"]


@pytest.fixture(scope="session")
def step_3_outputs(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    tmp_path = tmp_path_factory.mktemp("duckdb")
    db_path = tmp_path / "step_3.duckdb"
    frontend_data_path = tmp_path / "graphData.json"
    pipeline = Pipeline(db_path=db_path, csv_dir=DEFAULT_CSV_DIR, frontend_data_path=frontend_data_path)
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

    return db_path, frontend_data_path


@pytest.fixture(scope="session")
def step_3_db_path(step_3_outputs: tuple[Path, Path]) -> Path:
    db_path, _ = step_3_outputs

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
    ]
    assert set(STEP_3_TRANSFORMED_TABLES).issubset(actual_tables)
    assert all(row_count > 0 for row_count in row_counts.values())
    assert row_counts["final_participants"] == scalar(conn, "select count(*) from participant_descriptives")
    assert row_counts["final_dyads"] == scalar(conn, "select count(*) from dyadic_descriptives")
    assert row_counts["final_wars"] == scalar(conn, "select count(*) from wars")

    assert "file_name" not in table_columns(conn, "final_wars")
    assert "total_days_in_war" in table_columns(conn, "final_wars")
    assert "graph_json" in table_columns(conn, "final_wars")
    assert {"id", "node_key"}.issubset(table_columns(conn, "final_participants"))
    assert {"source", "target"}.issubset(table_columns(conn, "final_dyads"))


def test_step_3_exports_frontend_graph_data(step_3_outputs: tuple[Path, Path]):
    db_path, frontend_data_path = step_3_outputs
    payload = json.loads(frontend_data_path.read_text())

    assert payload["source"]["tables"] == ["final_wars", "final_participants", "final_dyads"]
    assert len(payload["wars"]) > 0
    assert len(payload["graphsByWarNum"]) > 0
    assert all(js_war_num_key(war["war_num"]) in payload["graphsByWarNum"] for war in payload["wars"])

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert scalar(conn, "select count(*) from final_wars where graph_json is not null") > 0


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
    assert scalar(conn, state_null_fill_sql) > 0

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
    assert scalar(conn, population_conversion_sql) > 0

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
    assert scalar(conn, non_state_null_sql) > 0


def test_step_3_final_dyad_links_resolve_all_final_participants(conn):
    missing_nodes_sql = """
    select count(*)
    from final_dyads a
    left join final_participants b on a.war_num = b.war_num
                                   and a.source = b.id
    left join final_participants c on a.war_num = c.war_num
                                   and a.target = c.id
    where
        a.source is null
        or a.target is null
        or b.id is null
        or c.id is null
    """
    assert scalar(conn, missing_nodes_sql) == 0


def test_step_3_materializes_valid_per_war_graph_json(conn):
    query = """
    select graph_json
    from final_wars
    where war_num = 419
    """
    graph_json = conn.execute(query).fetchone()[0]
    graph = json.loads(graph_json)

    assert set(graph) == {"war", "nodes", "links"}
    assert len(graph["war"]) == 1
    assert len(graph["nodes"]) == scalar(conn, "select count(*) from final_participants where war_num = 419")
    assert len(graph["links"]) == scalar(conn, "select count(*) from final_dyads where war_num = 419")


def js_war_num_key(value: float) -> str:
    return str(int(value)) if value == int(value) else str(value)
