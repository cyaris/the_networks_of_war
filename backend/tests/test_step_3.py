from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import STEP_1_SOURCE_KEYS, STEP_2_SOURCE_KEYS, STEP_3_SQL, Pipeline, sql_identifier

STEP_3_TRANSFORMED_TABLES = ["final_participants", "final_dyads", "final_wars"]
DESCRIPTOR_TIMEFRAMES = {"first_year", "last_year", "all_years"}


@pytest.fixture(scope="session")
def step_3_outputs(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    tmp_path = tmp_path_factory.mktemp("duckdb")
    db_path = tmp_path / "step_3.duckdb"
    frontend_data_path = tmp_path / "graphData.json"
    pipeline = Pipeline(db_path=db_path, frontend_data_path=frontend_data_path)
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
    assert row_counts["final_participants"] == scalar(conn, "select count(*) from participants")
    assert row_counts["final_dyads"] == scalar(conn, "select count(*) from dyads")
    assert row_counts["final_wars"] == scalar(conn, "select count(*) from wars")

    assert "file_name" not in table_columns(conn, "final_wars")
    assert "total_days_in_war" in table_columns(conn, "final_wars")
    assert "graph_json" in table_columns(conn, "final_wars")
    assert {"id", "node_key", "descriptor_timeframes"}.issubset(table_columns(conn, "final_participants"))
    assert {"source", "target", "descriptor_timeframes"}.issubset(table_columns(conn, "final_dyads"))


def test_step_3_exports_frontend_graph_data(step_3_outputs: tuple[Path, Path]):
    db_path, frontend_data_path = step_3_outputs
    payload = json.loads(frontend_data_path.read_text())

    assert payload["source"]["tables"] == ["final_wars", "final_participants", "final_dyads"]
    assert len(payload["wars"]) > 0
    assert len(payload["graphsByWarId"]) > 0
    assert all(js_war_id_key(war["war_id"]) in payload["graphsByWarId"] for war in payload["wars"])

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert scalar(conn, "select count(*) from final_wars where graph_json is not null") > 0


def test_step_3_frontend_graph_data_prunes_unavailable_descriptor_fields(step_3_outputs: tuple[Path, Path]):
    _, frontend_data_path = step_3_outputs
    payload = json.loads(frontend_data_path.read_text())

    for graph in payload["graphsByWarId"].values():
        nodes = graph["nodes"]
        links = graph["links"]
        node_fields = sorted({(timeframe, field) for node in nodes for timeframe, field in descriptor_fields(node)})
        link_fields = sorted({(timeframe, field) for link in links for timeframe, field in descriptor_fields(link)})

        assert all(not is_legacy_timeframe_field(field) for node in nodes for field in node)
        assert all(not is_legacy_timeframe_field(field) for link in links for field in link)

        for timeframe, field in node_fields:
            values = [number_or_none(node.get(timeframe, {}).get(field)) for node in nodes]
            coalesced_values = [max(value if value is not None else 0, 0) for value in values]

            assert max(coalesced_values, default=0) > 0
            assert len(set(coalesced_values)) > 1
            assert sum(value is None for value in values) / len(values) < 0.5

        for timeframe, field in link_fields:
            values = [number_or_none(link.get(timeframe, {}).get(field)) for link in links]

            assert any(value is not None and value > 0 for value in values)


def test_step_3_frontend_graph_data_omits_first_and_last_year_for_single_year_wars(
    step_3_outputs: tuple[Path, Path],
):
    _, frontend_data_path = step_3_outputs
    payload = json.loads(frontend_data_path.read_text())
    single_year_wars = [war for war in payload["wars"] if war["start_year"] == war["end_year"]]

    assert len(single_year_wars) > 0

    for war in single_year_wars:
        graph = payload["graphsByWarId"][js_war_id_key(war["war_id"])]
        descriptor_keys = {
            key for row in [*graph["nodes"], *graph["links"]] for key in row if key in DESCRIPTOR_TIMEFRAMES
        }

        assert descriptor_keys <= {"all_years"}


def test_step_3_applies_legacy_participant_fill_and_conversion_rules(conn):

    state_null_fill_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_id = b.war_id
                              and a.c_code = b.c_code
                              and a.participant = b.participant
    where
        a.timeframe = 'First Year'
        and
        a.c_code > 0
        and a.money_flow_in is null
        and json_extract(b.descriptor_timeframes, '$.first_year.money_flow_in')::double = 0
    """
    assert scalar(conn, state_null_fill_sql) > 0

    population_conversion_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_id = b.war_id
                              and a.c_code = b.c_code
                              and a.participant = b.participant
    where
        a.timeframe = 'First Year'
        and
        a.c_code > 0
        and a.population is not null
        and a.population not in (-9, -8)
        and json_extract(b.descriptor_timeframes, '$.first_year.population')::double = a.population * 1000
    """
    assert scalar(conn, population_conversion_sql) > 0

    non_state_descriptor_sql = """
    select count(*)
    from final_participants
    where
        c_code <= 0
        and descriptor_timeframes is null
    """
    assert scalar(conn, non_state_descriptor_sql) > 0


def test_step_3_final_dyad_links_resolve_all_final_participants(conn):
    missing_nodes_sql = """
    select count(*)
    from final_dyads a
    left join final_participants b on a.war_id = b.war_id
                                   and a.source = b.id
    left join final_participants c on a.war_id = c.war_id
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
    where war_id = 419
    """
    graph_json = conn.execute(query).fetchone()[0]
    graph = json.loads(graph_json)

    assert set(graph) == {"war", "nodes", "links"}
    assert len(graph["war"]) == 1
    assert len(graph["nodes"]) == scalar(conn, "select count(*) from final_participants where war_id = 419")
    assert len(graph["links"]) == scalar(conn, "select count(*) from final_dyads where war_id = 419")


def js_war_id_key(value: float) -> str:
    return str(int(value)) if value == int(value) else str(value)


def is_legacy_timeframe_field(field: str) -> bool:
    return field.endswith(("_x", "_y", "_z"))


def descriptor_fields(row: dict):
    return [(timeframe, field) for timeframe in DESCRIPTOR_TIMEFRAMES for field in row.get(timeframe, {})]


def number_or_none(value):
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value

    return None
