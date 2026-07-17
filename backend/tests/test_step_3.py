from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"

sys.path.insert(0, str(SRC_ROOT))

from shared import fail_if_detected_rows  # noqa: E402

from pipeline import STEP_1_SOURCE_KEYS, STEP_2_SOURCE_KEYS, STEP_3_SQL, Pipeline, sql_identifier

STEP_3_TRANSFORMED_TABLES = ["final_participants", "final_dyads", "final_wars"]
DESCRIPTOR_TIMEFRAMES = {"first_year", "last_year", "all_years"}
DYADIC_DESCRIPTOR_COLUMNS = [
    "territory_exchange",
    "colonial_contiguity",
    "contiguity",
    "alliance",
    "defense_cooperation_agreements",
    "inter_governmental_organizations",
    "diplomatic_exchange",
    "trade_relations",
    "same_leader_type",
    "military_leaders",
    "communist_leaders",
    "royal_leaders",
    "democratic_incumbent",
    "unconstitutional_incumbent",
    "democratic_regimes",
    "dictatorships",
    "collective_leaderships",
    "direct_election",
    "indirect_election",
    "non_elected_leaders",
    "no_legislature",
    "non_elective_legislature",
    "elective_legislature",
    "no_partisan_legislature_legal",
    "no_non_regime_legislature_parties_legal",
    "multi_party_legislature_legal",
    "all_parties_illegal",
    "single_party_state_exists",
    "multi_party_state_exists",
    "no_parties_exist",
    "one_party_exists",
    "no_non_regime_parties_exist",
    "leader_died",
    "new_leader",
    "transition_to_democracy",
    "transition_to_dictatorship",
    "atop",
    "mtops",
    "shared_arms_technology",
]


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
    assert set(payload["graphsByWarId"]) == {js_war_id_key(war["war_id"]) for war in payload["wars"]}

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
        assert all(
            number_or_none(node.get(timeframe, {}).get(field)) not in (-9, -8)
            for node in nodes
            for timeframe, field in descriptor_fields(node)
        )
        assert all(
            number_or_none(link.get(timeframe, {}).get(field)) not in (-9, -8)
            for link in links
            for timeframe, field in descriptor_fields(link)
        )

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


def test_step_3_applies_participant_null_and_conversion_rules(conn):

    state_null_fill_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_id = b.war_id
                             and a.c_code = b.c_code
                             and a.participant = b.participant
    where
        a.timeframe = 'First Year'
        and a.c_code > 0
        and a.money_flow_in is null
        and json_extract(b.descriptor_timeframes, '$.first_year.money_flow_in')::double = 0
    """
    assert scalar(conn, state_null_fill_sql) == 0

    population_conversion_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_id = b.war_id
                             and a.c_code = b.c_code
                             and a.participant = b.participant
    where
        a.timeframe = 'First Year'
        and a.c_code > 0
        and a.population is not null
        and a.population not in (-9, -8)
        and json_extract(b.descriptor_timeframes, '$.first_year.population')::double = a.population * 1000
    """
    assert scalar(conn, population_conversion_sql) > 0

    money_flow_conversion_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_id = b.war_id
                             and a.c_code = b.c_code
                             and a.participant = b.participant
    where
        a.timeframe = 'All Years'
        and a.money_flow_in is not null
        and json_extract(b.descriptor_timeframes, '$.all_years.money_flow_in')::double = a.money_flow_in * 1000000
    """
    assert scalar(conn, money_flow_conversion_sql) > 0

    military_personnel_conversion_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_id = b.war_id
                             and a.c_code = b.c_code
                             and a.participant = b.participant
    where
        a.timeframe = 'All Years'
        and a.military_personnel is not null
        and json_extract(b.descriptor_timeframes, '$.all_years.military_personnel')::double = a.military_personnel * 1000
    """
    assert scalar(conn, military_personnel_conversion_sql) > 0

    co2_conversion_sql = """
    select count(*)
    from participant_descriptives a
    join final_participants b on a.war_id = b.war_id
                             and a.c_code = b.c_code
                             and a.participant = b.participant
    where
        a.timeframe = 'All Years'
        and a.co2_emissions_per_capita is not null
        and json_extract(b.descriptor_timeframes, '$.all_years.co2_emissions_per_capita')::double = a.co2_emissions_per_capita
    """
    assert scalar(conn, co2_conversion_sql) > 0

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
    select
        a.war_id,
        a.source,
        a.target,
        b.id source_id,
        c.id target_id
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
    order by a.war_id, a.source, a.target
    """
    fail_if_detected_rows(
        conn,
        missing_nodes_sql,
        "Final dyad links should resolve final participant nodes.",
        "final_dyads unresolved nodes",
        {"source", "target", "source_id", "target_id"},
    )


def test_step_3_final_dyads_preserve_unknown_non_state_descriptors(conn):
    descriptor_columns = ", ".join(DYADIC_DESCRIPTOR_COLUMNS)
    query = f"""
    select
        a.war_id,
        a.c_code_a,
        a.c_code_b,
        a.participant_a,
        a.participant_b,
        b.timeframe,
        b.field,
        b.source_value,
        json_extract(a.descriptor_timeframes, '$.' || lower(replace(b.timeframe, ' ', '_')) || '.' || b.field)::double exported_value
    from dyadic_descriptives
    unpivot include nulls (source_value for field in ({descriptor_columns})) b
    join final_dyads a on a.war_id = b.war_id
                      and a.c_code_a = b.c_code_a
                      and a.c_code_b = b.c_code_b
                      and a.participant_a = b.participant_a
                      and a.participant_b = b.participant_b
    where
        (a.c_code_a <= 0 or a.c_code_b <= 0)
        and b.source_value is null
        and json_extract(a.descriptor_timeframes, '$.' || lower(replace(b.timeframe, ' ', '_')) || '.' || b.field)::double = 0
    order by a.war_id, a.c_code_a, a.c_code_b, b.timeframe, b.field
    limit 50
    """
    fail_if_detected_rows(
        conn,
        query,
        "Final non-state dyad descriptors should preserve unknown values instead of filling zero.",
        "non-state dyad descriptor nulls exported as zero",
        {"source_value", "exported_value"},
    )


def test_step_3_final_participant_nodes_all_have_links(conn):
    unlinked_nodes_sql = """
    select
        a.war_id,
        a.war_name,
        a.id,
        a.c_code,
        a.participant,
        a.side
    from final_participants a
    left join final_dyads b on a.war_id = b.war_id
                           and a.id in (b.source, b.target)
    where b.war_id is null
    order by a.war_id, a.id
    """
    fail_if_detected_rows(
        conn,
        unlinked_nodes_sql,
        "Final participants should all be linked by at least one final dyad.",
        "unlinked final_participants nodes",
        {"id"},
    )


def test_step_3_materializes_valid_per_war_graph_json(conn):
    query = """
    select
        war_id,
        total_participants,
        total_dyads,
        graph_json
    from final_wars
    order by war_id
    """
    rows = conn.execute(query).fetchall()

    assert len(rows) > 0

    for war_id, total_participants, total_dyads, graph_json in rows:
        graph = json.loads(graph_json)

        assert set(graph) == {"war", "nodes", "links"}
        assert len(graph["war"]) == 1
        assert graph["war"][0]["war_id"] == war_id
        assert len(graph["nodes"]) == total_participants
        assert len(graph["links"]) == total_dyads


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
