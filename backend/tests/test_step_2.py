from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import (
    SOURCE_FILES,
    SOURCE_METADATA,
    SOURCE_PREPARED_FILES,
    SQL_ROOT,
    STEP_1_SOURCE_KEYS,
    STEP_2_SOURCE_KEYS,
    STEP_2_SQL,
    Pipeline,
    sql_identifier,
)

STEP_2_SOURCE_TABLES = [
    "source_global_terrorism_database",
    "source_formal_alliances_directed_yearly",
    "source_territorial_changes",
    "source_forcibly_displaced_populations",
    "source_colonial_dependency_contiguity",
    "source_direct_contiguity",
    "source_defense_cooperation_agreements",
    "source_intergovernmental_organizations_dyadic",
    "source_diplomatic_exchange",
    "source_dd_revisited",
    "source_co_emissions_per_capita",
    "source_arms_technology",
    "source_atop_dyadic_years",
    "source_mtops_dyadic",
    "source_cow_trade_dyadic",
    "source_cow_trade_national",
    "source_national_material_capabilities",
]

STEP_2_TRANSFORMED_TABLES = [
    "country_year_descriptives",
    "participant_year_descriptives",
    "participant_descriptives",
    "dyad_year_descriptives",
    "dyadic_descriptives",
]


@pytest.fixture(scope="session")
def step_2_db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    db_path = tmp_path_factory.mktemp("duckdb") / "step_2.duckdb"
    pipeline = Pipeline(db_path=db_path)
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

    return db_path


@pytest.fixture()
def conn(step_2_db_path: Path):
    connection = duckdb.connect(str(step_2_db_path), read_only=True)
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


def test_step_2_source_insert_sql_keeps_compact_alias_style():
    insert_sql = (SQL_ROOT / "step_2/02_insert_source_tables.sql").read_text().lower()

    assert " as " not in insert_sql


def test_global_terrorism_database_source_metadata_converts_workbooks():
    metadata = next(row for row in SOURCE_METADATA if row["key"] == "global_terrorism_database")
    workbook_conversions = {
        download["filename"]: download.get("converted_filename")
        for download in metadata["downloads"]
        if download["filename"].endswith(".xlsx")
    }

    assert SOURCE_FILES["global_terrorism_database"] == "globalterrorismdb_0522dist.csv"
    assert SOURCE_PREPARED_FILES["global_terrorism_database"] == [
        "globalterrorismdb_0522dist.csv",
        "globalterrorismdb_2021Jan-June_1222dist.csv",
    ]
    assert workbook_conversions == {
        "globalterrorismdb_0522dist.xlsx": "globalterrorismdb_0522dist.csv",
        "globalterrorismdb_2021Jan-June_1222dist.xlsx": "globalterrorismdb_2021Jan-June_1222dist.csv",
    }


def test_global_terrorism_database_source_rows_do_not_overlap_on_event_id(conn):
    query = """
    select count(*)
    from source_global_terrorism_database a
    inner join source_global_terrorism_database b using (event_id)
    where
        a.source_file = 'globalterrorismdb_0522dist.csv'
        and b.source_file = 'globalterrorismdb_2021Jan-June_1222dist.csv'
        and a.event_id is not null
    """
    overlap_count = conn.execute(query).fetchone()[0]

    assert overlap_count == 0


def test_step_2_manifest_runs_source_ingestion(conn):
    actual_tables_query = """
    select table_name
    from information_schema.tables
    where
        table_schema = current_schema()
        and table_name like 'source_%'
    """
    terrorism_source_files_query = """
    select source_file
    from source_global_terrorism_database
    group by source_file
    order by source_file
    """
    actual_tables = {table_name for (table_name,) in conn.execute(actual_tables_query).fetchall()}
    row_counts = {}

    for table_name in STEP_2_SOURCE_TABLES:
        row_count_query = f"select count(*) from {sql_identifier(table_name)}"
        row_counts[table_name] = conn.execute(row_count_query).fetchone()[0]

    terrorism_source_files = conn.execute(terrorism_source_files_query).fetchall()
    igo_columns = table_columns(conn, "source_intergovernmental_organizations_dyadic")
    dd_revisited_columns = table_columns(conn, "source_dd_revisited")

    assert STEP_2_SQL == [
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
    assert set(STEP_2_SOURCE_TABLES).issubset(actual_tables)
    assert all(row_count > 0 for row_count in row_counts.values())
    assert terrorism_source_files == [
        ("globalterrorismdb_0522dist.csv",),
        ("globalterrorismdb_2021Jan-June_1222dist.csv",),
    ]
    assert igo_columns[:4] == ["c_code_a", "c_code_b", "country_name_a", "country_name_b"]
    assert dd_revisited_columns[:2] == ["c_code_a", "c_code_b"]


def test_step_2_manifest_runs_descriptive_transformations(conn):
    actual_tables_query = """
    select table_name
    from information_schema.tables
    where table_schema = current_schema()
    """
    actual_tables = {table_name for (table_name,) in conn.execute(actual_tables_query).fetchall()}
    row_counts = {}

    for table_name in STEP_2_TRANSFORMED_TABLES:
        row_count_query = f"select count(*) from {sql_identifier(table_name)}"
        row_counts[table_name] = conn.execute(row_count_query).fetchone()[0]

    participant_columns = table_columns(conn, "participant_descriptives")
    dyad_columns = table_columns(conn, "dyadic_descriptives")

    assert set(STEP_2_TRANSFORMED_TABLES).issubset(actual_tables)
    assert all(row_count > 0 for row_count in row_counts.values())
    assert "timeframe" in participant_columns
    assert "terrorism_deaths" in participant_columns
    assert "concurrent_wars" in participant_columns
    assert "timeframe" in dyad_columns
    assert "alliance" in dyad_columns
    assert "mtops" in dyad_columns
    assert not any(column.endswith(("_x", "_y", "_z")) for column in participant_columns)
    assert not any(column.endswith(("_x", "_y", "_z")) for column in dyad_columns)


def test_step_2_preserves_unknown_descriptive_values(conn):
    displaced_population_unknown_sql = """
    select
        a.c_code,
        a.year,
        b.refugees_originated,
        b.refugees_hosted,
        b.internally_displaced_persons
    from source_forcibly_displaced_populations a
    join country_year_descriptives b on a.c_code = b.c_code
                                     and a.year = b.year
    where
        a.source is null
        and a.hosted_refugees is null
        and a.internally_displaced_persons is null
        and (
            b.refugees_originated is not null
            or b.refugees_hosted is not null
            or b.internally_displaced_persons is not null
        )
    """
    displaced_population_unknown_rows = conn.execute(displaced_population_unknown_sql).fetchall()

    assert displaced_population_unknown_rows == []


def test_step_2_source_metadata_files_match_known_downloads():
    metadata_by_key = {metadata["key"]: metadata for metadata in SOURCE_METADATA}

    assert STEP_2_SOURCE_KEYS == [
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
    assert SOURCE_FILES["formal_alliances_directed_yearly"] == "alliance_v4.1_by_directed_yearly.csv"
    assert (
        metadata_by_key["formal_alliances_directed_yearly"]["downloads"][0]["url"]
        == "https://correlatesofwar.org/wp-content/uploads/version4.1_csv.zip"
    )
    assert SOURCE_FILES["territorial_changes"] == "tc2018.csv"
    assert SOURCE_FILES["forcibly_displaced_populations"] == "FDP2008a.csv"
    assert SOURCE_FILES["colonial_dependency_contiguity"] == "contcold.csv"
    assert SOURCE_FILES["direct_contiguity"] == "contdird.csv"
    assert SOURCE_FILES["defense_cooperation_agreements"] == "DCAD-v1.0-dyadic.csv"
    assert SOURCE_FILES["intergovernmental_organizations_dyadic"] == "dyadic_formatv3.csv"
    assert SOURCE_FILES["diplomatic_exchange"] == "Diplomatic_Exchange_2006v1.csv"
    assert SOURCE_FILES["dd_revisited"] == "ddrevisited_data_v1.csv"
    assert SOURCE_FILES["co_emissions_per_capita"] == "co-emissions-per-capita.csv"
    assert SOURCE_FILES["arms_technology"] == "cow_arms_tech_long.csv"
    assert SOURCE_FILES["atop_dyadic_years"] == "atop5_1ddyr.csv"
    assert SOURCE_FILES["mtops_dyadic"] == "mtopsd150.csv"
    assert SOURCE_FILES["cow_trade_dyadic"] == "Dyadic_COW_4.0.csv"
    assert SOURCE_FILES["cow_trade_national"] == "National_COW_4.0.csv"
    assert metadata_by_key["cow_trade_dyadic"]["downloads"] == metadata_by_key["cow_trade_national"]["downloads"]
    assert SOURCE_FILES["national_material_capabilities"] == "NMC-70-wsupplementary.csv"
    assert metadata_by_key["national_material_capabilities"]["downloads"][0]["nested_zips"] == [
        "NMC-v7-supplemental.zip"
    ]
    assert metadata_by_key["forcibly_displaced_populations"]["downloads"][0]["converted_filename"] == "FDP2008a.csv"
    assert metadata_by_key["dd_revisited"]["downloads"][0]["filename"] == "ddrevisited_data_v1.csv"
    assert "source-data-dd-revisited-v1" in metadata_by_key["dd_revisited"]["downloads"][0]["url"]
    assert all("?dl=1" not in download["url"] for download in metadata_by_key["dd_revisited"]["downloads"])
    assert metadata_by_key["atop_dyadic_years"]["downloads"][1]["filename"] == "atop_5_1_codebook.pdf"
