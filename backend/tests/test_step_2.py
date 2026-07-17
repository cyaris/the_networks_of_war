from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest
from shared import fail_if_detected_rows  # noqa: E402

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
    sql_literal,
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

PARTICIPANT_DESCRIPTOR_COLUMNS = [
    "terrorism_deaths",
    "mid_dyads",
    "mid_dyads_initiated",
    "mid_dyads_targeted",
    "mid_dyads_joined",
    "allied_countries",
    "trade_countries",
    "money_flow_in",
    "money_flow_out",
    "imports",
    "exports",
    "military_expenditure",
    "military_personnel",
    "iron_steel_production",
    "energy_consumption",
    "population",
    "urban_population",
    "urban_population_growth_rate",
    "cinc_score",
    "arms_technologies_used",
    "co2_emissions_per_capita",
    "land_mass_exchange_gain",
    "population_exchange_gain",
    "land_mass_exchange_loss",
    "population_exchange_loss",
    "refugees_originated",
    "refugees_hosted",
    "internally_displaced_persons",
    "concurrent_wars",
]

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

SIGNED_EXCHANGE_COLUMNS = {
    "land_mass_exchange_gain",
    "population_exchange_gain",
    "land_mass_exchange_loss",
    "population_exchange_loss",
}


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


def scalar(conn, sql: str):
    return conn.execute(sql).fetchone()[0]


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
    actual_tables_sql = """
    select table_name
    from information_schema.tables
    where
        table_schema = current_schema()
        and table_name like 'source_%'
    """
    terrorism_source_files_sql = """
    select source_file
    from source_global_terrorism_database
    group by source_file
    order by source_file
    """
    actual_tables = {table_name for (table_name,) in conn.execute(actual_tables_sql).fetchall()}
    row_counts = {}

    for table_name in STEP_2_SOURCE_TABLES:
        row_count_sql = f"select count(*) from {sql_identifier(table_name)}"
        row_counts[table_name] = conn.execute(row_count_sql).fetchone()[0]

    terrorism_source_files = conn.execute(terrorism_source_files_sql).fetchall()
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
    actual_tables_sql = """
    select table_name
    from information_schema.tables
    where table_schema = current_schema()
    """
    actual_tables = {table_name for (table_name,) in conn.execute(actual_tables_sql).fetchall()}
    row_counts = {}

    for table_name in STEP_2_TRANSFORMED_TABLES:
        row_count_sql = f"select count(*) from {sql_identifier(table_name)}"
        row_counts[table_name] = conn.execute(row_count_sql).fetchone()[0]

    participant_columns = table_columns(conn, "participant_descriptives")
    dyad_columns = table_columns(conn, "dyadic_descriptives")

    assert set(STEP_2_TRANSFORMED_TABLES).issubset(actual_tables)
    assert all(row_count > 0 for row_count in row_counts.values())
    assert "timeframe" in participant_columns
    assert "terrorism_deaths" in participant_columns
    assert "concurrent_wars" in participant_columns
    assert "co2_emissions_per_capita" in participant_columns
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


def test_step_2_descriptor_tables_keep_expected_grain(conn):
    checks = [
        ("country_year_descriptives", ["c_code", "year"]),
        ("participant_year_descriptives", ["war_id", "c_code", "participant", "year"]),
        ("participant_descriptives", ["war_id", "c_code", "participant", "timeframe"]),
        ("dyad_year_descriptives", ["war_id", "c_code_a", "c_code_b", "participant_a", "participant_b", "year"]),
        (
            "dyadic_descriptives",
            ["war_id", "c_code_a", "c_code_b", "participant_a", "participant_b", "timeframe"],
        ),
    ]

    for table_name, key_columns in checks:
        key_csv = ", ".join(key_columns)
        duplicate_rows_sql = f"""
        select
            {key_csv},
            count(*) row_count
        from {table_name}
        group by {key_csv}
        having count(*) > 1
        order by {key_csv}
        limit 50
        """
        fail_if_detected_rows(
            conn,
            duplicate_rows_sql,
            "Step 2 descriptor tables should keep one row per expected key.",
            f"duplicate {table_name} grain rows",
            set(key_columns),
        )


def test_step_2_participant_year_descriptives_cover_full_participant_year_spans(conn):
    query = """
    select
        a.war_id,
        a.c_code,
        a.participant,
        extract(year from a.start_date)::integer start_year,
        extract(year from a.end_date)::integer end_year,
        extract(year from a.end_date)::integer - extract(year from a.start_date)::integer + 1 expected_years,
        count(b.year) actual_years
    from participants a
    left join participant_year_descriptives b on a.war_id = b.war_id
                                              and a.c_code = b.c_code
                                              and a.participant = b.participant
    where a.c_code > 0
    group by 1, 2, 3, 4, 5
    having count(b.year) != extract(year from a.end_date)::integer - extract(year from a.start_date)::integer + 1
    order by 1, 2, 3
    limit 50
    """
    fail_if_detected_rows(
        conn,
        query,
        "Participant-year descriptors should cover every coded participant year.",
        "participant-year coverage gaps",
        {"expected_years", "actual_years"},
    )


def test_step_2_co2_emissions_join_into_country_year_descriptives(conn):
    known_match_sql = """
    select count(*)
    from source_co_emissions_per_capita a
    join country_year_descriptives b on b.c_code = 2
                                     and a.year = b.year
    where
        a.country_name = 'United States'
        and a.co2_emissions_per_capita is not null
        and b.co2_emissions_per_capita = a.co2_emissions_per_capita
    """
    assert scalar(conn, known_match_sql) > 0

    missing_co2_sql = """
    select
        a.country_name,
        a.year,
        a.co2_emissions_per_capita,
        c.c_code,
        c.state_name
    from source_co_emissions_per_capita a
    left join participant_name_replacements b on a.country_name = b.source
    join country_codes c on coalesce(b.replacement, a.country_name) = c.state_name
    left join country_year_descriptives d on c.c_code = d.c_code
                                          and a.year = d.year
    where
        a.co2_emissions_per_capita is not null
        and d.co2_emissions_per_capita is null
    order by c.c_code, a.year
    limit 50
    """
    fail_if_detected_rows(
        conn,
        missing_co2_sql,
        "Matched CO2 emissions rows should be available in country_year_descriptives.",
        "missing CO2 descriptor rows",
        {"co2_emissions_per_capita"},
    )


def test_step_2_cinc_score_matches_source_cinc_with_tolerance(conn):
    source_path = Pipeline().paths_for("national_material_capabilities")[0]
    compared_rows_sql = f"""
    select count(*)
    from country_year_descriptives a
    join read_csv_auto({sql_literal(str(source_path))}, normalize_names = false, all_varchar = true) b on a.c_code = clean_int(b.ccode)
                                                                                                       and a.year = clean_int(b.year)
    where
        a.cinc_score is not null
        and clean_number(b.cinc) is not null
    """
    assert scalar(conn, compared_rows_sql) > 0

    mismatch_sql = f"""
    select
        a.c_code,
        a.year,
        a.cinc_score,
        clean_number(b.cinc) source_cinc_score,
        abs(a.cinc_score - clean_number(b.cinc)) abs_diff
    from country_year_descriptives a
    join read_csv_auto({sql_literal(str(source_path))}, normalize_names = false, all_varchar = true) b on a.c_code = clean_int(b.ccode)
                                                                                                       and a.year = clean_int(b.year)
    where
        a.cinc_score is not null
        and clean_number(b.cinc) is not null
        and abs(a.cinc_score - clean_number(b.cinc)) > 0.0000001
    order by abs_diff desc
    limit 50
    """
    fail_if_detected_rows(
        conn,
        mismatch_sql,
        "Derived CINC scores should match the source CINC values within a small floating-point tolerance.",
        "CINC score mismatches",
        {"abs_diff"},
    )


def test_step_2_arms_technologies_used_matches_source_total_use_with_tolerance(conn):
    source_path = Pipeline().paths_for("arms_technology")[0]
    compared_rows_sql = f"""
    select count(*)
    from country_year_descriptives a
    join read_csv_auto({sql_literal(str(source_path))}, normalize_names = false, all_varchar = true) b on a.c_code = clean_int(b.ccode)
                                                                                                       and a.year = clean_int(b.year)
    where
        b.techname = 'Adopted technologies'
        and a.arms_technologies_used is not null
        and clean_number(b.total_use) is not null
    """
    assert scalar(conn, compared_rows_sql) > 0

    mismatch_sql = f"""
    select
        a.c_code,
        a.year,
        a.arms_technologies_used,
        clean_number(b.total_use) source_total_use,
        abs(a.arms_technologies_used - clean_number(b.total_use)) abs_diff
    from country_year_descriptives a
    join read_csv_auto({sql_literal(str(source_path))}, normalize_names = false, all_varchar = true) b on a.c_code = clean_int(b.ccode)
                                                                                                       and a.year = clean_int(b.year)
    where
        b.techname = 'Adopted technologies'
        and a.arms_technologies_used is not null
        and clean_number(b.total_use) is not null
        and abs(a.arms_technologies_used - clean_number(b.total_use)) > 0.0000001
    order by abs_diff desc
    limit 50
    """
    fail_if_detected_rows(
        conn,
        mismatch_sql,
        "Derived arms technology counts should match the source total_use values within a small tolerance.",
        "arms technology total_use mismatches",
        {"abs_diff"},
    )


def test_step_2_descriptor_values_do_not_leak_source_special_codes(conn):
    checks = [
        (
            "country_year_descriptives",
            [
                column
                for column in PARTICIPANT_DESCRIPTOR_COLUMNS
                if column != "concurrent_wars" and column not in SIGNED_EXCHANGE_COLUMNS
            ],
        ),
        (
            "participant_year_descriptives",
            [column for column in PARTICIPANT_DESCRIPTOR_COLUMNS if column not in SIGNED_EXCHANGE_COLUMNS],
        ),
        ("participant_descriptives", PARTICIPANT_DESCRIPTOR_COLUMNS),
        ("dyad_year_descriptives", DYADIC_DESCRIPTOR_COLUMNS),
        ("dyadic_descriptives", DYADIC_DESCRIPTOR_COLUMNS),
    ]

    for table_name, columns in checks:
        for column_name in columns:
            special_codes_sql = f"""
            select
                {table_name!r} table_name,
                {column_name!r} column_name,
                {column_name} descriptor_value
            from {table_name}
            where {column_name} in (-9, -8)
            order by descriptor_value
            limit 50
            """
            fail_if_detected_rows(
                conn,
                special_codes_sql,
                "Step 2 descriptor values should not leak source special-code values.",
                f"special-code values in {table_name}.{column_name}",
                {"descriptor_value"},
            )


def test_step_2_dyadic_descriptor_flags_are_binary(conn):
    for table_name in ["dyad_year_descriptives", "dyadic_descriptives"]:
        for column_name in DYADIC_DESCRIPTOR_COLUMNS:
            invalid_flags_sql = f"""
            select
                {table_name!r} table_name,
                {column_name!r} column_name,
                {column_name} descriptor_value
            from {table_name}
            where
                {column_name} is not null
                and {column_name} not in (0, 1)
            order by descriptor_value
            limit 50
            """
            fail_if_detected_rows(
                conn,
                invalid_flags_sql,
                "Dyadic descriptor flags should be binary.",
                f"non-binary {table_name}.{column_name}",
                {"descriptor_value"},
            )


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
    co_emissions_metadata_url = (
        "https://ourworldindata.org/grapher/co-emissions-per-capita.metadata.json"
        "?v=1&csvType=full&useColumnShortNames=true&utm_source=chatgpt.com"
    )
    assert metadata_by_key["co_emissions_per_capita"]["downloads"][1] == {
        "url": co_emissions_metadata_url,
        "kind": "file",
        "filename": "co-emissions-per-capita.metadata.json",
    }
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
