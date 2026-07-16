from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest
from shared import (  # noqa: E402
    CLEAN_PARTICIPANT_TEXT_ASSUMPTIONS,
    RAW_SOURCE_DATE_ENCODING_OVERRIDES,
    apply_legacy_participant_replacements,
    clean_text_python,
    column_names,
    fail_if_detected_rows,
    fail_sql_check,
    flagged_row_count,
    flagged_rows_query,
    non_date_column_csv,
    null_problem_cells,
    participant_input_values,
    participant_name_replacements,
    problem_cells_for_columns,
    problem_cells_matching,
    query_result,
    raw_source_date_component_check_sql,
    scalar,
    sql_check_failure,
)

from pipeline import SOURCE_FILES, STEP_1_SOURCE_KEYS, Pipeline, sql_identifier  # noqa: E402


@pytest.fixture(scope="session")
def step_1_db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    db_path = tmp_path_factory.mktemp("duckdb") / "step_1.duckdb"
    pipeline = Pipeline(db_path=db_path)
    missing = [str(pipeline.path_for(key)) for key in STEP_1_SOURCE_KEYS if not pipeline.path_for(key).exists()]

    if missing:
        pytest.skip("Step 1 source CSVs are not available:\n" + "\n".join(missing))

    with pipeline.connect() as conn:
        pipeline.run_step_1(conn)

    return db_path


@pytest.fixture()
def conn(step_1_db_path: Path):
    connection = duckdb.connect(str(step_1_db_path), read_only=True)
    try:
        yield connection
    finally:
        connection.close()


SOURCE_DATE_PAIR_TABLES = [
    ("source_interstate_wars", 2),
    ("source_interstate_war_dyads", 1),
    ("source_interstate_mid_dyads", 1),
    ("source_extrastate_wars", 2),
    ("source_intrastate_wars", 4),
]

RAW_SOURCE_DATE_COMPONENTS = [
    (
        "interstate_war_dyads",
        ["warnum", "disno", "dyindex", "statea", "stateb", "year"],
        {"month": ["warstrtmnth", "warendmnth"], "day": ["warstrtday", "warenday"], "year": ["warstrtyr", "warendyr"]},
    ),
    (
        "interstate_mid_dyads",
        ["disno", "dyindex", "statea", "stateb", "year"],
        {"month": ["strtmnth", "endmnth"], "day": ["strtday", "endday"], "year": ["strtyr", "endyear"]},
    ),
    (
        "extrastate_wars",
        ["WarNum", "WarName", "ccode1", "ccode2"],
        {
            "month": ["StartMonth1", "EndMonth1", "StartMonth2", "EndMonth2"],
            "day": ["StartDay1", "EndDay1", "StartDay2", "EndDay2"],
            "year": ["StartYear1", "EndYear1", "StartYear2", "EndYear2"],
        },
    ),
    (
        "interstate_wars",
        ["WarNum", "WarName", "ccode", "StateName"],
        {
            "month": ["StartMonth1", "EndMonth1", "StartMonth2", "EndMonth2"],
            "day": ["StartDay1", "EndDay1", "StartDay2", "EndDay2"],
            "year": ["StartYear1", "EndYear1", "StartYear2", "EndYear2"],
        },
    ),
    (
        "intrastate_wars",
        ["WarNum", "WarName", "CcodeA", "CcodeB"],
        {
            "month": ["StartMo1", "EndMo1", "StartMo2", "EndMo2", "StartMo3", "EndMo3", "StartMo4", "EndMo4"],
            "day": ["StartDy1", "EndDy1", "StartDy2", "EndDy2", "StartDy3", "EndDy3", "StartDy4", "EndDy4"],
            "year": ["StartYr1", "EndYr1", "StartYr2", "EndYr2", "StartYr3", "EndYr3", "StartYr4", "EndYr4"],
        },
    ),
]


def test_negative_date_sentinels_are_cleaned_except_ongoing_end_year(conn):
    query = """
    select
        table_name,
        column_name
    from information_schema.columns
    where
        table_name like 'source_%'
        and regexp_matches(column_name, '^(start|end)_(day|month|year)_[0-9]+$')
    order by table_name, column_name
    """
    date_columns = conn.execute(query).fetchall()
    failures = []

    for table_name, column_name in date_columns:
        flagged_rows_sql = f"""
        select
            {table_name!r} table_name,
            {column_name!r} column_name,
            {sql_identifier(column_name)} invalid_value
        from {sql_identifier(table_name)}
        where
            {sql_identifier(column_name)} < 0
            and not ({sql_identifier(column_name)} = -7 and {column_name!r} like 'end_year_%')
        order by invalid_value
        limit 50
        """
        flagged_count = flagged_row_count(conn, flagged_rows_sql)

        if flagged_count == 0:
            continue

        detected_rows = query_result(conn, flagged_rows_sql)
        problem_cells = problem_cells_for_columns(detected_rows, {"invalid_value"})
        failures.append(
            sql_check_failure(
                f"{table_name}.{column_name}", flagged_rows_sql, flagged_count, detected_rows, problem_cells
            )
        )

    if failures:
        fail_sql_check("Negative date sentinels should be cleaned except ongoing end years:", failures=failures)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("cow_date(2012, 10, null, 12, 31)", "2012-10-31"),
        ("cow_date(2012, null, null, 12, 31)", "2012-12-31"),
        ("cow_date(2012, 2, 31, 12, 31)", "2012-02-29"),
        ("clean_date_month(-9)", None),
        ("clean_date_day(-8)", None),
        ("clean_date_year(-7)", None),
        ("clean_end_year(-7)", -7),
        ("clean_end_year(-8)", None),
        ("ongoing_war(-7)", 1),
        ("ongoing_war(null)", 0),
        ("date_estimated(2012, null, 1)", 1),
        ("date_estimated(2012, 10, 1)", 0),
        ("clean_war_reference(-8)", None),
        ("clean_war_reference(-9)", None),
        ("clean_war_reference(905)", 905),
        ("clean_war_type(-8)", None),
        ("clean_war_type(0)", 0),
        ("clean_war_type(4)", 4),
    ],
)
def test_date_macros_capture_step_1_date_assumptions(conn, expression, expected):
    actual = scalar(conn, f"select {expression}")

    if isinstance(expected, str):
        actual = str(actual)

    assert actual == expected


def test_clean_participant_nested_replacement_inventory_matches_current_sources(conn):
    actual_replacements = set()
    current_inputs = {clean_text_python(value) for value in participant_input_values(conn)}
    expected_replacements = {
        (source, replacement)
        for source, replacement in participant_name_replacements()
        if source in current_inputs and apply_legacy_participant_replacements(source) == replacement
    }

    for input_value in participant_input_values(conn):
        before = clean_text_python(input_value)
        after = apply_legacy_participant_replacements(input_value)

        if before != after:
            actual_replacements.add((before, after))

    assert actual_replacements == expected_replacements


@pytest.mark.parametrize(
    ("input_value", "expected"), [*CLEAN_PARTICIPANT_TEXT_ASSUMPTIONS, *participant_name_replacements()]
)
def test_clean_participant_macro_captures_step_1_participant_assumptions(conn, input_value, expected):
    query = """
    select clean_participant(?, any_value(b.replacement))
    from participant_name_replacements b
    where clean_text(?) = b.source
    """
    actual = conn.execute(query, [input_value, input_value]).fetchone()[0]

    assert actual == expected


def test_participant_name_replacements_are_unique_and_materialized(conn):
    assert len(participant_name_replacements()) == len({source for source, _ in participant_name_replacements()})

    query = """
    select
        source,
        replacement
    from participant_name_replacements
    order by 1
    """
    actual_replacements = set(conn.execute(query).fetchall())

    assert actual_replacements == set(participant_name_replacements())


def test_participant_name_replacements_do_not_duplicate_country_code_names(conn):
    query = """
    select
        a.source,
        a.replacement,
        b.c_code
    from participant_name_replacements a
    join country_codes b on a.replacement = b.state_name
    order by 1
    """
    fail_if_detected_rows(
        conn,
        query,
        "Participant name replacements should not duplicate country-code names.",
        "replacement targets matching country-code names",
        {"replacement"},
    )


def test_shared_participant_replacement_targets_do_not_cross_country_codes(conn):
    query = """
    with

    shared_replacements as (

    select replacement
    from participant_name_replacements
    group by 1
    having count(*) > 1),

    replacement_names as (

    select
        a.replacement,
        a.source source_name
    from participant_name_replacements a
    join shared_replacements b on a.replacement = b.replacement
    union
    select
        a.replacement,
        a.replacement source_name
    from participant_name_replacements a
    join shared_replacements b on a.replacement = b.replacement),

    name_codes as (

    select
        a.replacement,
        a.source_name,
        b.c_code
    from replacement_names a
    join source_country_codes b on a.source_name = b.state_name)

    select
        replacement,
        string_agg(distinct source_name || '=' || c_code, ', ' order by source_name || '=' || c_code) matched_names
    from name_codes
    group by 1
    having count(distinct c_code) > 1
    order by 1
    """
    fail_if_detected_rows(
        conn,
        query,
        "Shared participant replacement targets should not cross country codes.",
        "replacement targets with conflicting country codes",
        {"matched_names"},
    )


def test_expected_cow_code_fields_are_not_null(conn):
    checks = [
        ("source_interstate_wars", ["c_code"]),
        ("source_interstate_war_dyads", ["c_code_a", "c_code_b"]),
        ("source_interstate_mid_dyads", ["c_code_a", "c_code_b"]),
        ("source_extrastate_wars", ["c_code_a", "c_code_b"]),
        ("source_intrastate_wars", ["c_code_a", "c_code_b"]),
        ("war_dyads", ["c_code_a", "c_code_b"]),
        ("war_participants", ["c_code"]),
        ("participants", ["c_code"]),
        ("dyads", ["c_code_a", "c_code_b"]),
    ]
    failures = []

    for table_name, columns in checks:
        null_conditions = " or ".join(f"{column} is null" for column in columns)
        count_sql = f"""
        select count(*)
        from {sql_identifier(table_name)}
        where {null_conditions}
        """
        null_count = scalar(conn, count_sql)

        if null_count == 0:
            continue

        detected_rows_sql = f"""
        select *
        from {sql_identifier(table_name)}
        where {null_conditions}
        order by all
        limit 50
        """
        detected_rows = query_result(conn, detected_rows_sql)
        problem_cells = null_problem_cells(detected_rows, set(columns))
        failures.append(sql_check_failure(table_name, detected_rows_sql, null_count, detected_rows, problem_cells))

    if failures:
        fail_sql_check("Expected COW code fields should not be null:", failures=failures)


def test_coded_participant_names_come_from_country_codes(conn):
    query = """
    select
        'war_participants' table_name,
        a.war_id,
        a.c_code,
        a.participant,
        b.state_name
    from war_participants a
    join country_codes b on a.c_code = b.c_code
    where
        a.c_code > 0
        and a.participant != b.state_name
    union all
    select
        'participants' table_name,
        a.war_id,
        a.c_code,
        a.participant,
        b.state_name
    from participants a
    join country_codes b on a.c_code = b.c_code
    where
        a.c_code > 0
        and a.participant != b.state_name
    union all
    select
        'dyads_a' table_name,
        a.war_id,
        a.c_code_a c_code,
        a.participant_a participant,
        b.state_name
    from dyads a
    join country_codes b on a.c_code_a = b.c_code
    where
        a.c_code_a > 0
        and a.participant_a != b.state_name
    union all
    select
        'dyads_b' table_name,
        a.war_id,
        a.c_code_b c_code,
        a.participant_b participant,
        b.state_name
    from dyads a
    join country_codes b on a.c_code_b = b.c_code
    where
        a.c_code_b > 0
        and a.participant_b != b.state_name
    order by 1, 2, 3, 4
    """
    fail_if_detected_rows(
        conn,
        query,
        "Coded participant names should come from country_codes.",
        "coded participant names not matching country_codes",
        {"participant", "state_name"},
    )


def test_raw_source_date_components_use_valid_domains(conn):
    pipeline = Pipeline()
    failures = []

    for source_key, row_reference_columns, date_components in RAW_SOURCE_DATE_COMPONENTS:
        source_file = Path(SOURCE_FILES[source_key]).name
        source_path = pipeline.prepared_path_for(source_key)
        encoding_kwargs = (
            {"encoding": RAW_SOURCE_DATE_ENCODING_OVERRIDES[source_key]}
            if source_key in RAW_SOURCE_DATE_ENCODING_OVERRIDES
            else {}
        )
        flagged_rows_sql = raw_source_date_component_check_sql(
            source_key,
            source_file,
            source_path,
            row_reference_columns,
            date_components,
            **encoding_kwargs,
        )
        flagged_count = flagged_row_count(conn, flagged_rows_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = flagged_rows_query(flagged_rows_sql, "source_key, row_reference, column_name")
        detected_rows = query_result(conn, detected_rows_sql)
        problem_cells = problem_cells_for_columns(detected_rows, {"raw_value"})
        failures.append(sql_check_failure(source_key, detected_rows_sql, flagged_count, detected_rows, problem_cells))

    if failures:
        fail_sql_check("Raw source date components outside valid domains:", failures=failures)


def test_source_resolved_date_pairs_do_not_start_after_they_end(conn):
    failures = []
    output_column_allowlist = [
        "war_id",
        "disno",
        "dyindex",
        "war_name",
        "war_type_id",
        "c_code",
        "c_code_a",
        "c_code_b",
        "participant",
        "participant_a",
        "participant_b",
        "name_a",
        "name_b",
        "side",
        "internationalized",
        "source_year",
    ]

    for table_name, date_pair_count in SOURCE_DATE_PAIR_TABLES:
        existing_columns = column_names(conn, table_name)
        output_columns = ", ".join(
            sql_identifier(column_name) for column_name in output_column_allowlist if column_name in existing_columns
        )
        date_pair_values_sql = ",\n            ".join([f"""(
                {date_pair},
                cow_date(start_year_{date_pair}, start_month_{date_pair}, start_day_{date_pair}, 1, 1),
                cow_end_date(end_year_{date_pair}, end_month_{date_pair}, end_day_{date_pair})
            )""" for date_pair in range(1, date_pair_count + 1)])
        flagged_rows_sql = f"""
        select
            dates.date_pair,
            dates.start_date,
            dates.end_date,
            {output_columns}
        from {table_name}
        cross join lateral (
            values
            {date_pair_values_sql}
        ) dates(date_pair, start_date, end_date)
        where dates.start_date > dates.end_date
        """
        flagged_count = flagged_row_count(conn, flagged_rows_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = flagged_rows_query(flagged_rows_sql, "all")
        detected_rows = query_result(conn, detected_rows_sql)
        problem_cells = problem_cells_for_columns(detected_rows, {"start_date", "end_date"})
        failures.append(sql_check_failure(table_name, detected_rows_sql, flagged_count, detected_rows, problem_cells))

    if failures:
        fail_sql_check("Source date pairs where resolved start_date exceeds end_date:", failures=failures)


def test_source_date_pairs_have_required_year_bounds(conn):
    failures = []

    for table_name, date_pair_count in SOURCE_DATE_PAIR_TABLES:
        output_columns = non_date_column_csv(conn, table_name)
        date_pair_values_sql = ",\n            ".join([f"""(
                {date_pair},
                start_year_{date_pair},
                end_year_{date_pair},
                coalesce(start_year_{date_pair}, start_month_{date_pair}, start_day_{date_pair}, end_year_{date_pair}, end_month_{date_pair}, end_day_{date_pair}) is not null
            )""" for date_pair in range(1, date_pair_count + 1)])
        flagged_rows_sql = f"""
        select
            dates.date_pair,
            if(dates.start_year is null, 'missing_start_year', 'missing_non_ongoing_end_year') date_issue,
            dates.start_year,
            dates.end_year,
            {output_columns}
        from {table_name}
        cross join lateral (
            values
            {date_pair_values_sql}
        ) dates(date_pair, start_year, end_year, date_pair_present)
        where
            dates.date_pair_present
            and (
                dates.start_year is null
                or dates.end_year is null
            )
        """
        flagged_count = flagged_row_count(conn, flagged_rows_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = flagged_rows_query(flagged_rows_sql, "all")
        detected_rows = query_result(conn, detected_rows_sql)
        problem_cells = null_problem_cells(detected_rows, {"start_year", "end_year"})
        failures.append(sql_check_failure(table_name, detected_rows_sql, flagged_count, detected_rows, problem_cells))

    if failures:
        fail_sql_check("Source date pairs with missing required date bounds:", failures=failures)


def test_calculated_and_transient_source_columns_are_not_materialized(conn):
    assert {"batdths", "durindx", "battle_deaths_total"}.isdisjoint(column_names(conn, "source_interstate_war_dyads"))
    assert {"war_dyad_role_a", "war_dyad_role_b"}.isdisjoint(column_names(conn, "source_interstate_war_dyads"))
    assert {"role_a", "role_b", "dyad_role_a", "dyad_role_b"}.issubset(
        column_names(conn, "source_interstate_war_dyads")
    )
    assert {"durindx", "duration", "cumdurat", "war_id"}.isdisjoint(column_names(conn, "source_interstate_mid_dyads"))
    assert {"wduratdays", "wduratmo", "totalbdeaths", "battle_deaths_total"}.isdisjoint(
        column_names(conn, "source_intrastate_wars")
    )

    transient_columns = {
        "disno",
        "dyindex",
        "outcome",
        "outcome_a",
        "outcome_b",
        "start_day_1",
        "start_month_1",
        "start_year_1",
        "end_day_1",
        "end_month_1",
        "end_year_1",
        "source_file",
    }
    for table_name in ["war_participants", "dyads_after_mid", "participants", "dyads"]:
        assert transient_columns.isdisjoint(column_names(conn, table_name))
    for table_name in ["war_participants", "dyads_after_mid", "participants", "dyads", "dyad_years"]:
        assert "ongoing_war" not in column_names(conn, table_name)
    assert "ongoing_war" in column_names(conn, "wars")


def test_war_type_ids_are_populated_when_required_and_exist_in_reference_table(conn):
    checks = [
        ("source_interstate_wars", False),
        ("source_extrastate_wars", False),
        ("source_intrastate_wars", False),
        ("source_interstate_war_metadata_adjustments", True),
        ("dyads_after_mid", True),
    ]

    failures = []

    for table_name, require_war_type in checks:
        missing_war_type_sql = "a.war_type_id is null or" if require_war_type else ""
        flagged_rows_sql = f"""
        select a.*
        from {table_name} a
        left join war_types b on a.war_type_id = b.war_type_id
        where
            {missing_war_type_sql}
            (a.war_type_id is not null and b.war_type_id is null)
        """
        flagged_count = flagged_row_count(conn, flagged_rows_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = flagged_rows_query(flagged_rows_sql, "all")
        detected_rows = query_result(conn, detected_rows_sql)
        problem_cells = problem_cells_for_columns(detected_rows, {"war_type_id"})
        failures.append(sql_check_failure(table_name, detected_rows_sql, flagged_count, detected_rows, problem_cells))

    if failures:
        fail_sql_check(
            "War type ids should be populated when required and exist in the war_types reference table:",
            failures=failures,
        )


def test_source_transition_war_references_are_positive_or_null(conn):
    checks = ["source_interstate_wars", "source_extrastate_wars", "source_intrastate_wars"]

    failures = []

    for table_name in checks:
        count_sql = f"""
        select count(*)
        from {table_name}
        where
            lagging_war < 0
            or leading_war < 0
        """
        flagged_count = scalar(conn, count_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = f"""
        select *
        from {table_name}
        where
            lagging_war < 0
            or leading_war < 0
        order by all
        limit 50
        """
        detected_rows = query_result(conn, detected_rows_sql)
        problem_cells = problem_cells_matching(
            detected_rows, lambda column, value: column in {"lagging_war", "leading_war"} and value < 0
        )
        failures.append(sql_check_failure(table_name, detected_rows_sql, flagged_count, detected_rows, problem_cells))

    if failures:
        fail_sql_check("Source transition war references should be positive war numbers or null:", failures=failures)


def test_source_interstate_war_dyad_data_entry_fixes_are_applied(conn):
    query = """
    select
        start_month_1,
        start_year_1
    from source_interstate_war_dyads
    where
        war_id = 106
        and dyindex = 257.03
        and c_code_a = 2
        and c_code_b = 300
    """
    assert conn.execute(query).fetchone() == (None, 1918)

    query = """
    select end_year_1
    from source_interstate_war_dyads
    where
        war_id = 106
        and dyindex = 257.24
        and c_code_a = 325
        and c_code_b = 355
        and source_year = 1916
    """
    assert scalar(conn, query) == 1918

    query = """
    select battle_deaths_a
    from source_interstate_war_dyads
    where
        war_id = 139
        and dyindex = 1694.001
        and c_code_a = 800
        and c_code_b = 710
    """
    assert scalar(conn, query) == 5569


def test_source_interstate_mid_fatality_levels_are_converted_to_estimates(conn):
    invalid_estimates_sql = """
    select
        disno,
        dyindex,
        c_code_a,
        c_code_b,
        battle_deaths_estimated_a,
        battle_deaths_estimated_b
    from source_interstate_mid_dyads
    where
        coalesce(battle_deaths_estimated_a, -1) not in (-1, 0, 25, 100, 250, 500, 999, 1000)
        or coalesce(battle_deaths_estimated_b, -1) not in (-1, 0, 25, 100, 250, 500, 999, 1000)
    order by disno, dyindex, c_code_a, c_code_b
    """
    fail_if_detected_rows(
        conn,
        invalid_estimates_sql,
        "Interstate MID fatality levels should be converted to known estimates.",
        "invalid MID fatality estimates",
        {"battle_deaths_estimated_a", "battle_deaths_estimated_b"},
    )

    query = """
    select battle_deaths_estimated_a battle_deaths_estimated
    from source_interstate_mid_dyads
    where battle_deaths_estimated_a is not null
    group by 1
    union
    select battle_deaths_estimated_b battle_deaths_estimated
    from source_interstate_mid_dyads
    where battle_deaths_estimated_b is not null
    group by 1
    """
    actual_estimates = {row[0] for row in conn.execute(query).fetchall()}

    assert actual_estimates == {0, 25, 100, 250, 500, 999, 1000}


def test_required_source_battle_death_fields_are_not_null(conn):
    checks = [
        ("source_interstate_wars", ("battle_deaths",)),
        ("source_interstate_war_dyads", ("battle_deaths_a", "battle_deaths_b")),
        ("source_extrastate_wars", ("battle_deaths_a",)),
        ("source_extrastate_wars", ("battle_deaths_b",)),
        ("source_intrastate_wars", ("battle_deaths_a",)),
        ("source_intrastate_wars", ("battle_deaths_b",)),
    ]

    failures = []

    for table_name, column_names in checks:
        null_predicate = " or ".join(f"{column_name} is null" for column_name in column_names)
        count_sql = f"""
        select count(*)
        from {table_name}
        where {null_predicate}
        """
        null_count = scalar(conn, count_sql)

        if null_count == 0:
            continue

        output_columns = non_date_column_csv(conn, table_name)
        detected_rows_sql = f"""
        select
            '{', '.join(column_names)}' checked_columns,
            {output_columns}
        from {table_name}
        where {null_predicate}
        order by all
        limit 50
        """
        detected_rows = query_result(conn, detected_rows_sql)
        problem_cells = null_problem_cells(detected_rows, set(column_names))
        failures.append(sql_check_failure(table_name, detected_rows_sql, null_count, detected_rows, problem_cells))

    if failures:
        fail_sql_check("Null battle-death source fields:", failures=failures)


def test_source_adjusted_mid_war_id_relationships_are_applied(conn):
    query = """
    select
        source_file,
        source_version
    from source_file_versions
    where source_key = 'interstate_mid_dyads'
    """
    actual_source_file_version = conn.execute(query).fetchone()

    assert actual_source_file_version == ("dyadic_mid_4.03.csv", "4.03")

    query = """
    select
        a.disno,
        a.war_id
    from source_interstate_mid_war_id_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                               and a.source_version = b.source_version
    order by 1, 2
    """
    actual_assignments = set(conn.execute(query).fetchall())

    assert actual_assignments == {(3582, 139), (3583, 139), (3585, 139), (4182, 4182), (4339, 905)}

    query = """
    select
        a.source_key,
        a.source_version,
        a.war_name,
        a.war_type_id
    from source_interstate_war_metadata_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                               and a.source_version = b.source_version
    where a.war_id = 4182
    """
    actual_war_metadata = conn.execute(query).fetchone()

    assert actual_war_metadata == ("interstate_mid_dyads", "4.03", "Israeli–Hezbollah Conflict (South Lebanon)", 1)

    old_war_rows_sql = """
    select
        war_id,
        war_name,
        c_code,
        participant
    from source_interstate_wars
    where war_id = 4182
    order by c_code, participant
    """
    fail_if_detected_rows(
        conn,
        old_war_rows_sql,
        "Adjusted MID war 4182 should not also exist in source_interstate_wars.",
        "unexpected source_interstate_wars rows for 4182",
        {"war_id"},
    )

    missing_codes_sql = """
    select
        disno,
        dyindex,
        war_id,
        c_code_a,
        c_code_b,
        source_year,
        role_a,
        role_b
    from source_interstate_war_dyads
    where
        disno in (3582, 3583, 3585, 4182, 4339)
        and c_code_a is null
        and c_code_b is null
    order by disno, dyindex
    """
    fail_if_detected_rows(
        conn,
        missing_codes_sql,
        "Adjusted MID relationship rows should retain participant codes.",
        "adjusted MID relationship rows without codes",
        {"c_code_a", "c_code_b"},
    )

    query = """
    select
        war_name,
        war_type_id,
        total_participants,
        total_dyads
    from wars
    where war_id = 4182
    """
    actual_war = conn.execute(query).fetchone()

    assert actual_war == ("Israeli–Hezbollah Conflict (South Lebanon)", 1, 2, 1)


def test_source_adjusted_mid_participant_side_assignments_are_applied(conn):
    query = """
    select
        c_code,
        participant,
        side
    from participants
    where war_id = 4182
    order by 1, 2
    """
    actual_sides = set(conn.execute(query).fetchall())

    assert actual_sides == {(660, "Lebanon", 1), (666, "Israel", 2)}


def test_source_adjusted_interstate_war_dyads_are_applied(conn):
    query = """
    select
        source_key,
        source_version,
        war_id,
        c_code_a,
        c_code_b,
        start_date,
        end_date
    from source_interstate_war_dyad_adjustments
    order by c_code_a, c_code_b
    """
    actual_adjustments = conn.execute(query).fetchall()

    assert actual_adjustments == [
        ("interstate_war_dyads", "unversioned", 106, 740, 255, date(1914, 8, 23), date(1918, 11, 11)),
        ("interstate_war_dyads", "unversioned", 106, 740, 300, date(1914, 8, 23), date(1918, 11, 3)),
    ]

    query = """
    select
        war_id,
        c_code_a,
        participant_a,
        c_code_b,
        participant_b,
        start_date,
        end_date
    from dyads
    where
        war_id = 106
        and (
            (c_code_a = 255 and c_code_b = 740)
            or (c_code_a = 300 and c_code_b = 740)
        )
    order by c_code_a, c_code_b
    """
    actual_dyads = conn.execute(query).fetchall()

    assert actual_dyads == [
        (106, 255, "Germany", 740, "Japan", date(1914, 8, 23), date(1918, 11, 11)),
        (106, 300, "Austria-Hungary", 740, "Japan", date(1914, 8, 23), date(1918, 11, 3)),
    ]


def test_source_adjustment_inserts_do_not_duplicate_existing_source_facts(conn):
    duplicate_facts_sql = """
    select
        'source_interstate_mid_war_id_adjustments' adjustment_table,
        a.source_key,
        a.source_version,
        a.war_id,
        'disno=' || a.disno row_key,
        null participant,
        null side
    from source_interstate_mid_war_id_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                               and a.source_version = b.source_version
    join source_interstate_war_dyads c on a.disno = c.disno
                                      and a.war_id = c.war_id
    union all
    select
        'source_interstate_war_metadata_adjustments' adjustment_table,
        a.source_key,
        a.source_version,
        a.war_id,
        a.war_name row_key,
        null participant,
        null side
    from source_interstate_war_metadata_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                               and a.source_version = b.source_version
    join source_interstate_wars c on a.war_id = c.war_id
                                 and a.war_name = c.war_name
                                 and a.war_type_id = c.war_type_id
    union all
    select
        'source_interstate_war_dyad_adjustments' adjustment_table,
        a.source_key,
        a.source_version,
        a.war_id,
        'c_code_a=' || a.c_code_a || ', c_code_b=' || a.c_code_b row_key,
        null participant,
        null side
    from source_interstate_war_dyad_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                               and a.source_version = b.source_version
    join source_interstate_war_dyads c on a.war_id = c.war_id
                                      and (
                                          (a.c_code_a = c.c_code_a and a.c_code_b = c.c_code_b)
                                          or (a.c_code_a = c.c_code_b and a.c_code_b = c.c_code_a)
                                      )
    union all
    select
        'source_participant_side_adjustments' adjustment_table,
        a.source_key,
        a.source_version,
        a.war_id,
        'c_code=' || a.c_code row_key,
        clean_participant(a.participant, d.replacement) participant,
        a.side
    from source_participant_side_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                               and a.source_version = b.source_version
    join source_interstate_wars c on a.war_id = c.war_id
                                 and a.c_code = c.c_code
    left join participant_name_replacements d on clean_text(a.participant) = d.source
    left join participant_name_replacements e on clean_text(c.participant) = e.source
    where
        clean_participant(a.participant, d.replacement) = clean_participant(c.participant, e.replacement)
        and a.side = c.side
    order by adjustment_table, source_key, source_version, war_id, row_key, participant, side
    """
    fail_if_detected_rows(
        conn,
        duplicate_facts_sql,
        "Source adjustment inserts should not duplicate existing source facts.",
        "source adjustment rows duplicating source facts",
        {"adjustment_table", "row_key"},
    )


def test_source_intrastate_war_data_entry_fixes_are_applied(conn):
    removed_war_rows_sql = """
    select
        war_id,
        war_name,
        c_code_a,
        c_code_b
    from source_intrastate_wars
    where war_id = 977
    order by c_code_a, c_code_b
    """
    fail_if_detected_rows(
        conn,
        removed_war_rows_sql,
        "Intrastate war data-entry fixes should remove war 977.",
        "unexpected source_intrastate_wars rows for 977",
        {"war_id"},
    )

    query = """
    select start_year_1
    from source_intrastate_wars
    where war_id = 976
    """
    actual_start_years = {row[0] for row in conn.execute(query).fetchall()}

    assert actual_start_years == {2011}

    query = """
    select
        war_id,
        end_year_1
    from source_intrastate_wars
    where war_id in (942, 990.4, 991, 991.4, 992.5)
    order by 1
    """
    actual_end_years = set(conn.execute(query).fetchall())

    assert actual_end_years == {(942, -7), (990.4, -7), (991, -7), (991.4, -7), (992.5, -7)}


def test_source_wars_named_present_or_ongoing_are_marked_ongoing(conn):
    detected_rows_sql = """
    select
        'source_interstate_wars' source_table,
        war_id,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) ongoing_war
    from source_interstate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2
    having max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) = 0
    union all
    select
        'source_extrastate_wars' source_table,
        war_id,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) ongoing_war
    from source_extrastate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2
    having max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) = 0
    union all
    select
        'source_intrastate_wars' source_table,
        war_id,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2), ongoing_war(end_year_3), ongoing_war(end_year_4))) ongoing_war
    from source_intrastate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2
    having max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2), ongoing_war(end_year_3), ongoing_war(end_year_4))) = 0
    order by source_table, war_id
    """
    detected_rows = query_result(conn, detected_rows_sql)

    if detected_rows.rows:
        problem_cells = problem_cells_for_columns(detected_rows, {"ongoing_war"})
        fail_sql_check(
            "Source wars named present or ongoing should retain the ongoing end-year marker.",
            failures=[
                sql_check_failure(
                    label="named ongoing source wars without ongoing marker",
                    sql=detected_rows_sql,
                    row_count=len(detected_rows.rows),
                    result=detected_rows,
                    problem_cells=problem_cells,
                )
            ],
        )


def test_mid_dyads_do_not_duplicate_source_dyad_overlaps(conn):
    duplicate_overlaps_sql = """
    select
        a.war_id,
        a.c_code_a,
        a.c_code_b,
        a.participant_a,
        a.participant_b,
        a.start_date,
        a.end_date
    from dyads_after_mid a
    join dyads_after_sources b on a.war_id = b.war_id
                              and a.c_code_a = b.c_code_a
                              and a.c_code_b = b.c_code_b
                              and least(a.end_date, b.end_date) >= greatest(a.start_date, b.start_date)
    where
        a.battle_deaths_estimated_a = 1
        or a.battle_deaths_estimated_b = 1
    order by a.war_id, a.c_code_a, a.c_code_b, a.start_date, a.end_date
    """
    fail_if_detected_rows(
        conn,
        duplicate_overlaps_sql,
        "MID dyads should not duplicate source dyad overlaps.",
        "MID dyads duplicating source dyad overlaps",
        {"war_id", "c_code_a", "c_code_b"},
    )


@pytest.mark.parametrize(
    ("table_name", "flag_columns"),
    [
        ("dyads_after_mid", ("battle_deaths_estimated_a", "battle_deaths_estimated_b")),
        ("war_participants", ("battle_deaths_estimated",)),
        ("participants", ("battle_deaths_estimated",)),
    ],
)
def test_battle_death_estimate_flags_are_binary(conn, table_name, flag_columns):
    invalid_predicate = " or ".join(
        f"{column_name} not in (0, 1) or {column_name} is null" for column_name in flag_columns
    )
    invalid_flags_sql = f"""
    select *
    from {table_name}
    where {invalid_predicate}
    order by all
    limit 50
    """
    fail_if_detected_rows(
        conn,
        invalid_flags_sql,
        f"{table_name} battle-death estimate flags should be binary.",
        f"invalid {table_name} battle-death estimate flags",
        set(flag_columns),
    )


def test_interstate_war_source_rows_are_valid(conn):
    detected_rows_sql = """
    select *
    from source_interstate_wars
    where
        c_code is null
        or side is null
        or side not in (1, 2)
    order by all
    limit 50
    """
    count_sql = """
    select count(*)
    from source_interstate_wars
    where
        c_code is null
        or side is null
        or side not in (1, 2)
    """
    invalid_count = scalar(conn, count_sql)

    if invalid_count == 0:
        return

    detected_rows = query_result(conn, detected_rows_sql)
    problem_cells = problem_cells_matching(
        detected_rows,
        lambda column, value: (column == "c_code" and value is None)
        or (column == "side" and (value is None or value not in (1, 2))),
    )

    fail_sql_check(
        "Interstate war source rows should have participant codes and valid side assignments:",
        failures=[
            sql_check_failure(
                "source_interstate_wars participant rows",
                detected_rows_sql,
                invalid_count,
                detected_rows,
                problem_cells,
            )
        ],
    )


def test_interstate_war_dyads_use_semantic_participant_sides(conn):
    detected_rows_sql = """
    with

    side_rows as (

    select
        war_id,
        c_code_a c_code,
        participant_a participant,
        side_a side
    from war_dyads
    where
        war_type_id = 1
        and c_code_a is not null
        and side_a is not null
    union all
    select
        war_id,
        c_code_b c_code,
        participant_b participant,
        side_b side
    from war_dyads
    where
        war_type_id = 1
        and c_code_b is not null
        and side_b is not null)

    select
        war_id,
        c_code,
        participant,
        list(distinct side order by side) sides,
        count(*) row_count
    from side_rows
    group by 1, 2, 3
    having count(distinct side) > 1
    order by war_id, c_code, participant
    limit 50
    """
    detected_rows = query_result(conn, detected_rows_sql)

    if not detected_rows.rows:
        return

    problem_cells = problem_cells_for_columns(detected_rows, {"sides"})
    fail_sql_check(
        "Transformed interstate war dyads should use semantic participant sides.",
        failures=[
            sql_check_failure(
                "interstate war_dyads side conflicts",
                detected_rows_sql,
                len(detected_rows.rows),
                detected_rows,
                problem_cells,
            )
        ],
    )


def test_final_participant_side_assignments_are_present_and_valid(conn):
    count_sql = """
    select count(*)
    from participants
    where
        side is null
        or side not in (1, 2, 3)
    """
    invalid_count = scalar(conn, count_sql)

    if invalid_count == 0:
        return

    detected_rows_sql = """
    select *
    from participants
    where
        side is null
        or side not in (1, 2, 3)
    order by war_id, c_code, participant
    limit 50
    """
    detected_rows = query_result(conn, detected_rows_sql)
    problem_cells = problem_cells_matching(
        detected_rows, lambda column, value: column == "side" and (value is None or value not in (1, 2, 3))
    )

    fail_sql_check(
        "Final participant side assignments should be present and in (1, 2, 3):",
        failures=[
            sql_check_failure(
                "participants missing or invalid side rows",
                detected_rows_sql,
                invalid_count,
                detected_rows,
                problem_cells,
            )
        ],
    )


def test_mid_dyads_resolve_all_mid_war_ids(conn):
    unresolved_mid_war_ids_sql = """
    select
        war_id,
        war_name,
        c_code_a,
        c_code_b,
        participant_a,
        participant_b
    from dyads_after_mid
    where war_id = -1
    order by c_code_a, c_code_b, participant_a, participant_b
    """
    fail_if_detected_rows(
        conn,
        unresolved_mid_war_ids_sql,
        "MID dyads should resolve all MID war ids.",
        "unresolved MID dyad war ids",
        {"war_id"},
    )

    query = """
    select
        war_id,
        war_name,
        c_code_a,
        c_code_b,
        participant_a,
        participant_b
    from dyads_after_mid
    where war_id = 4182
    order by 1, 2, 3, 4, 5, 6
    """
    actual_dyads = set(conn.execute(query).fetchall())

    assert actual_dyads == {
        (4182, "Israeli–Hezbollah Conflict (South Lebanon)", 660, 666, "Lebanon", "Israel"),
        (4182, "Israeli–Hezbollah Conflict (South Lebanon)", 666, 660, "Israel", "Lebanon"),
    }


def test_dyads_apply_final_transformation_assumptions(conn):
    invalid_participants_sql = """
    select
        war_id,
        c_code_a,
        participant_a,
        c_code_b,
        participant_b
    from dyads
    where
        participant_a is null
        or participant_b is null
        or participant_a = '-8'
        or participant_b = '-8'
    order by war_id, c_code_a, participant_a, c_code_b, participant_b
    """
    fail_if_detected_rows(
        conn,
        invalid_participants_sql,
        "Dyads should have valid final participants.",
        "dyads with invalid participants",
        {"participant_a", "participant_b"},
    )

    invalid_dyad_years_sql = """
    select
        war_id,
        c_code_a,
        participant_a,
        c_code_b,
        participant_b,
        start_date,
        end_date,
        year
    from dyad_years
    where
        year < extract(year from start_date)::integer
        or year > extract(year from end_date)::integer
    order by war_id, c_code_a, participant_a, c_code_b, participant_b, year
    """
    fail_if_detected_rows(
        conn,
        invalid_dyad_years_sql,
        "Dyad years should stay inside each dyad date span.",
        "dyad years outside dyad date span",
        {"year", "start_date", "end_date"},
    )

    dyad_year_coverage_sql = """
    select
        a.war_id,
        a.c_code_a,
        a.participant_a,
        a.c_code_b,
        a.participant_b,
        extract(year from a.start_date)::integer start_year,
        extract(year from a.end_date)::integer end_year,
        extract(year from a.end_date)::integer - extract(year from a.start_date)::integer + 1 expected_years,
        count(b.year) actual_years
    from dyads a
    left join dyad_years b on a.war_id = b.war_id
                          and a.c_code_a = b.c_code_a
                          and a.c_code_b = b.c_code_b
                          and a.participant_a = b.participant_a
                          and a.participant_b = b.participant_b
    group by 1, 2, 3, 4, 5, 6, 7
    having count(b.year) != extract(year from a.end_date)::integer - extract(year from a.start_date)::integer + 1
    order by a.war_id, a.c_code_a, a.participant_a, a.c_code_b, a.participant_b
    """
    fail_if_detected_rows(
        conn,
        dyad_year_coverage_sql,
        "Dyad years should cover every year in each dyad date span.",
        "dyad year coverage gaps",
        {"expected_years", "actual_years"},
    )

    self_dyads_sql = """
    select
        war_id,
        c_code_a,
        participant_a,
        c_code_b,
        participant_b
    from dyads
    where
        c_code_a = c_code_b
        and participant_a = participant_b
    order by war_id, c_code_a, participant_a
    """
    fail_if_detected_rows(
        conn,
        self_dyads_sql,
        "Dyads should not link participants to themselves.",
        "self dyads",
        {"c_code_a", "participant_a", "c_code_b", "participant_b"},
    )

    query = """
    select
        war_id,
        c_code_a,
        participant_a,
        c_code_b,
        participant_b
    from dyads
    group by 1, 2, 3, 4, 5
    having count(*) > 1
    order by 1, 2, 3, 4, 5
    """
    fail_if_detected_rows(
        conn,
        query,
        "Dyads should be unique per war and participant pair.",
        "duplicate dyads",
        {"war_id", "c_code_a", "participant_a", "c_code_b", "participant_b"},
    )

    mirrored_dyads_sql = """
    select
        a.war_id,
        a.c_code_a,
        a.participant_a,
        a.c_code_b,
        a.participant_b
    from dyads a
    join dyads b on a.war_id = b.war_id
                and a.c_code_a = b.c_code_b
                and a.participant_a = b.participant_b
                and a.c_code_b = b.c_code_a
                and a.participant_b = b.participant_a
    order by a.war_id, a.c_code_a, a.participant_a, a.c_code_b, a.participant_b
    """
    fail_if_detected_rows(
        conn,
        mirrored_dyads_sql,
        "Dyads should not retain mirrored participant pairs.",
        "mirrored dyads",
        {"war_id", "c_code_a", "participant_a", "c_code_b", "participant_b"},
    )
    assert scalar(conn, "select sum(total_dyads) from wars") == scalar(conn, "select count(*) from dyads")


def test_dyads_retain_named_non_state_anchor_dyads(conn):
    query = """
    select
        least(participant_a, participant_b),
        greatest(participant_a, participant_b)
    from dyads
    where
        war_id = 940.8
        and (participant_a in ('ICU', 'Eritrea') or participant_b in ('ICU', 'Eritrea'))
    group by 1, 2
    order by 1, 2
    """
    actual_dyads = set(conn.execute(query).fetchall())

    expected_side_1_participants = {"United States of America", "Uganda", "Kenya", "Burundi", "Somalia", "Ethiopia"}

    assert actual_dyads == {
        tuple(sorted((anchor, participant)))
        for anchor in {"ICU", "Eritrea"}
        for participant in expected_side_1_participants
    }
