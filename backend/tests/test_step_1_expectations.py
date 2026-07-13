from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest
from colorama import Fore, Style, init

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = BACKEND_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import (  # noqa: E402
    DEFAULT_CSV_DIR,
    SOURCE_FILES,
    SOURCE_METADATA,
    Pipeline,
    format_query_results,
    sql_identifier,
    sql_literal,
)

init(strip=False)


@pytest.fixture(scope="session")
def step_1_db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    db_path = tmp_path_factory.mktemp("duckdb") / "step_1.duckdb"
    pipeline = Pipeline(db_path=db_path, csv_dir=DEFAULT_CSV_DIR)
    missing = [str(pipeline.path_for(key)) for key in SOURCE_FILES if not pipeline.path_for(key).exists()]

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


def scalar(conn, sql: str):
    return conn.execute(sql).fetchone()[0]


def column_names(conn, table_name: str) -> set[str]:
    query = """
    select column_name
    from information_schema.columns
    where table_name = ?
    """
    return {column_name for (column_name,) in conn.execute(query, [table_name]).fetchall()}


def non_date_column_csv(conn, table_name: str) -> str:
    query = """
    select column_name
    from information_schema.columns
    where
        table_name = ?
        and not regexp_matches(column_name, '^(start|end)_(day|month|year)_[0-9]+$')
    order by ordinal_position
    """
    return ", ".join(column_name for (column_name,) in conn.execute(query, [table_name]).fetchall())


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
        {
            "month": ["warstrtmnth", "warendmnth"],
            "day": ["warstrtday", "warenday"],
            "year": ["warstrtyr", "warendyr"],
        },
    ),
    (
        "interstate_mid_dyads",
        ["disno", "dyindex", "statea", "stateb", "year"],
        {
            "month": ["strtmnth", "endmnth"],
            "day": ["strtday", "endday"],
            "year": ["strtyr", "endyear"],
        },
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

RAW_SOURCE_DATE_ENCODINGS = {
    "interstate_war_dyads": "latin-1",
    "interstate_mid_dyads": "latin-1",
    "interstate_wars": "latin-1",
    "intrastate_wars": "latin-1",
}


def raw_source_date_component_check_sql(
    source_key: str,
    source_file: str,
    source_path: Path,
    row_reference_columns: list[str],
    date_components: dict[str, list[str]],
) -> str:
    encoding = RAW_SOURCE_DATE_ENCODINGS.get(source_key)
    encoding_sql = f", encoding = {sql_literal(encoding)}" if encoding else ""
    row_reference = ", ".join(
        f"'{column_name}=' || coalesce({sql_identifier(column_name)}, '<null>')"
        for column_name in row_reference_columns
    )
    date_component_values = ",\n            ".join(
        f"({sql_literal(column_name)}, {sql_literal(date_part)}, {sql_identifier(column_name)})"
        for date_part, column_names in date_components.items()
        for column_name in column_names
    )

    return f"""
    select
        {sql_literal(source_key)} source_key,
        {sql_literal(source_file)} source_file,
        concat_ws(' | ', {row_reference}) row_reference,
        dates.column_name,
        dates.date_part,
        dates.raw_value
    from read_csv_auto({sql_literal(source_path)}, normalize_names = false, all_varchar = true{encoding_sql})
    cross join lateral (
        values
            {date_component_values}
    ) dates(column_name, date_part, raw_value)
    where
        trim(dates.raw_value) != ''
        and (
            try_cast(dates.raw_value as integer) is null
            or (
                try_cast(dates.raw_value as integer) not in (-9, -8, -7)
                and (
                    (dates.date_part = 'month' and try_cast(dates.raw_value as integer) not between 1 and 12)
                    or (dates.date_part = 'day' and try_cast(dates.raw_value as integer) not between 1 and 31)
                    or (dates.date_part = 'year' and try_cast(dates.raw_value as integer) not between 1500 and 2100)
                )
            )
        )
    """


def test_source_tables_have_download_urls_or_are_marked_local():
    missing = [
        metadata["key"] for metadata in SOURCE_METADATA if not metadata.get("local") and not metadata.get("downloads")
    ]

    assert missing == []


def test_interstate_war_dyad_source_metadata_is_unversioned():
    metadata = next(metadata for metadata in SOURCE_METADATA if metadata["key"] == "interstate_war_dyads")

    assert metadata["version"] == "unversioned"


@dataclass(frozen=True)
class SqlCheckFailure:
    label: str
    sql: str
    summary: str
    detected_rows: str


def failure_summary(check: str, row_count: int) -> str:
    return format_query_results(["check", "row_count"], [(check, row_count)])


def fail_sql_check(title: str, *, failures: list[SqlCheckFailure]) -> None:
    section_color = Style.BRIGHT + Fore.CYAN
    label_color = Style.BRIGHT + Fore.YELLOW
    title_color = Style.BRIGHT + Fore.RED
    data_color = Fore.WHITE
    reset = Style.RESET_ALL
    title_text, _, title_details = title.partition("\n")

    title_section = f"{title_color}{title_text}{reset}"
    if title_details:
        title_section += f"\n\n{data_color}{title_details}{reset}"

    sections = [title_section]

    for failure in failures:
        sections.append(
            "\n\n".join(
                [
                    f"{label_color}{failure.label}:{reset}",
                    f"{section_color}SQL query:{reset}\n\n{data_color}{failure.sql}{reset}",
                    f"{section_color}Failure summary:{reset}\n\n{data_color}{failure.summary.strip()}{reset}",
                    f"{section_color}Detected rows:{reset}\n\n{data_color}{failure.detected_rows.strip()}{reset}",
                ]
            )
        )

    pytest.fail("\n\n".join(["", *sections]), pytrace=False)


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

    checks = [f"""
        select
            '{table_name}' table_name,
            '{column_name}' column_name,
            count(*) negative_count
        from {table_name}
        where
            {column_name} < 0
            and not ({column_name} = -7 and '{column_name}' like 'end_year_%')
        """ for table_name, column_name in date_columns]

    unexpected = [row for row in conn.execute(" union all ".join(checks) + " order by 1, 2").fetchall() if row[2] != 0]

    assert unexpected == []


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
    ],
)
def test_date_macros_capture_step_1_date_assumptions(conn, expression, expected):
    actual = scalar(conn, f"select {expression}")

    if isinstance(expected, str):
        actual = str(actual)

    assert actual == expected


def test_raw_source_date_components_use_valid_domains(conn):
    pipeline = Pipeline(csv_dir=DEFAULT_CSV_DIR)
    failures = []

    for source_key, row_reference_columns, date_components in RAW_SOURCE_DATE_COMPONENTS:
        source_file = Path(SOURCE_FILES[source_key]).name
        source_path = pipeline.prepared_path_for(source_key)
        flagged_rows_sql = raw_source_date_component_check_sql(
            source_key, source_file, source_path, row_reference_columns, date_components
        )
        count_sql = f"""
        with

        flagged_rows as (
        {flagged_rows_sql})

        select count(*)
        from flagged_rows
        """
        flagged_count = scalar(conn, count_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = f"""
        with

        flagged_rows as (
        {flagged_rows_sql})

        select *
        from flagged_rows
        order by source_key, row_reference, column_name
        limit 50
        """
        result = conn.execute(detected_rows_sql)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        failures.append(
            SqlCheckFailure(
                label=source_key,
                sql=detected_rows_sql,
                summary=failure_summary(source_key, flagged_count),
                detected_rows=format_query_results(columns, rows),
            )
        )

    if failures:
        fail_sql_check("Raw source date components outside valid domains:", failures=failures)


def test_source_resolved_date_pairs_do_not_start_after_they_end(conn):
    failures = []
    output_column_allowlist = [
        "war_num",
        "disno",
        "dyindex",
        "war_name",
        "war_type",
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
        count_sql = f"""
        with

        flagged_rows as (
        {flagged_rows_sql})

        select count(*)
        from flagged_rows
        """
        flagged_count = scalar(conn, count_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = f"""
        with

        flagged_rows as (
        {flagged_rows_sql})

        select *
        from flagged_rows
        order by all
        limit 50
        """
        result = conn.execute(detected_rows_sql)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        failures.append(
            SqlCheckFailure(
                label=table_name,
                sql=detected_rows_sql,
                summary=failure_summary(table_name, flagged_count),
                detected_rows=format_query_results(columns, rows),
            )
        )

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
        count_sql = f"""
        with

        flagged_rows as (
        {flagged_rows_sql})

        select count(*)
        from flagged_rows
        """
        flagged_count = scalar(conn, count_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = f"""
        with

        flagged_rows as (
        {flagged_rows_sql})

        select *
        from flagged_rows
        order by all
        limit 50
        """
        result = conn.execute(detected_rows_sql)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        failures.append(
            SqlCheckFailure(
                label=table_name,
                sql=detected_rows_sql,
                summary=failure_summary(table_name, flagged_count),
                detected_rows=format_query_results(columns, rows),
            )
        )

    if failures:
        fail_sql_check("Source date pairs with missing required date bounds:", failures=failures)


def test_calculated_and_transient_source_columns_are_not_materialized(conn):
    assert {"batdths", "durindx", "battle_deaths_total"}.isdisjoint(column_names(conn, "source_interstate_war_dyads"))
    assert {"war_dyad_role_a", "war_dyad_role_b"}.isdisjoint(column_names(conn, "source_interstate_war_dyads"))
    assert {"role_a", "role_b", "dyad_role_a", "dyad_role_b"}.issubset(
        column_names(conn, "source_interstate_war_dyads")
    )
    assert {"durindx", "duration", "cumdurat", "war_num"}.isdisjoint(column_names(conn, "source_interstate_mid_dyads"))
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
        result = conn.execute(detected_rows_sql)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        failures.append(
            SqlCheckFailure(
                label=table_name,
                sql=detected_rows_sql,
                summary=failure_summary(table_name, flagged_count),
                detected_rows=format_query_results(columns, rows),
            )
        )

    if failures:
        fail_sql_check("Source transition war references should be positive war numbers or null:", failures=failures)


def test_source_interstate_war_dyad_data_entry_fixes_are_applied(conn):
    query = """
    select
        start_month_1,
        start_year_1
    from source_interstate_war_dyads
    where
        war_num = 106
        and dyindex = 257.03
        and c_code_a = 2
        and c_code_b = 300
    """
    assert conn.execute(query).fetchone() == (None, 1918)

    query = """
    select end_year_1
    from source_interstate_war_dyads
    where
        war_num = 106
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
        war_num = 139
        and dyindex = 1694.001
        and c_code_a = 800
        and c_code_b = 710
    """
    assert scalar(conn, query) == 5569


def test_source_interstate_mid_fatality_levels_are_converted_to_estimates(conn):
    count_sql = """
    select count(*)
    from source_interstate_mid_dyads
    where
        coalesce(battle_deaths_estimated_a, -1) not in (-1, 0, 25, 100, 250, 500, 999, 1000)
        or coalesce(battle_deaths_estimated_b, -1) not in (-1, 0, 25, 100, 250, 500, 999, 1000)
    """
    assert scalar(conn, count_sql) == 0

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
        label = f"{table_name}.{', '.join(column_names)}"
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
            '{', '.join(column_names)}' column_name,
            {output_columns}
        from {table_name}
        where {null_predicate}
        order by all
        limit 50
        """
        result = conn.execute(detected_rows_sql)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        failures.append(
            SqlCheckFailure(
                label=label,
                sql=detected_rows_sql,
                summary=failure_summary(label, null_count),
                detected_rows=format_query_results(columns, rows),
            )
        )

    if failures:
        fail_sql_check("Null battle-death source fields:", failures=failures)


def test_source_adjusted_mid_war_number_relationships_are_applied(conn):
    count_sql = """
    select count(*)
    from source_file_versions
    where
        source_key = 'interstate_mid_dyads'
        and source_file = 'dyadic_mid_4.03.csv'
        and source_version = '4.03'
    """
    assert scalar(conn, count_sql) == 1

    query = """
    select
        a.disno,
        a.war_num
    from source_interstate_mid_war_num_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    order by 1, 2
    """
    actual_assignments = set(conn.execute(query).fetchall())

    assert actual_assignments == {(3582, 139), (3583, 139), (3585, 139), (4182, 4182), (4339, 905)}

    count_sql = """
    select count(*)
    from source_interstate_war_metadata_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    where
        a.war_num = 4182
        and a.war_name = 'Israeli–Hezbollah Conflict (South Lebanon)'
    """
    assert scalar(conn, count_sql) == 1

    assert scalar(conn, "select count(*) from source_interstate_wars where war_num = 4182") == 0

    count_sql = """
    select count(*)
    from source_interstate_war_dyads
    where
        disno in (3582, 3583, 3585, 4182, 4339)
        and c_code_a is null
        and c_code_b is null
    """
    assert scalar(conn, count_sql) == 0

    count_sql = """
    select count(*)
    from wars
    where
        war_num = 4182
        and war_name = 'Israeli–Hezbollah Conflict (South Lebanon)'
        and total_participants = 2
        and total_dyads = 1
    """
    assert scalar(conn, count_sql) == 1


def test_source_adjusted_mid_participant_side_assignments_are_applied(conn):
    query = """
    select
        c_code,
        participant,
        side
    from participants
    where war_num = 4182
    order by 1, 2
    """
    actual_sides = set(conn.execute(query).fetchall())

    assert actual_sides == {(660, "Lebanon", 1), (666, "Israel", 2)}


def test_source_adjustment_inserts_do_not_duplicate_existing_source_facts(conn):
    query = """
    with

    duplicated_adjustments as (

    select 'source_interstate_mid_war_num_adjustments' adjustment_table
    from source_interstate_mid_war_num_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    join source_interstate_war_dyads c on a.disno = c.disno
                                       and a.war_num = c.war_num
    union all
    select 'source_interstate_war_metadata_adjustments' adjustment_table
    from source_interstate_war_metadata_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    join source_interstate_wars c on a.war_num = c.war_num
                                   and a.war_name = c.war_name
                                   and a.war_type = c.war_type
    union all
    select 'source_participant_side_adjustments' adjustment_table
    from source_participant_side_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    join source_interstate_wars c on a.war_num = c.war_num
                                   and a.c_code = c.c_code
                                   and clean_participant(a.participant) = clean_participant(c.participant)
                                   and a.side = c.side)

    select count(*)
    from duplicated_adjustments
    """
    assert scalar(conn, query) == 0


def test_interstate_war_source_rows_are_participant_rows(conn):
    count_sql = """
    select count(*)
    from source_interstate_wars
    where c_code is null
    """
    assert scalar(conn, count_sql) == 0


def test_source_intrastate_war_data_entry_fixes_are_applied(conn):
    assert scalar(conn, "select count(*) from source_intrastate_wars where war_num = 977") == 0

    count_sql = """
    select count(*)
    from source_intrastate_wars
    where
        war_num = 976
        and start_year_1 != 2011
    """
    assert scalar(conn, count_sql) == 0

    count_sql = """
    select count(*)
    from source_intrastate_wars
    where
        war_num in (942, 990.4, 991, 991.4, 992.5)
        and end_year_1 != -7
    """
    assert scalar(conn, count_sql) == 0


def test_source_wars_named_present_or_ongoing_are_marked_ongoing(conn):
    detected_rows_sql = """
    with

    named_ongoing_wars as (

    select
        'source_interstate_wars' source_table,
        war_num,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) ongoing_war
    from source_interstate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2
    union all
    select
        'source_extrastate_wars' source_table,
        war_num,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) ongoing_war
    from source_extrastate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2
    union all
    select
        'source_intrastate_wars' source_table,
        war_num,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2), ongoing_war(end_year_3), ongoing_war(end_year_4))) ongoing_war
    from source_intrastate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2)

    select *
    from named_ongoing_wars
    where ongoing_war = 0
    order by source_table, war_num
    """
    result = conn.execute(detected_rows_sql)
    rows = result.fetchall()
    columns = [column[0] for column in result.description]

    if rows:
        fail_sql_check(
            "Source wars named present or ongoing should retain the ongoing end-year marker.",
            failures=[
                SqlCheckFailure(
                    label="named ongoing source wars without ongoing marker",
                    sql=detected_rows_sql,
                    summary=failure_summary("named ongoing source wars without ongoing marker", len(rows)),
                    detected_rows=format_query_results(columns, rows),
                )
            ],
        )


def test_mid_dyads_do_not_duplicate_source_dyad_overlaps(conn):
    count_sql = """
    select count(*)
    from dyads_after_mid a
    join dyads_after_sources b on a.war_num = b.war_num
                              and a.c_code_a = b.c_code_a
                              and a.c_code_b = b.c_code_b
                              and least(a.end_date, b.end_date) >= greatest(a.start_date, b.start_date)
    where
        a.battle_deaths_estimated_a = 1
        or a.battle_deaths_estimated_b = 1
    """
    assert scalar(conn, count_sql) == 0


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
    count_sql = f"""
    select count(*)
    from {table_name}
    where {invalid_predicate}
    """
    assert scalar(conn, count_sql) == 0


def test_source_side_assignments_are_valid(conn):
    failures = []
    checks = [("source_interstate_wars", "side", "(1, 2)")]

    for table_name, column_name, valid_values in checks:
        count_sql = f"""
        select count(*)
        from {table_name}
        where {column_name} not in {valid_values}
        """
        invalid_count = scalar(conn, count_sql)

        if invalid_count == 0:
            continue

        output_columns = non_date_column_csv(conn, table_name)
        detected_rows_sql = f"""
        select
            {output_columns}
        from {table_name}
        where {column_name} not in {valid_values}
        order by all
        limit 50
        """
        result = conn.execute(detected_rows_sql)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]

        failures.append(
            SqlCheckFailure(
                label=f"{table_name}.{column_name}",
                sql=detected_rows_sql,
                summary=failure_summary(f"{table_name}.{column_name}", invalid_count),
                detected_rows=format_query_results(columns, rows),
            )
        )

    if failures:
        fail_sql_check("Source side assignments should stay within their valid value domains:", failures=failures)


def test_interstate_war_dyads_use_semantic_participant_sides(conn):
    detected_rows_sql = """
    with

    side_rows as (

    select
        war_num,
        c_code_a c_code,
        participant_a participant,
        side_a side
    from war_dyads
    where
        war_type = 1
        and c_code_a is not null
        and side_a is not null
    union all
    select
        war_num,
        c_code_b c_code,
        participant_b participant,
        side_b side
    from war_dyads
    where
        war_type = 1
        and c_code_b is not null
        and side_b is not null),

    side_conflicts as (

    select
        war_num,
        c_code,
        participant,
        list(distinct side order by side) sides,
        count(*) row_count
    from side_rows
    group by 1, 2, 3
    having count(distinct side) > 1)

    select *
    from side_conflicts
    order by war_num, c_code, participant
    limit 50
    """
    result = conn.execute(detected_rows_sql)
    rows = result.fetchall()
    columns = [column[0] for column in result.description]

    if not rows:
        return

    fail_sql_check(
        "Transformed interstate war dyads should use semantic participant sides.",
        failures=[
            SqlCheckFailure(
                label="interstate war_dyads side conflicts",
                sql=detected_rows_sql,
                summary=failure_summary("interstate war_dyads side conflicts", len(rows)),
                detected_rows=format_query_results(columns, rows),
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
    order by war_num, c_code, participant
    limit 50
    """
    result = conn.execute(detected_rows_sql)
    rows = result.fetchall()
    columns = [column[0] for column in result.description]

    fail_sql_check(
        "Final participant side assignments should be present and in (1, 2, 3):",
        failures=[
            SqlCheckFailure(
                label="participants missing or invalid side rows",
                sql=detected_rows_sql,
                summary=failure_summary("participants missing or invalid side rows", invalid_count),
                detected_rows=format_query_results(columns, rows),
            )
        ],
    )


def test_mid_dyads_resolve_all_mid_war_numbers(conn):
    count_sql = """
    select count(*)
    from dyads_after_mid
    where war_num = -1
    """
    assert scalar(conn, count_sql) == 0

    query = """
    select
        war_num,
        war_name,
        c_code_a,
        c_code_b,
        participant_a,
        participant_b
    from dyads_after_mid
    where war_num = 4182
    order by 1, 2, 3, 4, 5, 6
    """
    actual_dyads = set(conn.execute(query).fetchall())

    assert actual_dyads == {
        (4182, "Israeli–Hezbollah Conflict (South Lebanon)", 660, 666, "Lebanon", "Israel"),
        (4182, "Israeli–Hezbollah Conflict (South Lebanon)", 666, 660, "Israel", "Lebanon"),
    }


def test_dyads_apply_final_transformation_assumptions(conn):
    count_sql = """
    select count(*)
    from dyads
    where
        participant_a is null
        or participant_b is null
        or participant_a = '-8'
        or participant_b = '-8'
    """
    assert scalar(conn, count_sql) == 0

    count_sql = """
    select count(*)
    from dyad_years
    where
        year < extract(year from start_date)::integer
        or year > extract(year from end_date)::integer
    """
    assert scalar(conn, count_sql) == 0

    count_sql = """
    select count(*)
    from dyads
    where
        c_code_a = c_code_b
        and participant_a = participant_b
    """
    assert scalar(conn, count_sql) == 0

    count_sql = """
    with

    duplicate_dyads as (

    select
        war_num,
        c_code_a,
        participant_a,
        c_code_b,
        participant_b
    from dyads
    group by 1, 2, 3, 4, 5
    having count(*) > 1)

    select count(*)
    from duplicate_dyads
    """
    assert scalar(conn, count_sql) == 0

    count_sql = """
    select count(*)
    from dyads a
    join dyads b on a.war_num = b.war_num
                 and a.c_code_a = b.c_code_b
                 and a.participant_a = b.participant_b
                 and a.c_code_b = b.c_code_a
                 and a.participant_b = b.participant_a
    """
    assert scalar(conn, count_sql) == 0
    assert scalar(conn, "select sum(total_dyads) from wars") == scalar(conn, "select count(*) from dyads")


def test_dyads_retain_named_non_state_anchor_dyads(conn):
    query = """
    select
        least(participant_a, participant_b),
        greatest(participant_a, participant_b)
    from dyads
    where
        war_num = 940.8
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
