from __future__ import annotations

from dataclasses import dataclass
import sys
from pathlib import Path
from textwrap import dedent

import duckdb
import pytest
from colorama import Fore, Style, init

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = BACKEND_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import DEFAULT_CSV_DIR, SOURCE_FILES, Pipeline, format_query_results  # noqa: E402

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
    return {
        column_name
        for (column_name,) in conn.execute(
            """
            select column_name
            from information_schema.columns
            where table_name = ?
            """,
            [table_name],
        ).fetchall()
    }


def non_date_column_csv(conn, table_name: str) -> str:

    return ", ".join(
        column_name
        for (column_name,) in conn.execute(
            """
            select column_name
            from information_schema.columns
            where
                table_name = ?
                and not regexp_matches(column_name, '^(start|end)_(day|month|year)_[0-9]+$')
            order by ordinal_position
            """,
            [table_name],
        ).fetchall()
    )


def clean_sql(sql: str) -> str:
    lines = [line.rstrip() for line in dedent(sql).strip().splitlines()]
    cleaned_lines = []
    previous_blank = False

    for line in lines:
        is_blank = line == ""
        if is_blank and previous_blank:
            continue
        cleaned_lines.append(line)
        previous_blank = is_blank

    return "\n".join(cleaned_lines)


def indent_sql(sql: str, spaces: int = 16) -> str:
    return ("\n" + " " * spaces).join(clean_sql(sql).splitlines())


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
                    f"{section_color}SQL query:{reset}\n\n{data_color}{clean_sql(failure.sql)}{reset}",
                    f"{section_color}Failure summary:{reset}\n\n{data_color}{failure.summary.strip()}{reset}",
                    f"{section_color}Detected rows:{reset}\n\n{data_color}{failure.detected_rows.strip()}{reset}",
                ]
            )
        )

    pytest.fail("\n\n".join(["", *sections]), pytrace=False)


def test_fail_sql_check_groups_each_failure_by_query_summary_and_detected_rows():
    with pytest.raises(pytest.fail.Exception) as excinfo:
        fail_sql_check(
            "Bad source rows",
            failures=[
                SqlCheckFailure(
                    label="source rows",
                    sql="select\n    *\nfrom source_table",
                    summary=failure_summary("source rows", 2),
                    detected_rows="detected table",
                )
            ],
        )

    message = str(excinfo.value)

    assert f"{Style.BRIGHT}{Fore.RED}Bad source rows{Style.RESET_ALL}" in message
    assert "SQL queries:" not in message
    assert "Source fields:" not in message
    assert message.index("SQL query:") < message.index("Failure summary:") < message.index("Detected rows:")
    assert f"{Fore.WHITE}{failure_summary('source rows', 2).strip()}{Style.RESET_ALL}" in message
    assert f"{Fore.WHITE}select\n    *\nfrom source_table{Style.RESET_ALL}" in message
    assert f"{Fore.WHITE}detected table{Style.RESET_ALL}" in message


def test_negative_date_sentinels_are_cleaned_except_ongoing_end_year(conn):
    date_columns = conn.execute("""
        select
            table_name,
            column_name
        from information_schema.columns
        where
            table_name like 'source_%'
            and regexp_matches(column_name, '^(start|end)_(day|month|year)_[0-9]+$')
        order by table_name, column_name
        """).fetchall()

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


def test_source_resolved_start_dates_do_not_exceed_end_dates(conn):
    checks = [
        (
            "source_interstate_wars",
            """
            least(cow_date(start_year_1, start_month_1, start_day_1, 1, 1), cow_date(start_year_2, start_month_2, start_day_2, 1, 1))
            """,
            """
            greatest(cow_end_date(end_year_1, end_month_1, end_day_1), cow_end_date(end_year_2, end_month_2, end_day_2))
            """,
        ),
        (
            "source_interstate_war_dyads",
            """
            cow_date(start_year_1, start_month_1, start_day_1, 1, 1)
            """,
            """
            cow_end_date(end_year_1, end_month_1, end_day_1)
            """,
        ),
        (
            "source_interstate_mid_dyads",
            """
            cow_date(start_year_1, start_month_1, start_day_1, 1, 1)
            """,
            """
            cow_end_date(end_year_1, end_month_1, end_day_1)
            """,
        ),
        (
            "source_extrastate_wars",
            """
            least(cow_date(start_year_1, start_month_1, start_day_1, 1, 1), cow_date(start_year_2, start_month_2, start_day_2, 1, 1))
            """,
            """
            greatest(cow_end_date(end_year_1, end_month_1, end_day_1), cow_end_date(end_year_2, end_month_2, end_day_2))
            """,
        ),
        (
            "source_intrastate_wars",
            """
            least(cow_date(start_year_1, start_month_1, start_day_1, 1, 1), cow_date(start_year_2, start_month_2, start_day_2, 1, 1), cow_date(start_year_3, start_month_3, start_day_3, 1, 1), cow_date(start_year_4, start_month_4, start_day_4, 1, 1))
            """,
            """
            greatest(cow_end_date(end_year_1, end_month_1, end_day_1), cow_end_date(end_year_2, end_month_2, end_day_2), cow_end_date(end_year_3, end_month_3, end_day_3), cow_end_date(end_year_4, end_month_4, end_day_4))
            """,
        ),
    ]

    failures = []

    for table_name, start_date_expression, end_date_expression in checks:
        output_columns = non_date_column_csv(conn, table_name)
        flagged_rows_sql = f"""
        select
            {indent_sql(start_date_expression)} start_date,
            {indent_sql(end_date_expression)} end_date,
            {output_columns}
        from {table_name}
        where start_date > end_date
        """
        count_sql = f"""
        with

        flagged_rows as ({indent_sql(flagged_rows_sql)})

        select count(*)
        from flagged_rows
        """
        flagged_count = scalar(conn, count_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = "\n".join(
            [
                "with",
                "",
                "flagged_rows as (",
                "",
                clean_sql(flagged_rows_sql) + ")",
                "",
                "select",
                "    *",
                "from flagged_rows",
                "order by all",
                "limit 50",
            ]
        )
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
        fail_sql_check("Source rows where resolved start_date exceeds end_date:", failures=failures)


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
    select start_month_1
    from source_interstate_war_dyads
    where
        war_num = 106
        and dyindex = 257.03
        and c_code_a = 2
        and c_code_b = 300
    """
    assert scalar(conn, query) == 12

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


def test_source_battle_death_fields_are_not_null(conn):
    checks = [
        ("source_interstate_wars", "battle_deaths"),
        ("source_interstate_war_dyads", "battle_deaths_a"),
        ("source_interstate_war_dyads", "battle_deaths_b"),
        ("source_extrastate_wars", "battle_deaths_a"),
        ("source_extrastate_wars", "battle_deaths_b"),
        ("source_intrastate_wars", "battle_deaths_a"),
        ("source_intrastate_wars", "battle_deaths_b"),
    ]

    failures = []

    for table_name, column_name in checks:
        count_sql = f"""
        select count(*)
        from {table_name}
        where {column_name} is null
        """
        null_count = scalar(conn, count_sql)

        if null_count == 0:
            continue

        output_columns = non_date_column_csv(conn, table_name)
        detected_rows_sql = f"""
        select
            '{column_name}' column_name,
            {output_columns}
        from {table_name}
        where {column_name} is null
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
                summary=failure_summary(f"{table_name}.{column_name}", null_count),
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
        and source_file = 'dyadic_mid_4.02.csv'
        and source_version = '4.02'
    """
    assert scalar(conn, count_sql) == 1

    query = """
    select
        disno,
        war_num
    from source_interstate_war_dyads
    where disno in (3582, 3583, 3585, 4182, 4339)
    group by 1, 2
    order by 1, 2
    """
    actual_assignments = set(conn.execute(query).fetchall())

    assert actual_assignments == {(3582, 139), (3583, 139), (3585, 139), (4182, 4182), (4339, 905)}

    count_sql = """
    select count(*)
    from source_interstate_wars
    where
        war_num = 4182
        and war_name = 'Israeli–Hezbollah Conflict (South Lebanon)'
    """
    assert scalar(conn, count_sql) == 1

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


def test_dyad_battle_death_estimate_flags_are_binary(conn):
    for table_name in ["dyads_after_mid"]:
        count_sql = f"""
        select count(*)
        from {table_name}
        where
            battle_deaths_estimated_a not in (0, 1)
            or battle_deaths_estimated_b not in (0, 1)
            or battle_deaths_estimated_a is null
            or battle_deaths_estimated_b is null
        """
        assert scalar(conn, count_sql) == 0


def test_participant_battle_death_estimate_flags_are_binary(conn):
    for table_name in ["war_participants", "participants"]:
        count_sql = f"""
        select count(*)
        from {table_name}
        where
            battle_deaths_estimated not in (0, 1)
            or battle_deaths_estimated is null
        """
        assert scalar(conn, count_sql) == 0


def test_participants_have_side_assignments(conn):
    detected_rows_sql = """
    select *
    from participants
    where side is null
    order by war_num, c_code, participant
    """
    result = conn.execute(detected_rows_sql)
    rows = result.fetchall()
    columns = [column[0] for column in result.description]

    if rows:
        fail_sql_check(
            "Participants should have side assignments.",
            failures=[
                SqlCheckFailure(
                    label="participants with missing sides",
                    sql=detected_rows_sql,
                    summary=failure_summary("participants with missing sides", len(rows)),
                    detected_rows=format_query_results(columns, rows),
                )
            ],
        )


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


def test_final_participant_side_assignments_are_valid(conn):
    count_sql = """
    select count(*)
    from participants
    where side not in (1, 2, 3)
    """
    invalid_count = scalar(conn, count_sql)

    if invalid_count == 0:
        return

    detected_rows_sql = """
    select *
    from participants
    where side not in (1, 2, 3)
    order by war_num, c_code, participant
    limit 50
    """
    result = conn.execute(detected_rows_sql)
    rows = result.fetchall()
    columns = [column[0] for column in result.description]

    fail_sql_check(
        "Final participant side assignments should be in (1, 2, 3):",
        failures=[
            SqlCheckFailure(
                label="participants invalid side rows",
                sql=detected_rows_sql,
                summary=failure_summary("participants invalid side rows", invalid_count),
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
