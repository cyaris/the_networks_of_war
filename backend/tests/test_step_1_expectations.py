from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = BACKEND_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import DEFAULT_CSV_DIR, SOURCE_FILES, Pipeline, format_query_results  # noqa: E402


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

    unexpected = []
    flagged_row_outputs = []

    for table_name, start_date_expression, end_date_expression in checks:
        flagged_rows_sql = f"""
            select
                {start_date_expression} start_date,
                {end_date_expression} end_date,
                *
            from {table_name}
            where start_date > end_date
            """
        flagged_count = scalar(
            conn,
            f"""
            select count(*)
            from ({flagged_rows_sql})
            """,
        )

        if flagged_count == 0:
            continue

        unexpected.append((table_name, flagged_count))
        result = conn.execute(f"""
            select
                '{table_name}' table_name,
                *
            from ({flagged_rows_sql})
            order by all
            limit 50
            """)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        flagged_row_outputs.append(format_query_results(columns, rows))

    assert unexpected == [], "\n".join(
        [
            "\nSource rows where resolved start_date exceeds end_date:",
            format_query_results(["table_name", "row_count"], unexpected),
            "\nFlagged rows:",
            "\n\n".join(flagged_row_outputs),
        ]
    )


def test_calculated_and_transient_source_columns_are_not_materialized(conn):
    assert {"batdths", "durindx", "battle_deaths_total"}.isdisjoint(column_names(conn, "source_interstate_war_dyads"))
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


def test_source_transition_war_references_are_positive_or_null(conn):
    checks = [
        "source_interstate_wars",
        "source_extrastate_wars",
        "source_intrastate_wars",
    ]

    unexpected = []
    flagged_row_outputs = []

    for table_name in checks:
        flagged_count = scalar(
            conn,
            f"""
            select count(*)
            from {table_name}
            where
                lagging_war < 0
                or leading_war < 0
            """,
        )

        if flagged_count == 0:
            continue

        unexpected.append((table_name, flagged_count))
        result = conn.execute(f"""
            select
                '{table_name}' table_name,
                *
            from {table_name}
            where
                lagging_war < 0
                or leading_war < 0
            order by all
            limit 50
            """)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        flagged_row_outputs.append(format_query_results(columns, rows))

    assert unexpected == [], "\n".join(
        [
            "\nSource transition war references should be positive war numbers or null:",
            format_query_results(["table_name", "row_count"], unexpected),
            "\nFlagged rows:",
            "\n\n".join(flagged_row_outputs),
        ]
    )


def test_source_interstate_war_dyad_data_entry_fixes_are_applied(conn):
    assert (
        scalar(
            conn,
            """
            select start_month_1
            from source_interstate_war_dyads
            where
                war_num = 106
                and dyindex = 257.03
                and c_code_a = 2
                and c_code_b = 300
            """,
        )
        == 12
    )
    assert (
        scalar(
            conn,
            """
            select end_year_1
            from source_interstate_war_dyads
            where
                war_num = 106
                and dyindex = 257.24
                and c_code_a = 325
                and c_code_b = 355
                and source_year = 1916
            """,
        )
        == 1918
    )
    assert (
        scalar(
            conn,
            """
            select battle_deaths_a
            from source_interstate_war_dyads
            where
                war_num = 139
                and dyindex = 1694.001
                and c_code_a = 800
                and c_code_b = 710
            """,
        )
        == 5569
    )


def test_source_interstate_mid_fatality_levels_are_converted_to_estimates(conn):
    assert (
        scalar(
            conn,
            """
            select count(*)
            from source_interstate_mid_dyads
            where
                coalesce(battle_deaths_estimated_a, -1) not in (-1, 0, 25, 100, 250, 500, 999, 1000)
                or coalesce(battle_deaths_estimated_b, -1) not in (-1, 0, 25, 100, 250, 500, 999, 1000)
            """,
        )
        == 0
    )

    actual_estimates = {row[0] for row in conn.execute("""
            select battle_deaths_estimated_a battle_deaths_estimated
            from source_interstate_mid_dyads
            where battle_deaths_estimated_a is not null
            group by 1
            union
            select battle_deaths_estimated_b battle_deaths_estimated
            from source_interstate_mid_dyads
            where battle_deaths_estimated_b is not null
            group by 1
        """).fetchall()}

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

    unexpected = []
    flagged_row_outputs = []

    for table_name, column_name in checks:
        null_count = scalar(
            conn,
            f"""
            select count(*)
            from {table_name}
            where {column_name} is null
            """,
        )

        if null_count == 0:
            continue

        unexpected.append((table_name, column_name, null_count))
        result = conn.execute(f"""
            select
                '{table_name}' table_name,
                '{column_name}' column_name,
                *
            from {table_name}
            where {column_name} is null
            order by all
            limit 50
            """)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        flagged_row_outputs.append(format_query_results(columns, rows))

    assert unexpected == [], "\n".join(
        [
            "\nNull battle-death source fields:",
            format_query_results(["table_name", "column_name", "null_count"], unexpected),
            "\nFlagged rows:",
            "\n\n".join(flagged_row_outputs),
        ]
    )


def test_source_adjusted_mid_war_number_relationships_are_applied(conn):
    assert (
        scalar(
            conn,
            """
            select count(*)
            from source_file_versions
            where
                source_key = 'interstate_mid_dyads'
                and source_file = 'dyadic_mid_4.02.csv'
                and source_version = '4.02'
            """,
        )
        == 1
    )

    actual_assignments = set(conn.execute("""
        select
            disno,
            war_num
        from source_interstate_war_dyads
        where disno in (3582, 3583, 3585, 4182, 4339)
        group by 1, 2
        order by 1, 2
        """).fetchall())

    assert actual_assignments == {
        (3582, 139),
        (3583, 139),
        (3585, 139),
        (4182, 4182),
        (4339, 905),
    }

    assert (
        scalar(
            conn,
            """
            select count(*)
            from source_interstate_wars
            where
                war_num = 4182
                and war_name = 'Israeli–Hezbollah Conflict (South Lebanon)'
            """,
        )
        == 1
    )


def test_source_intrastate_war_data_entry_fixes_are_applied(conn):
    assert scalar(conn, "select count(*) from source_intrastate_wars where war_num = 977") == 0
    assert (
        scalar(
            conn,
            """
            select count(*)
            from source_intrastate_wars
            where
                war_num = 976
                and start_year_1 != 2011
            """,
        )
        == 0
    )
    assert (
        scalar(
            conn,
            """
            select count(*)
            from source_intrastate_wars
            where
                war_num in (942, 990.4, 991, 991.4, 992.5)
                and end_year_1 != -7
            """,
        )
        == 0
    )


def test_mid_dyads_do_not_duplicate_source_dyad_overlaps(conn):
    assert (
        scalar(
            conn,
            """
            select count(*)
            from dyads_after_mid a
            join dyads_after_sources b on a.war_num = b.war_num
                                      and a.c_code_a = b.c_code_a
                                      and a.c_code_b = b.c_code_b
                                      and least(a.end_date, b.end_date) >= greatest(a.start_date, b.start_date)
            where
                a.battle_deaths_estimated_a = 1
                or a.battle_deaths_estimated_b = 1
            """,
        )
        == 0
    )


def test_dyad_battle_death_estimate_flags_are_binary(conn):
    for table_name in ["dyads_after_mid"]:
        assert (
            scalar(
                conn,
                f"""
                select count(*)
                from {table_name}
                where
                    battle_deaths_estimated_a not in (0, 1)
                    or battle_deaths_estimated_b not in (0, 1)
                    or battle_deaths_estimated_a is null
                    or battle_deaths_estimated_b is null
                """,
            )
            == 0
        )


def test_participant_battle_death_estimate_flags_are_binary(conn):
    for table_name in ["war_participants", "participants"]:
        assert (
            scalar(
                conn,
                f"""
                select count(*)
                from {table_name}
                where
                    battle_deaths_estimated not in (0, 1)
                    or battle_deaths_estimated is null
                """,
            )
            == 0
        )


def test_participants_have_side_assignments(conn):
    result = conn.execute("""
        select *
        from participants
        where side is null
        order by war_num, c_code, participant
        """)
    rows = result.fetchall()
    columns = [column[0] for column in result.description]

    assert rows == [], "\n" + format_query_results(columns, rows)


def test_mid_dyads_resolve_all_mid_war_numbers(conn):
    assert (
        scalar(
            conn,
            """
            select count(*)
            from dyads_after_mid
            where war_num = -1
            """,
        )
        == 0
    )

    actual_dyads = set(conn.execute("""
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
        """).fetchall())

    assert actual_dyads == {
        (4182, "Israeli–Hezbollah Conflict (South Lebanon)", 660, 666, "Lebanon", "Israel"),
        (4182, "Israeli–Hezbollah Conflict (South Lebanon)", 666, 660, "Israel", "Lebanon"),
    }


def test_dyads_apply_final_transformation_assumptions(conn):
    assert (
        scalar(
            conn,
            """
            select count(*)
            from dyads
            where
                participant_a is null
                or participant_b is null
                or participant_a = '-8'
                or participant_b = '-8'
            """,
        )
        == 0
    )
    assert (
        scalar(
            conn,
            """
            select count(*)
            from dyad_years
            where
                year < extract(year from start_date)::integer
                or year > extract(year from end_date)::integer
            """,
        )
        == 0
    )
    assert (
        scalar(
            conn,
            """
            select count(*)
            from dyads
            where
                c_code_a = c_code_b
                and participant_a = participant_b
            """,
        )
        == 0
    )


def test_dyads_retain_named_non_state_anchor_dyads(conn):
    actual_dyads = set(conn.execute("""
        select
            participant_a,
            participant_b
        from dyads
        where
            war_num = 940.8
            and participant_a in ('ICU', 'Eritrea')
        group by 1, 2
        order by 1, 2
        """).fetchall())

    expected_side_1_participants = {"United States of America", "Uganda", "Kenya", "Burundi", "Somalia", "Ethiopia"}

    assert actual_dyads == {
        (anchor, participant) for anchor in {"ICU", "Eritrea"} for participant in expected_side_1_participants
    }
