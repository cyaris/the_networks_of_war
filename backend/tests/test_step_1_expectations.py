from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = BACKEND_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import DEFAULT_CSV_DIR, SOURCE_FILES, Pipeline  # noqa: E402


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

    unexpected = [row for row in conn.execute(" union all ".join(checks)).fetchall() if row[2] != 0]

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
    ],
)
def test_date_macros_capture_step_1_date_assumptions(conn, expression, expected):
    actual = scalar(conn, f"select {expression}")

    if isinstance(expected, str):
        actual = str(actual)

    assert actual == expected


def test_calculated_and_transient_source_columns_are_not_materialized(conn):
    assert {"batdths", "durindx", "battle_deaths_total"}.isdisjoint(column_names(conn, "source_interstate_war_dyads"))
    assert {"durindx", "duration", "cumdurat"}.isdisjoint(column_names(conn, "source_interstate_mid_dyads"))
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
    }
    for table_name in ["war_dyads", "war_participants", "dyads_after_mid", "initial_participants", "initial_dyads"]:
        assert transient_columns.isdisjoint(column_names(conn, table_name))


def test_initial_dyads_apply_final_transformation_assumptions(conn):
    assert (
        scalar(
            conn,
            """
            select count(*)
            from initial_dyads
            where
                c_code_a = -8
                or c_code_b = -8
            """,
        )
        == 0
    )
    assert (
        scalar(
            conn,
            """
            select count(*)
            from initial_dyads
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
            from initial_dyads
            where
                c_code_a = c_code_b
                and participant_a = participant_b
            """,
        )
        == 0
    )
