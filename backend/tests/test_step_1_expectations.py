from __future__ import annotations

import json
import sys
from collections import Counter
from collections.abc import Callable
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
    PARTICIPANT_NAME_REPLACEMENTS_PATH,
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


CLEAN_PARTICIPANT_NESTED_REPLACEMENTS = (
    (" and ", " & "),
    (" rebels", " Rebels"),
    (" resistance", " Resistance"),
    (" resistence", " Resistance"),
    (" sultanate", " Sultanate"),
    (" tribe", " Tribe"),
)

CLEAN_PARTICIPANT_TEXT_ASSUMPTIONS = ((" Janissaries", "Janissaries"),)


def participant_name_replacements() -> tuple[tuple[str, str], ...]:
    rows = json.loads(PARTICIPANT_NAME_REPLACEMENTS_PATH.read_text())

    return tuple((row["source"], row["replacement"]) for row in rows)


def duplicate_values(values: list[str]) -> list[str]:
    counts = Counter(values)

    return sorted(value for value, count in counts.items() if count > 1)


def clean_text_python(value) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if text == "" or text in {"-7", "-8", "-9"}:
        return None

    return text


def apply_legacy_participant_replacements(value: str | None) -> str | None:
    text = clean_text_python(value)

    if text is None:
        return None

    for old, new in CLEAN_PARTICIPANT_NESTED_REPLACEMENTS:
        text = text.replace(old, new)

    return text


def participant_input_values(conn) -> set[str | None]:
    query = """
    select state_name input_value
    from source_country_codes
    union
    select coalesce(b.state_name, a.participant) input_value
    from source_interstate_wars a
    left join country_codes b on a.c_code = b.c_code
    union
    select participant_a input_value
    from war_dyads
    union
    select participant_b input_value
    from war_dyads
    union
    select participant input_value
    from source_participant_side_adjustments
    """
    return {input_value for (input_value,) in conn.execute(query).fetchall()}


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

RAW_SOURCE_DATE_DEFAULT_ENCODING = "latin-1"
# The pipeline prepares the cp1252 extra-state source as UTF-8 before DuckDB reads it.
RAW_SOURCE_DATE_ENCODING_OVERRIDES = {"extrastate_wars": None}


def raw_source_date_component_check_sql(
    source_key: str,
    source_file: str,
    source_path: Path,
    row_reference_columns: list[str],
    date_components: dict[str, list[str]],
    encoding: str | None = RAW_SOURCE_DATE_DEFAULT_ENCODING,
) -> str:
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


def test_source_tables_have_download_urls():
    missing = [metadata["key"] for metadata in SOURCE_METADATA if not metadata.get("downloads")]
    non_absolute_urls = [
        download["url"]
        for metadata in SOURCE_METADATA
        for download in metadata.get("downloads", [])
        if not download["url"].startswith(("http://", "https://"))
    ]

    assert missing == []
    assert non_absolute_urls == []


def test_json_config_items_have_unique_source_or_key_values():
    source_metadata_keys = [metadata["key"] for metadata in SOURCE_METADATA]
    participant_replacement_sources = [source for source, _ in participant_name_replacements()]

    assert duplicate_values(source_metadata_keys) == []
    assert duplicate_values(participant_replacement_sources) == []


def test_source_encoding_overrides_are_defined_in_metadata():
    encodings = {metadata["key"]: metadata["encoding"] for metadata in SOURCE_METADATA if metadata.get("encoding")}

    assert encodings == {"extrastate_wars": "cp1252"}


@dataclass(frozen=True)
class SqlCheckFailure:
    label: str
    sql: str
    summary: str
    detected_rows: str


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[tuple]


ProblemCell = tuple[int, str]
ProblemCellPredicate = Callable[[str, object], bool]


def problem_cell_style(text: str) -> str:
    return f"{Style.BRIGHT}{Fore.RED}{text}{Style.RESET_ALL}"


def highlighted_detected_rows(result: QueryResult, problem_cells: set[ProblemCell] | None = None) -> str:
    problem_cells = problem_cells or set()

    def style_problem_cell(row_index: int, column: str, value: object, text: str) -> str:
        if (row_index, column) in problem_cells:
            return problem_cell_style(text)

        return text

    return format_query_results(result.columns, result.rows, null_text="null", cell_style=style_problem_cell)


def problem_cells_matching(result: QueryResult, predicate: ProblemCellPredicate) -> set[ProblemCell]:
    return {
        (row_index, column)
        for row_index, row in enumerate(result.rows)
        for column, value in zip(result.columns, row)
        if predicate(column, value)
    }


def problem_cells_for_columns(result: QueryResult, problem_columns: set[str]) -> set[ProblemCell]:
    return problem_cells_matching(result, lambda column, value: column in problem_columns)


def null_problem_cells(result: QueryResult, nullable_columns: set[str]) -> set[ProblemCell]:
    return problem_cells_matching(result, lambda column, value: column in nullable_columns and value is None)


def query_result(conn, sql: str) -> QueryResult:
    result = conn.execute(sql)
    columns = [column[0] for column in result.description] if result.description else []
    rows = result.fetchall() if columns else []

    return QueryResult(columns, rows)


def flagged_row_count(conn, flagged_rows_sql: str) -> int:
    return conn.sql(flagged_rows_sql).aggregate("count(*)").fetchone()[0]


def flagged_rows_query(flagged_rows_sql: str, order_by: str) -> str:
    return f"""
    {flagged_rows_sql}
    order by {order_by}
    limit 50
    """


def sql_check_failure(
    label: str, sql: str, row_count: int, result: QueryResult, problem_cells: set[ProblemCell]
) -> SqlCheckFailure:
    return SqlCheckFailure(
        label=label,
        sql=sql,
        summary=failure_summary(label, row_count),
        detected_rows=highlighted_detected_rows(result, problem_cells),
    )


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
    expected_replacements = {
        (source, replacement)
        for source, replacement in participant_name_replacements()
        if apply_legacy_participant_replacements(source) == replacement
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


def test_raw_source_date_components_use_valid_domains(conn):
    pipeline = Pipeline(csv_dir=DEFAULT_CSV_DIR)
    failures = []

    for source_key, row_reference_columns, date_components in RAW_SOURCE_DATE_COMPONENTS:
        source_file = Path(SOURCE_FILES[source_key]).name
        source_path = pipeline.prepared_path_for(source_key)
        flagged_rows_sql = raw_source_date_component_check_sql(
            source_key,
            source_file,
            source_path,
            row_reference_columns,
            date_components,
            RAW_SOURCE_DATE_ENCODING_OVERRIDES.get(source_key, RAW_SOURCE_DATE_DEFAULT_ENCODING),
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
        missing_war_type_sql = "a.war_type is null or" if require_war_type else ""
        flagged_rows_sql = f"""
        select a.*
        from {table_name} a
        left join war_types b on a.war_type = b.war_type
        where
            {missing_war_type_sql}
            (a.war_type is not null and b.war_type is null)
        """
        flagged_count = flagged_row_count(conn, flagged_rows_sql)

        if flagged_count == 0:
            continue

        detected_rows_sql = flagged_rows_query(flagged_rows_sql, "all")
        detected_rows = query_result(conn, detected_rows_sql)
        problem_cells = problem_cells_for_columns(detected_rows, {"war_type"})
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


def test_source_adjusted_mid_war_number_relationships_are_applied(conn):
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
        a.war_num
    from source_interstate_mid_war_num_adjustments a
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
        a.war_type
    from source_interstate_war_metadata_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    where a.war_num = 4182
    """
    actual_war_metadata = conn.execute(query).fetchone()

    assert actual_war_metadata == ("interstate_mid_dyads", "4.03", "Israeli–Hezbollah Conflict (South Lebanon)", 1)

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

    query = """
    select
        war_name,
        war_type,
        total_participants,
        total_dyads
    from wars
    where war_num = 4182
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
    where war_num = 4182
    order by 1, 2
    """
    actual_sides = set(conn.execute(query).fetchall())

    assert actual_sides == {(660, "Lebanon", 1), (666, "Israel", 2)}


def test_source_adjustment_inserts_do_not_duplicate_existing_source_facts(conn):
    query = """
    select count(*) duplicate_count
    from source_interstate_mid_war_num_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    join source_interstate_war_dyads c on a.disno = c.disno
                                       and a.war_num = c.war_num
    union all
    select count(*) duplicate_count
    from source_interstate_war_metadata_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    join source_interstate_wars c on a.war_num = c.war_num
                                  and a.war_name = c.war_name
                                  and a.war_type = c.war_type
    union all
    select count(*) duplicate_count
    from source_participant_side_adjustments a
    join source_file_versions b on a.source_key = b.source_key
                                and a.source_version = b.source_version
    join source_interstate_wars c on a.war_num = c.war_num
                                  and a.c_code = c.c_code
    left join participant_name_replacements d on clean_text(a.participant) = d.source
    left join participant_name_replacements e on clean_text(c.participant) = e.source
    where
        clean_participant(a.participant, d.replacement) = clean_participant(c.participant, e.replacement)
        and a.side = c.side
    """
    duplicate_count = sum(row[0] for row in conn.execute(query).fetchall())

    assert duplicate_count == 0


def test_source_intrastate_war_data_entry_fixes_are_applied(conn):
    assert scalar(conn, "select count(*) from source_intrastate_wars where war_num = 977") == 0

    query = """
    select start_year_1
    from source_intrastate_wars
    where war_num = 976
    """
    actual_start_years = {row[0] for row in conn.execute(query).fetchall()}

    assert actual_start_years == {2011}

    query = """
    select
        war_num,
        end_year_1
    from source_intrastate_wars
    where war_num in (942, 990.4, 991, 991.4, 992.5)
    order by 1
    """
    actual_end_years = set(conn.execute(query).fetchall())

    assert actual_end_years == {(942, -7), (990.4, -7), (991, -7), (991.4, -7), (992.5, -7)}


def test_source_wars_named_present_or_ongoing_are_marked_ongoing(conn):
    detected_rows_sql = """
    select
        'source_interstate_wars' source_table,
        war_num,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) ongoing_war
    from source_interstate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2
    having max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) = 0
    union all
    select
        'source_extrastate_wars' source_table,
        war_num,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) ongoing_war
    from source_extrastate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2
    having max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2))) = 0
    union all
    select
        'source_intrastate_wars' source_table,
        war_num,
        any_value(war_name) war_name,
        max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2), ongoing_war(end_year_3), ongoing_war(end_year_4))) ongoing_war
    from source_intrastate_wars
    where regexp_matches(lower(war_name), '(^|[^a-z])(present|ongoing)([^a-z]|$)')
    group by 1, 2
    having max(greatest(ongoing_war(end_year_1), ongoing_war(end_year_2), ongoing_war(end_year_3), ongoing_war(end_year_4))) = 0
    order by source_table, war_num
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
        and side_b is not null)

    select
        war_num,
        c_code,
        participant,
        list(distinct side order by side) sides,
        count(*) row_count
    from side_rows
    group by 1, 2, 3
    having count(distinct side) > 1
    order by war_num, c_code, participant
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
    order by war_num, c_code, participant
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

    query = """
    select
        war_num,
        c_code_a,
        participant_a,
        c_code_b,
        participant_b
    from dyads
    group by 1, 2, 3, 4, 5
    having count(*) > 1
    order by 1, 2, 3, 4, 5
    """
    assert conn.execute(query).fetchall() == []

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
