from __future__ import annotations

import json
import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest
from colorama import Fore, Style, init

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = BACKEND_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

from pipeline import PARTICIPANT_NAME_REPLACEMENTS_PATH, format_query_results, sql_identifier, sql_literal  # noqa: E402

init(strip=False)

CLEAN_PARTICIPANT_NESTED_REPLACEMENTS = (
    (" and ", " & "),
    (" rebels", " Rebels"),
    (" resistance", " Resistance"),
    (" resistence", " Resistance"),
    (" sultanate", " Sultanate"),
    (" tribe", " Tribe"),
)

CLEAN_PARTICIPANT_TEXT_ASSUMPTIONS = ((" Janissaries", "Janissaries"),)

RAW_SOURCE_DATE_DEFAULT_ENCODING = "latin-1"
# The pipeline prepares the cp1252 extra-state source as UTF-8 before DuckDB reads it.
RAW_SOURCE_DATE_ENCODING_OVERRIDES = {"extrastate_wars": None}


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
