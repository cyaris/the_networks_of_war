create or replace macro clean_int(value) as (
    try_cast(nullif(trim(value::varchar), '') as integer)
);

create or replace macro clean_number(value) as (
    try_cast(nullif(trim(value::varchar), '') as double)
);

create or replace macro clean_text(value) as (
    if(nullif(trim(value::varchar), '') in ('-7', '-8', '-9'), null, nullif(trim(value::varchar), ''))
);

create or replace macro clean_date_month(value) as (
    if(clean_int(value) between 1 and 12, clean_int(value), null)
);

create or replace macro clean_date_day(value) as (
    if(clean_int(value) between 1 and 31, clean_int(value), null)
);

create or replace macro clean_date_year(value) as (
    if(clean_int(value) > 0, clean_int(value), null)
);

create or replace macro clean_end_year(value) as (
    case
        when clean_int(value) = -7 then -7
        when clean_int(value) > 0 then clean_int(value)
        else null
    end
);

create or replace macro clean_war_reference(value) as (
    if(clean_int(value) > 0, clean_int(value), null)
);

create or replace table participant_name_replacements as

select
    clean_text("source") "source",
    clean_text("replacement") "replacement"
from read_json_auto({participant_name_replacements_path})
where
    clean_text("source") is not null
    and clean_text("replacement") is not null;

create or replace macro clean_participant(value, replacement) as (
    coalesce(replacement, clean_text(value))
);

create or replace macro cow_date(year_value, month_value, day_value, default_month, default_day) as (
    if(
        clean_int(year_value) > 0,
        make_date(
            clean_int(year_value),
            coalesce(clean_date_month(month_value), default_month),
            least(coalesce(clean_date_day(day_value), default_day), extract(day from last_day(make_date(clean_int(year_value), coalesce(clean_date_month(month_value), default_month), 1)))::integer)
        ),
        null
    )
);

create or replace macro cow_end_date(year_value, month_value, day_value) as (
    if(clean_int(year_value) = -7, make_date(extract(year from current_date)::integer, 12, 31), cow_date(year_value, month_value, day_value, 12, 31))
);

create or replace macro date_estimated(year_value, month_value, day_value) as (
    if(clean_int(year_value) = -7 or (clean_int(year_value) > 0 and (clean_date_month(month_value) is null or clean_date_day(day_value) is null)), 1, 0)
);

create or replace macro ongoing_war(year_value) as (
    if(clean_int(year_value) = -7, 1, 0)
);
