create or replace macro clean_int(value) as (
    try_cast(nullif(trim(value::varchar), '') as integer)
);

create or replace macro clean_number(value) as (
    try_cast(nullif(trim(value::varchar), '') as double)
);

create or replace macro clean_text(value) as (
    if(nullif(trim(value::varchar), '') in ('-7', '-8', '-9'), null, nullif(trim(value::varchar), ''))
);

create or replace macro clean_participant(value) as (
    case
        when clean_text(value) = 'United States' then 'United States of America'
        when clean_text(value) = 'Baron von Ungern-Sternbergs White army' then 'Baron von Ungern-Sternberg''s White army'
        when clean_text(value) = ' Janissaries' then 'Janissaries'
        when clean_text(value) = 'Turkey/Ottoman Empire/Egypt' then 'Turkey, Ottoman Empire & Egypt'
        else replace(
            replace(
                replace(
                    replace(
                        replace(
                            replace(clean_text(value), ' and ', ' & '),
                            ' rebels',
                            ' Rebels'
                        ),
                        ' resistance',
                        ' Resistance'
                    ),
                    ' resistence',
                    ' Resistance'
                ),
                ' sultanate',
                ' Sultanate'
            ),
            ' tribe',
            ' Tribe'
        )
    end
);

create or replace macro cow_date(year_value, month_value, day_value, default_month, default_day) as (
    if(clean_int(year_value) > 0, make_date(clean_int(year_value), if(clean_int(month_value) between 1 and 12, clean_int(month_value), default_month), least(if(clean_int(day_value) between 1 and 31, clean_int(day_value), default_day), extract(day from last_day(make_date(clean_int(year_value), if(clean_int(month_value) between 1 and 12, clean_int(month_value), default_month), 1)))::integer)), null)
);

create or replace macro cow_end_date(year_value, month_value, day_value) as (
    if(clean_int(year_value) = -7, make_date(extract(year from current_date)::integer, 12, 31), cow_date(year_value, month_value, day_value, 12, 31))
);

create or replace macro date_estimated(year_value, month_value, day_value) as (
    if(clean_int(year_value) = -7 or (clean_int(year_value) > 0 and (clean_int(month_value) = -9 or clean_int(day_value) = -9)), 1, 0)
);

create or replace macro ongoing_war(year_value) as (
    if(clean_int(year_value) = -7, 1, 0)
);
