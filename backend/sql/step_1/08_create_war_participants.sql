create or replace table war_participants as

with

cleaned_participant_rows as (

select
    a.war_id,
    a.war_name,
    a.war_type_id,
    c.war_type,
    c.war_subtype,
    a.c_code,
    coalesce(d.state_name, clean_participant(a.participant, e.replacement)) participant,
    a.side,
    least(cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1), cow_date(a.start_year_2, a.start_month_2, a.start_day_2, 1, 1)) start_date,
    greatest(cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1), cow_end_date(a.end_year_2, a.end_month_2, a.end_day_2)) end_date,
    greatest(date_estimated(a.start_year_1, a.start_month_1, a.start_day_1), date_estimated(a.start_year_2, a.start_month_2, a.start_day_2)) start_date_estimated,
    greatest(date_estimated(a.end_year_1, a.end_month_1, a.end_day_1), date_estimated(a.end_year_2, a.end_month_2, a.end_day_2)) end_date_estimated,
    a.battle_deaths,
    0 battle_deaths_estimated
from source_interstate_wars a
left join war_types c on a.war_type_id = c.war_type_id
left join country_codes d on a.c_code = d.c_code
left join participant_name_replacements e on d.c_code is null
                                          and clean_text(a.participant) = e.source
union all
select
    a.war_id,
    a.war_name,
    a.war_type_id,
    a.war_type,
    a.war_subtype,
    a.c_code_a c_code,
    coalesce(b.state_name, clean_participant(a.participant_a, c.replacement)) participant,
    a.side_a side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_a battle_deaths,
    0 battle_deaths_estimated
from war_dyads a
left join country_codes b on a.c_code_a = b.c_code
left join participant_name_replacements c on b.c_code is null
                                          and clean_text(a.participant_a) = c.source
where
    a.war_type_id <> 1
    and a.participant_a is not null
union all
select
    a.war_id,
    a.war_name,
    a.war_type_id,
    a.war_type,
    a.war_subtype,
    a.c_code_b c_code,
    coalesce(b.state_name, clean_participant(a.participant_b, c.replacement)) participant,
    a.side_b side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_b battle_deaths,
    0 battle_deaths_estimated
from war_dyads a
left join country_codes b on a.c_code_b = b.c_code
left join participant_name_replacements c on b.c_code is null
                                          and clean_text(a.participant_b) = c.source
where
    a.war_type_id <> 1
    and a.participant_b is not null),

dyadic_side_rows as (

select
    war_id,
    c_code_a c_code,
    participant_a participant,
    side_a side
from war_dyads
where participant_a is not null
union all
select
    war_id,
    c_code_b c_code,
    participant_b participant,
    side_b side
from war_dyads
where participant_b is not null),

dyadic_side_assignments as (

select
    a.war_id,
    a.c_code,
    coalesce(b.state_name, clean_participant(a.participant, c.replacement)) participant,
    min(side) min_side,
    max(side) max_side
from dyadic_side_rows a
left join country_codes b on a.c_code = b.c_code
left join participant_name_replacements c on b.c_code is null
                                          and clean_text(a.participant) = c.source
group by 1, 2, 3)

select
    a.war_id,
    any_value(a.war_name) war_name,
    any_value(a.war_type_id) war_type_id,
    any_value(a.war_type) war_type,
    any_value(a.war_subtype) war_subtype,
    a.c_code,
    a.participant,
    case
        when max(b.min_side) = 1 and max(b.max_side) = 2 then 3
        when min(a.side) = 1 and max(a.side) = 2 then 3
        else max(a.side)
    end side,
    min(a.start_date) start_date,
    max(a.end_date) end_date,
    max(a.start_date_estimated) start_date_estimated,
    max(a.end_date_estimated) end_date_estimated,
    sum(if(a.battle_deaths >= 0, a.battle_deaths, null)) battle_deaths,
    max(a.battle_deaths_estimated) battle_deaths_estimated
from cleaned_participant_rows a
left join dyadic_side_assignments b on a.war_id = b.war_id
                                    and a.c_code = b.c_code
                                    and a.participant = b.participant
where a.participant is not null
group by 1, 6, 7;
