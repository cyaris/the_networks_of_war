create or replace table war_participants as

with

cleaned_participant_rows as (

select
    a.war_num,
    a.war_name,
    a.war_type,
    c.war_type_name,
    c.war_subtype,
    a.c_code,
    clean_participant(coalesce(d.state_name, a.participant), e.replacement) participant,
    a.side,
    least(cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1), cow_date(a.start_year_2, a.start_month_2, a.start_day_2, 1, 1)) start_date,
    greatest(cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1), cow_end_date(a.end_year_2, a.end_month_2, a.end_day_2)) end_date,
    greatest(date_estimated(a.start_year_1, a.start_month_1, a.start_day_1), date_estimated(a.start_year_2, a.start_month_2, a.start_day_2)) start_date_estimated,
    greatest(date_estimated(a.end_year_1, a.end_month_1, a.end_day_1), date_estimated(a.end_year_2, a.end_month_2, a.end_day_2)) end_date_estimated,
    a.battle_deaths,
    0 battle_deaths_estimated
from source_interstate_wars a
left join war_types c on a.war_type = c.war_type
left join country_codes d on a.c_code = d.c_code
left join participant_name_replacements e on clean_text(coalesce(d.state_name, a.participant)) = e.source
union all
-- Interstate participants are sourced above; directed dyad rows carry dyad-level dates and deaths.
select
    a.war_num,
    a.war_name,
    a.war_type,
    a.war_type_name,
    a.war_subtype,
    a.c_code_a c_code,
    clean_participant(a.participant_a, b.replacement) participant,
    a.side_a side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_a battle_deaths,
    0 battle_deaths_estimated
from war_dyads a
left join participant_name_replacements b on clean_text(a.participant_a) = b.source
where
    a.war_type <> 1
    and a.participant_a is not null
union all
select
    a.war_num,
    a.war_name,
    a.war_type,
    a.war_type_name,
    a.war_subtype,
    a.c_code_b c_code,
    clean_participant(a.participant_b, b.replacement) participant,
    a.side_b side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_b battle_deaths,
    0 battle_deaths_estimated
from war_dyads a
left join participant_name_replacements b on clean_text(a.participant_b) = b.source
where
    a.war_type <> 1
    and a.participant_b is not null),

dyadic_side_rows as (

select
    war_num,
    c_code_a c_code,
    participant_a participant,
    side_a side
from war_dyads
where participant_a is not null
union all
select
    war_num,
    c_code_b c_code,
    participant_b participant,
    side_b side
from war_dyads
where participant_b is not null),

dyadic_side_assignments as (

select
    a.war_num,
    a.c_code,
    clean_participant(a.participant, b.replacement) participant,
    min(side) min_side,
    max(side) max_side
from dyadic_side_rows a
left join participant_name_replacements b on clean_text(a.participant) = b.source
group by 1, 2, 3)

select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(a.war_type_name) war_type_name,
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
left join dyadic_side_assignments b on a.war_num = b.war_num
                                    and a.c_code = b.c_code
                                    and a.participant = b.participant
where a.participant is not null
group by 1, 6, 7;
