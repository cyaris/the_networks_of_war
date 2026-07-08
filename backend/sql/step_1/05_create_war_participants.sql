create or replace table war_participants as

with

cleaned_participant_rows as (

select
    'Inter-StateWarData_v4.0.csv' source_file,
    a.war_num,
    a.war_name,
    a.war_type,
    c.war_type_name,
    c.war_subtype,
    a.c_code,
    clean_participant(coalesce(d.state_name, a.participant)) participant,
    a.side,
    least(cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1), cow_date(a.start_year_2, a.start_month_2, a.start_day_2, 1, 1)) start_date,
    extract(year from least(cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1), cow_date(a.start_year_2, a.start_month_2, a.start_day_2, 1, 1)))::integer start_year,
    greatest(cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1), cow_end_date(a.end_year_2, a.end_month_2, a.end_day_2)) end_date,
    extract(year from greatest(cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1), cow_end_date(a.end_year_2, a.end_month_2, a.end_day_2)))::integer end_year,
    greatest(date_estimated(a.start_year_1, a.start_month_1, a.start_day_1), date_estimated(a.start_year_2, a.start_month_2, a.start_day_2)) start_date_estimated,
    greatest(date_estimated(a.end_year_1, a.end_month_1, a.end_day_1), date_estimated(a.end_year_2, a.end_month_2, a.end_day_2)) end_date_estimated,
    greatest(ongoing_war(a.end_year_1), ongoing_war(a.end_year_2)) ongoing_war,
    a.battle_deaths
from source_interstate_wars a
left join war_types c on a.war_type = c.war_type
left join country_codes d on a.c_code = d.c_code
union all
select
    source_file,
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code_a c_code,
    clean_participant(participant_a) participant,
    side_a side,
    start_date,
    start_year,
    end_date,
    end_year,
    start_date_estimated,
    end_date_estimated,
    ongoing_war,
    battle_deaths_a battle_deaths
from war_dyads
where
    source_file in ('Extra-StateWarData_v4.0.csv', 'INTRA-STATE_State_participants v5.1.csv')
    and participant_a is not null
union all
select
    source_file,
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code_b c_code,
    clean_participant(participant_b) participant,
    side_b side,
    start_date,
    start_year,
    end_date,
    end_year,
    start_date_estimated,
    end_date_estimated,
    ongoing_war,
    battle_deaths_b battle_deaths
from war_dyads
where
    source_file in ('Extra-StateWarData_v4.0.csv', 'INTRA-STATE_State_participants v5.1.csv')
    and participant_b is not null),

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
    war_num,
    c_code,
    clean_participant(participant) participant,
    min(side) min_side,
    max(side) max_side
from dyadic_side_rows
group by 1, 2, 3)

select
    string_agg(distinct a.source_file, ', ') source_file,
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
    min(a.start_year) start_year,
    max(a.end_date) end_date,
    max(a.end_year) end_year,
    min(a.start_date_estimated) start_date_estimated,
    max(a.end_date_estimated) end_date_estimated,
    max(a.ongoing_war) ongoing_war,
    sum(a.battle_deaths) battle_deaths
from cleaned_participant_rows a
left join dyadic_side_assignments b on a.war_num = b.war_num
                                    and a.c_code = b.c_code
                                    and a.participant = b.participant
where a.participant is not null
group by 2, 7, 8;
