create or replace table war_dyads as

with

interstate_wars as (

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type
from source_interstate_wars
group by 1),

extra_dates as (

select
    *,
    least(cow_date(start_year_1, start_month_1, start_day_1, 1, 1), cow_date(start_year_2, start_month_2, start_day_2, 1, 1)) start_date,
    greatest(cow_end_date(end_year_1, end_month_1, end_day_1), cow_end_date(end_year_2, end_month_2, end_day_2)) end_date,
    greatest(date_estimated(start_year_1, start_month_1, start_day_1), date_estimated(start_year_2, start_month_2, start_day_2)) start_date_estimated,
    greatest(date_estimated(end_year_1, end_month_1, end_day_1), date_estimated(end_year_2, end_month_2, end_day_2)) end_date_estimated,
    greatest(ongoing_war(end_year_1), ongoing_war(end_year_2)) ongoing_war
from source_extrastate_wars),

intra_dates as (

select
    *,
    least(cow_date(start_year_1, start_month_1, start_day_1, 1, 1), cow_date(start_year_2, start_month_2, start_day_2, 1, 1), cow_date(start_year_3, start_month_3, start_day_3, 1, 1), cow_date(start_year_4, start_month_4, start_day_4, 1, 1)) start_date,
    greatest(cow_end_date(end_year_1, end_month_1, end_day_1), cow_end_date(end_year_2, end_month_2, end_day_2), cow_end_date(end_year_3, end_month_3, end_day_3), cow_end_date(end_year_4, end_month_4, end_day_4)) end_date,
    greatest(date_estimated(start_year_1, start_month_1, start_day_1), date_estimated(start_year_2, start_month_2, start_day_2), date_estimated(start_year_3, start_month_3, start_day_3), date_estimated(start_year_4, start_month_4, start_day_4)) start_date_estimated,
    greatest(date_estimated(end_year_1, end_month_1, end_day_1), date_estimated(end_year_2, end_month_2, end_day_2), date_estimated(end_year_3, end_month_3, end_day_3), date_estimated(end_year_4, end_month_4, end_day_4)) end_date_estimated,
    greatest(ongoing_war(end_year_1), ongoing_war(end_year_2), ongoing_war(end_year_3), ongoing_war(end_year_4)) ongoing_war
from source_intrastate_wars)

select
    'directed_dyadic_war.csv' source_file,
    a.war_num,
    b.war_name,
    b.war_type,
    c.war_type_name,
    c.war_subtype,
    a.disno,
    a.dyindex,
    a.c_code_a,
    a.c_code_b,
    d.state_name participant_a,
    e.state_name participant_b,
    a.side_a,
    a.side_b,
    cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1) start_date,
    a.start_year_1 start_year,
    cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1) end_date,
    extract(year from cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1))::integer end_year,
    date_estimated(a.start_year_1, a.start_month_1, a.start_day_1) start_date_estimated,
    date_estimated(a.end_year_1, a.end_month_1, a.end_day_1) end_date_estimated,
    ongoing_war(a.end_year_1) ongoing_war,
    a.battle_deaths_a,
    a.battle_deaths_b,
    a.battle_deaths_total,
    a.outcome_a,
    null::integer outcome_b
from source_directed_dyadic_war a
left join interstate_wars b on a.war_num = b.war_num
left join war_types c on b.war_type = c.war_type
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code
union all
select
    'Extra-StateWarData_v4.0.csv' source_file,
    a.war_num,
    a.war_name,
    a.war_type,
    c.war_type_name,
    c.war_subtype,
    null::double disno,
    null::double dyindex,
    a.c_code_a,
    a.c_code_b,
    coalesce(d.state_name, a.participant_a) participant_a,
    coalesce(e.state_name, a.participant_b) participant_b,
    1 side_a,
    2 side_b,
    a.start_date,
    extract(year from a.start_date)::integer start_year,
    a.end_date,
    extract(year from a.end_date)::integer end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war,
    a.battle_deaths_a,
    a.battle_deaths_b,
    a.battle_deaths_a + a.battle_deaths_b battle_deaths_total,
    a.outcome_a,
    null::integer outcome_b
from extra_dates a
left join war_types c on a.war_type = c.war_type
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code
union all
select
    'INTRA-STATE_State_participants v5.1.csv' source_file,
    a.war_num,
    a.war_name,
    a.war_type,
    c.war_type_name,
    c.war_subtype,
    null::double disno,
    null::double dyindex,
    a.c_code_a,
    a.c_code_b,
    coalesce(d.state_name, a.participant_a) participant_a,
    coalesce(e.state_name, a.participant_b) participant_b,
    1 side_a,
    2 side_b,
    a.start_date,
    extract(year from a.start_date)::integer start_year,
    a.end_date,
    extract(year from a.end_date)::integer end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war,
    a.battle_deaths_a,
    a.battle_deaths_b,
    a.battle_deaths_total,
    a.outcome_a,
    null::integer outcome_b
from intra_dates a
left join war_types c on a.war_type = c.war_type
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code;
