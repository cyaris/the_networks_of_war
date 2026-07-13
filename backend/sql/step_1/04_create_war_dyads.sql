create or replace view war_dyads as

with

interstate_wars as (

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type
from source_interstate_wars
group by 1),

interstate_participant_sides as (

select
    war_num,
    c_code,
    if(min(side) = 1 and max(side) = 2, 3, max(side)) side
from source_interstate_wars
where c_code is not null
group by 1, 2)

select
    a.war_num,
    b.war_name,
    b.war_type,
    c.war_type_name,
    c.war_subtype,
    a.c_code_a,
    a.c_code_b,
    d.state_name participant_a,
    e.state_name participant_b,
    f.side side_a,
    g.side side_b,
    cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1) start_date,
    cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1) end_date,
    date_estimated(a.start_year_1, a.start_month_1, a.start_day_1) start_date_estimated,
    date_estimated(a.end_year_1, a.end_month_1, a.end_day_1) end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b
from source_interstate_war_dyads a
left join interstate_wars b on a.war_num = b.war_num
left join war_types c on b.war_type = c.war_type
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code
left join interstate_participant_sides f on a.war_num = f.war_num
                                         and a.c_code_a = f.c_code
left join interstate_participant_sides g on a.war_num = g.war_num
                                         and a.c_code_b = g.c_code
union all
select
    a.war_num,
    a.war_name,
    a.war_type,
    c.war_type_name,
    c.war_subtype,
    a.c_code_a,
    a.c_code_b,
    coalesce(d.state_name, a.participant_a) participant_a,
    coalesce(e.state_name, a.participant_b) participant_b,
    1 side_a,
    2 side_b,
    least(cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1), cow_date(a.start_year_2, a.start_month_2, a.start_day_2, 1, 1)) start_date,
    greatest(cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1), cow_end_date(a.end_year_2, a.end_month_2, a.end_day_2)) end_date,
    greatest(date_estimated(a.start_year_1, a.start_month_1, a.start_day_1), date_estimated(a.start_year_2, a.start_month_2, a.start_day_2)) start_date_estimated,
    greatest(date_estimated(a.end_year_1, a.end_month_1, a.end_day_1), date_estimated(a.end_year_2, a.end_month_2, a.end_day_2)) end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b
from source_extrastate_wars a
left join war_types c on a.war_type = c.war_type
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code
union all
select
    a.war_num,
    a.war_name,
    a.war_type,
    c.war_type_name,
    c.war_subtype,
    a.c_code_a,
    a.c_code_b,
    coalesce(d.state_name, a.participant_a) participant_a,
    coalesce(e.state_name, a.participant_b) participant_b,
    1 side_a,
    2 side_b,
    least(cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1), cow_date(a.start_year_2, a.start_month_2, a.start_day_2, 1, 1), cow_date(a.start_year_3, a.start_month_3, a.start_day_3, 1, 1), cow_date(a.start_year_4, a.start_month_4, a.start_day_4, 1, 1)) start_date,
    greatest(cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1), cow_end_date(a.end_year_2, a.end_month_2, a.end_day_2), cow_end_date(a.end_year_3, a.end_month_3, a.end_day_3), cow_end_date(a.end_year_4, a.end_month_4, a.end_day_4)) end_date,
    greatest(date_estimated(a.start_year_1, a.start_month_1, a.start_day_1), date_estimated(a.start_year_2, a.start_month_2, a.start_day_2), date_estimated(a.start_year_3, a.start_month_3, a.start_day_3), date_estimated(a.start_year_4, a.start_month_4, a.start_day_4)) start_date_estimated,
    greatest(date_estimated(a.end_year_1, a.end_month_1, a.end_day_1), date_estimated(a.end_year_2, a.end_month_2, a.end_day_2), date_estimated(a.end_year_3, a.end_month_3, a.end_day_3), date_estimated(a.end_year_4, a.end_month_4, a.end_day_4)) end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b
from source_intrastate_wars a
left join war_types c on a.war_type = c.war_type
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code;

create or replace table dyads_after_sources as

with

cleaned_war_dyads as (

select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(a.war_type_name) war_type_name,
    any_value(a.war_subtype) war_subtype,
    a.c_code_a,
    a.c_code_b,
    clean_participant(a.participant_a, b."replacement") participant_a,
    clean_participant(a.participant_b, c."replacement") participant_b,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b
from war_dyads a
left join participant_name_replacements b on clean_text(a.participant_a) = b."source"
left join participant_name_replacements c on clean_text(a.participant_b) = c."source"
where
    a.participant_a is not null
    and a.participant_b is not null
group by 1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
union all
select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(a.war_type_name) war_type_name,
    any_value(a.war_subtype) war_subtype,
    a.c_code_b c_code_a,
    a.c_code_a c_code_b,
    clean_participant(a.participant_b, b."replacement") participant_a,
    clean_participant(a.participant_a, c."replacement") participant_b,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_b battle_deaths_a,
    a.battle_deaths_a battle_deaths_b
from war_dyads a
left join participant_name_replacements b on clean_text(a.participant_b) = b."source"
left join participant_name_replacements c on clean_text(a.participant_a) = c."source"
where
    a.participant_a is not null
    and a.participant_b is not null
group by 1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    min(start_date) start_date,
    max(end_date) end_date,
    max(start_date_estimated) start_date_estimated,
    max(end_date_estimated) end_date_estimated,
    sum(if(battle_deaths_a >= 0, battle_deaths_a, null)) battle_deaths_a,
    sum(if(battle_deaths_b >= 0, battle_deaths_b, null)) battle_deaths_b
from cleaned_war_dyads a
group by 1, 6, 7, 8, 9;
