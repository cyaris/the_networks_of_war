create or replace view war_dyads as

with

interstate_wars as (

select
    war_id,
    any_value(war_name) war_name,
    any_value(war_type_id) war_type_id
from source_interstate_wars
group by 1),

interstate_participant_sides as (

select
    war_id,
    c_code,
    if(min(side) = 1 and max(side) = 2, 3, max(side)) side
from source_interstate_wars
where c_code is not null
group by 1, 2)

select
    a.war_id,
    b.war_name,
    b.war_type_id,
    c.war_type,
    c.war_subtype,
    a.c_code_a,
    a.c_code_b,
    d.state_name participant_a,
    e.state_name participant_b,
    f.side side_a,
    g.side side_b,
    cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1) start_date,
    cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1, h.source_release_date) end_date,
    date_estimated(a.start_year_1, a.start_month_1, a.start_day_1) start_date_estimated,
    date_estimated(a.end_year_1, a.end_month_1, a.end_day_1) end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b
from source_interstate_war_dyads a
left join interstate_wars b on a.war_id = b.war_id
left join war_types c on b.war_type_id = c.war_type_id
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code
left join interstate_participant_sides f on a.war_id = f.war_id
                                         and a.c_code_a = f.c_code
left join interstate_participant_sides g on a.war_id = g.war_id
                                         and a.c_code_b = g.c_code
join source_file_versions h on h.source_key = 'interstate_war_dyads'
union all
select
    a.war_id,
    a.war_name,
    a.war_type_id,
    c.war_type,
    c.war_subtype,
    a.c_code_a,
    a.c_code_b,
    coalesce(d.state_name, a.participant_a) participant_a,
    coalesce(e.state_name, a.participant_b) participant_b,
    1 side_a,
    2 side_b,
    least(cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1), cow_date(a.start_year_2, a.start_month_2, a.start_day_2, 1, 1)) start_date,
    greatest(cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1, f.source_release_date), cow_end_date(a.end_year_2, a.end_month_2, a.end_day_2, f.source_release_date)) end_date,
    greatest(date_estimated(a.start_year_1, a.start_month_1, a.start_day_1), date_estimated(a.start_year_2, a.start_month_2, a.start_day_2)) start_date_estimated,
    greatest(date_estimated(a.end_year_1, a.end_month_1, a.end_day_1), date_estimated(a.end_year_2, a.end_month_2, a.end_day_2)) end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b
from source_extrastate_wars a
left join war_types c on a.war_type_id = c.war_type_id
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code
join source_file_versions f on f.source_key = 'extrastate_wars'
union all
select
    a.war_id,
    a.war_name,
    a.war_type_id,
    c.war_type,
    c.war_subtype,
    a.c_code_a,
    a.c_code_b,
    coalesce(d.state_name, a.participant_a) participant_a,
    coalesce(e.state_name, a.participant_b) participant_b,
    1 side_a,
    2 side_b,
    least(cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1), cow_date(a.start_year_2, a.start_month_2, a.start_day_2, 1, 1), cow_date(a.start_year_3, a.start_month_3, a.start_day_3, 1, 1), cow_date(a.start_year_4, a.start_month_4, a.start_day_4, 1, 1)) start_date,
    greatest(cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1, f.source_release_date), cow_end_date(a.end_year_2, a.end_month_2, a.end_day_2, f.source_release_date), cow_end_date(a.end_year_3, a.end_month_3, a.end_day_3, f.source_release_date), cow_end_date(a.end_year_4, a.end_month_4, a.end_day_4, f.source_release_date)) end_date,
    greatest(date_estimated(a.start_year_1, a.start_month_1, a.start_day_1), date_estimated(a.start_year_2, a.start_month_2, a.start_day_2), date_estimated(a.start_year_3, a.start_month_3, a.start_day_3), date_estimated(a.start_year_4, a.start_month_4, a.start_day_4)) start_date_estimated,
    greatest(date_estimated(a.end_year_1, a.end_month_1, a.end_day_1), date_estimated(a.end_year_2, a.end_month_2, a.end_day_2), date_estimated(a.end_year_3, a.end_month_3, a.end_day_3), date_estimated(a.end_year_4, a.end_month_4, a.end_day_4)) end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b
from source_intrastate_wars a
left join war_types c on a.war_type_id = c.war_type_id
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code
join source_file_versions f on f.source_key = 'intrastate_wars';

create or replace table dyads_after_sources as

with

cleaned_war_dyads as (

select
    a.war_id,
    any_value(a.war_name) war_name,
    any_value(a.war_type_id) war_type_id,
    any_value(a.war_type) war_type,
    any_value(a.war_subtype) war_subtype,
    a.c_code_a,
    a.c_code_b,
    coalesce(b.state_name, clean_participant(a.participant_a, d.replacement)) participant_a,
    coalesce(c.state_name, clean_participant(a.participant_b, e.replacement)) participant_b,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b
from war_dyads a
left join country_codes b on a.c_code_a = b.c_code
left join country_codes c on a.c_code_b = c.c_code
left join participant_name_replacements d on b.c_code is null
                                          and clean_text(a.participant_a) = d.source
left join participant_name_replacements e on c.c_code is null
                                          and clean_text(a.participant_b) = e.source
where
    a.participant_a is not null
    and a.participant_b is not null
group by 1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
union all
select
    a.war_id,
    any_value(a.war_name) war_name,
    any_value(a.war_type_id) war_type_id,
    any_value(a.war_type) war_type,
    any_value(a.war_subtype) war_subtype,
    a.c_code_b c_code_a,
    a.c_code_a c_code_b,
    coalesce(b.state_name, clean_participant(a.participant_b, d.replacement)) participant_a,
    coalesce(c.state_name, clean_participant(a.participant_a, e.replacement)) participant_b,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_b battle_deaths_a,
    a.battle_deaths_a battle_deaths_b
from war_dyads a
left join country_codes b on a.c_code_b = b.c_code
left join country_codes c on a.c_code_a = c.c_code
left join participant_name_replacements d on b.c_code is null
                                          and clean_text(a.participant_b) = d.source
left join participant_name_replacements e on c.c_code is null
                                          and clean_text(a.participant_a) = e.source
where
    a.participant_a is not null
    and a.participant_b is not null
group by 1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)

select
    war_id,
    any_value(war_name) war_name,
    any_value(war_type_id) war_type_id,
    any_value(war_type) war_type,
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
from cleaned_war_dyads
group by 1, 6, 7, 8, 9;
