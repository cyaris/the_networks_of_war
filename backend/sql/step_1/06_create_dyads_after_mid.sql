create or replace table dyads_after_mid as

with

source_mid_war_numbers as (

select
    disno,
    any_value(war_num) war_num
from source_interstate_war_dyads
where disno is not null
group by 1),

adjusted_mid_war_numbers as (

select
    a.disno,
    a.war_num
from source_interstate_mid_war_num_adjustments a
join source_file_versions b on a.source_key = b.source_key
                            and a.source_version = b.source_version
left join source_mid_war_numbers c on a.disno = c.disno
where c.disno is null),

mid_war_numbers as (

select
    disno,
    war_num
from source_mid_war_numbers
union all
select
    disno,
    war_num
from adjusted_mid_war_numbers),

mid_wars_prepared as (

select
    a.disno,
    coalesce(b.war_num, -1) war_num,
    a.c_code_a,
    a.c_code_b,
    cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1) start_date,
    cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1) end_date,
    date_estimated(a.start_year_1, a.start_month_1, a.start_day_1) start_date_estimated,
    date_estimated(a.end_year_1, a.end_month_1, a.end_day_1) end_date_estimated,
    a.battle_deaths_estimated_a,
    a.battle_deaths_estimated_b
from source_interstate_mid_dyads a
left join mid_war_numbers b on a.disno = b.disno
where a.war = 1),

mid_wars_directed as (

select
    disno,
    war_num,
    c_code_a,
    c_code_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    battle_deaths_estimated_a,
    battle_deaths_estimated_b
from mid_wars_prepared
union all
select
    disno,
    war_num,
    c_code_b c_code_a,
    c_code_a c_code_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    battle_deaths_estimated_b battle_deaths_estimated_a,
    battle_deaths_estimated_a battle_deaths_estimated_b
from mid_wars_prepared),

war_name_candidates as (

select
    war_num,
    any_value(war_name) war_name
from dyads_after_sources
group by 1
union all
select
    war_num,
    any_value(war_name) war_name
from source_interstate_wars
where war_name is not null
group by 1
union all
select
    a.war_num,
    any_value(a.war_name) war_name
from source_interstate_war_metadata_adjustments a
join source_file_versions b on a.source_key = b.source_key
                            and a.source_version = b.source_version
group by 1),

war_names as (

select
    war_num,
    any_value(war_name) war_name
from war_name_candidates
group by 1),

mid_dyads as (

select
    a.war_num,
    any_value(b.war_name) war_name,
    null::integer war_type,
    null::varchar war_type_name,
    null::varchar war_subtype,
    a.disno,
    a.c_code_a,
    a.c_code_b,
    clean_participant(c.state_name, e.replacement_participant) participant_a,
    clean_participant(d.state_name, f.replacement_participant) participant_b,
    min(a.start_date) start_date,
    max(a.end_date) end_date,
    max(a.start_date_estimated) start_date_estimated,
    max(a.end_date_estimated) end_date_estimated,
    null::double battle_deaths_a,
    null::double battle_deaths_b,
    sum(greatest(coalesce(a.battle_deaths_estimated_a, 0), 0)) battle_deaths_estimated_a,
    sum(greatest(coalesce(a.battle_deaths_estimated_b, 0), 0)) battle_deaths_estimated_b
from mid_wars_directed a
left join war_names b on a.war_num = b.war_num
left join country_codes c on a.c_code_a = c.c_code
left join country_codes d on a.c_code_b = d.c_code
left join participant_name_replacements e on clean_text(c.state_name) = e.source_participant
left join participant_name_replacements f on clean_text(d.state_name) = f.source_participant
group by 1, 6, 7, 8, 9, 10),

merged_dyads as (

select
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    battle_deaths_a,
    battle_deaths_b,
    null::double battle_deaths_estimated_a,
    null::double battle_deaths_estimated_b
from dyads_after_sources
union all
select
    a.war_num,
    a.war_name,
    a.war_type,
    a.war_type_name,
    a.war_subtype,
    a.c_code_a,
    a.c_code_b,
    a.participant_a,
    a.participant_b,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths_a,
    a.battle_deaths_b,
    a.battle_deaths_estimated_a,
    a.battle_deaths_estimated_b
from mid_dyads a
left join dyads_after_sources b on a.war_num = b.war_num
                                and a.c_code_a = b.c_code_a
                                and a.c_code_b = b.c_code_b
                                and least(a.end_date, b.end_date) >= greatest(a.start_date, b.start_date)
where b.war_num is null)

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
    coalesce(nullif(sum(coalesce(battle_deaths_a, 0)), 0), sum(coalesce(battle_deaths_estimated_a, 0)), 0) battle_deaths_a,
    coalesce(nullif(sum(coalesce(battle_deaths_b, 0)), 0), sum(coalesce(battle_deaths_estimated_b, 0)), 0) battle_deaths_b,
    if(nullif(sum(coalesce(battle_deaths_a, 0)), 0) is null and sum(coalesce(battle_deaths_estimated_a, 0)) > 0, 1, 0) battle_deaths_estimated_a,
    if(nullif(sum(coalesce(battle_deaths_b, 0)), 0) is null and sum(coalesce(battle_deaths_estimated_b, 0)) > 0, 1, 0) battle_deaths_estimated_b
from merged_dyads
group by 1, 6, 7, 8, 9;
