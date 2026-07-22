create or replace table dyads_after_mid as

with

source_mid_war_ids as (

select
    disno,
    any_value(war_id) war_id
from source_interstate_war_dyads
where disno is not null
group by 1),

adjusted_mid_war_ids as (

select
    a.disno,
    a.war_id
from source_interstate_mid_war_id_adjustments a
join source_file_versions b on a.source_key = b.source_key
                            and a.source_version = b.source_version
left join source_mid_war_ids c on a.disno = c.disno
where c.disno is null),

mid_war_ids as (

select
    disno,
    war_id
from source_mid_war_ids
union all
select
    disno,
    war_id
from adjusted_mid_war_ids),

mid_wars_prepared as (

select
    a.disno,
    coalesce(b.war_id, -1) war_id,
    a.c_code_a,
    a.c_code_b,
    cow_date(a.start_year_1, a.start_month_1, a.start_day_1, 1, 1) start_date,
    cow_end_date(a.end_year_1, a.end_month_1, a.end_day_1, c.source_release_date) end_date,
    date_estimated(a.start_year_1, a.start_month_1, a.start_day_1) start_date_estimated,
    date_estimated(a.end_year_1, a.end_month_1, a.end_day_1) end_date_estimated,
    a.battle_deaths_estimated_a,
    a.battle_deaths_estimated_b
from source_interstate_mid_dyads a
left join mid_war_ids b on a.disno = b.disno
join source_file_versions c on c.source_key = 'interstate_mid_dyads'
where a.war = 1),

mid_wars_directed as (

select
    disno,
    war_id,
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
    war_id,
    c_code_b c_code_a,
    c_code_a c_code_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    battle_deaths_estimated_b battle_deaths_estimated_a,
    battle_deaths_estimated_a battle_deaths_estimated_b
from mid_wars_prepared),

war_metadata_candidates as (

select
    war_id,
    any_value(war_name) war_name,
    any_value(war_type_id) war_type_id,
    any_value(war_type) war_type,
    any_value(war_subtype) war_subtype
from dyads_after_sources
group by 1
union all
select
    a.war_id,
    any_value(a.war_name) war_name,
    any_value(a.war_type_id) war_type_id,
    any_value(b.war_type) war_type,
    any_value(b.war_subtype) war_subtype
from source_interstate_wars a
left join war_types b on a.war_type_id = b.war_type_id
where
    a.war_name is not null
    and a.war_type_id is not null
group by 1
union all
select
    a.war_id,
    any_value(a.war_name) war_name,
    any_value(a.war_type_id) war_type_id,
    any_value(c.war_type) war_type,
    any_value(c.war_subtype) war_subtype
from source_interstate_war_metadata_adjustments a
join source_file_versions b on a.source_key = b.source_key
                            and a.source_version = b.source_version
left join war_types c on a.war_type_id = c.war_type_id
group by 1),

war_metadata as (

select
    war_id,
    any_value(war_name order by war_name is null, war_name) war_name,
    any_value(war_type_id order by war_type_id is null, war_type_id) war_type_id,
    any_value(war_type order by war_type is null, war_type) war_type,
    any_value(war_subtype order by war_subtype is null, war_subtype) war_subtype
from war_metadata_candidates
group by 1),

mid_dyads as (

select
    a.war_id,
    any_value(b.war_name) war_name,
    any_value(b.war_type_id) war_type_id,
    any_value(b.war_type) war_type,
    any_value(b.war_subtype) war_subtype,
    a.disno,
    a.c_code_a,
    a.c_code_b,
    c.state_name participant_a,
    d.state_name participant_b,
    min(a.start_date) start_date,
    max(a.end_date) end_date,
    max(a.start_date_estimated) start_date_estimated,
    max(a.end_date_estimated) end_date_estimated,
    null::double battle_deaths_a,
    null::double battle_deaths_b,
    sum(greatest(coalesce(a.battle_deaths_estimated_a, 0), 0)) battle_deaths_estimated_a,
    sum(greatest(coalesce(a.battle_deaths_estimated_b, 0), 0)) battle_deaths_estimated_b
from mid_wars_directed a
left join war_metadata b on a.war_id = b.war_id
left join country_codes c on a.c_code_a = c.c_code
left join country_codes d on a.c_code_b = d.c_code
group by 1, 6, 7, 8, 9, 10),

adjusted_war_dyads as (

select
    a.war_id,
    any_value(c.war_name) war_name,
    any_value(c.war_type_id) war_type_id,
    any_value(c.war_type) war_type,
    any_value(c.war_subtype) war_subtype,
    a.c_code_a,
    a.c_code_b,
    d.state_name participant_a,
    e.state_name participant_b,
    min(a.start_date) start_date,
    max(a.end_date) end_date,
    0 start_date_estimated,
    0 end_date_estimated,
    null::double battle_deaths_a,
    null::double battle_deaths_b,
    0 battle_deaths_estimated_a,
    0 battle_deaths_estimated_b
from source_interstate_war_dyad_adjustments a
join source_file_versions b on a.source_key = b.source_key
                            and a.source_version = b.source_version
left join war_metadata c on a.war_id = c.war_id
left join country_codes d on a.c_code_a = d.c_code
left join country_codes e on a.c_code_b = e.c_code
group by 1, 6, 7, 8, 9),

merged_dyads as (

select
    war_id,
    war_name,
    war_type_id,
    war_type,
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
    a.war_id,
    a.war_name,
    a.war_type_id,
    a.war_type,
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
left join dyads_after_sources b on a.war_id = b.war_id
                                and a.c_code_a = b.c_code_a
                                and a.c_code_b = b.c_code_b
                                and least(a.end_date, b.end_date) >= greatest(a.start_date, b.start_date)
where b.war_id is null
union all
select
    a.war_id,
    a.war_name,
    a.war_type_id,
    a.war_type,
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
from adjusted_war_dyads a
left join dyads_after_sources b on a.war_id = b.war_id
                                and a.c_code_a = b.c_code_a
                                and a.c_code_b = b.c_code_b
                                and least(a.end_date, b.end_date) >= greatest(a.start_date, b.start_date)
left join mid_dyads c on a.war_id = c.war_id
                      and a.c_code_a = c.c_code_a
                      and a.c_code_b = c.c_code_b
                      and least(a.end_date, c.end_date) >= greatest(a.start_date, c.start_date)
where
    b.war_id is null
    and c.war_id is null)

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
    coalesce(nullif(sum(coalesce(battle_deaths_a, 0)), 0), sum(coalesce(battle_deaths_estimated_a, 0)), 0) battle_deaths_a,
    coalesce(nullif(sum(coalesce(battle_deaths_b, 0)), 0), sum(coalesce(battle_deaths_estimated_b, 0)), 0) battle_deaths_b,
    if(nullif(sum(coalesce(battle_deaths_a, 0)), 0) is null and sum(coalesce(battle_deaths_estimated_a, 0)) > 0, 1, 0) battle_deaths_estimated_a,
    if(nullif(sum(coalesce(battle_deaths_b, 0)), 0) is null and sum(coalesce(battle_deaths_estimated_b, 0)) > 0, 1, 0) battle_deaths_estimated_b
from merged_dyads
group by 1, 6, 7, 8, 9;
