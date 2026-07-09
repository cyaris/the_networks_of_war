create or replace table initial_participants as

with

participant_union as (

select
    source_file,
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code,
    participant,
    side,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    ongoing_war,
    battle_deaths
from war_participants
union all
select
    'dyadic_data' source_file,
    a.war_num,
    coalesce(any_value(a.war_name), '') war_name,
    any_value(a.war_type) war_type,
    any_value(a.war_type_name) war_type_name,
    any_value(a.war_subtype) war_subtype,
    a.c_code_a c_code,
    a.participant_a participant,
    null::integer side,
    min(a.start_date) start_date,
    min(a.end_date) end_date,
    null::integer start_date_estimated,
    null::integer end_date_estimated,
    null::integer ongoing_war,
    sum(a.battle_deaths_a) battle_deaths
from dyads_after_mid a
left join war_participants c on a.war_num = c.war_num
                             and a.c_code_a = c.c_code
                             and a.participant_a = c.participant
where c.c_code is null
group by 2, 7, 8),

side_assignments as (

select
    a.war_num,
    a.c_code,
    if(count(distinct if(c.side = 3, 3, 3 - c.side)) = 1, min(if(c.side = 3, 3, 3 - c.side)), null) side
from participant_union a
join dyads_after_mid b on a.war_num = b.war_num
                       and a.c_code = b.c_code_a
join participant_union c on b.war_num = c.war_num
                         and b.c_code_b = c.c_code
where
    a.side is null
    and c.side is not null
group by 1, 2)

select
    a.source_file,
    a.war_num,
    a.war_name,
    a.war_type,
    a.war_type_name,
    a.war_subtype,
    a.c_code,
    a.participant,
    coalesce(a.side, b.side) side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war,
    a.battle_deaths
from participant_union a
left join side_assignments b on a.war_num = b.war_num
                             and a.c_code = b.c_code;
