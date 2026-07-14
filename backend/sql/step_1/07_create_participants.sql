create or replace table participants as

with

participant_union as (

select
    war_num,
    c_code,
    participant,
    side,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    battle_deaths,
    battle_deaths_estimated
from war_participants
union all
select
    a.war_num,
    a.c_code_a c_code,
    a.participant_a participant,
    null::integer side,
    min(a.start_date) start_date,
    min(a.end_date) end_date,
    max(a.start_date_estimated) start_date_estimated,
    max(a.end_date_estimated) end_date_estimated,
    sum(a.battle_deaths_a) battle_deaths,
    max(a.battle_deaths_estimated_a) battle_deaths_estimated
from dyads_after_mid a
left join war_participants c on a.war_num = c.war_num
                             and a.c_code_a = c.c_code
                             and a.participant_a = c.participant
where c.c_code is null
group by 1, 2, 3),

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
                         and c.side is not null
where a.side is null
group by 1, 2),

source_side_adjustments as (

select
    a.war_num,
    a.c_code,
    clean_participant(a.participant, c.replacement) participant,
    a.side
from source_participant_side_adjustments a
join source_file_versions b on a.source_key = b.source_key
                            and a.source_version = b.source_version
left join participant_name_replacements c on clean_text(a.participant) = c.source)

select
    a.war_num,
    a.c_code,
    a.participant,
    coalesce(a.side, b.side, c.side) side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths,
    a.battle_deaths_estimated
from participant_union a
left join side_assignments b on a.war_num = b.war_num
                             and a.c_code = b.c_code
left join source_side_adjustments c on a.war_num = c.war_num
                                   and a.c_code = c.c_code
                                   and a.participant = c.participant;
