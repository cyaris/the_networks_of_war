create or replace table initial_dyads as

with

war_side_counts as (

select
    war_num,
    count(distinct participant) filter (where side = 1) side_1_total,
    count(distinct participant) filter (where side = 2) side_2_total,
    count(distinct participant) filter (where side = 1 and c_code = -8) side_1_non_state,
    count(distinct participant) filter (where side = 2 and c_code = -8) side_2_non_state,
    count(distinct c_code) filter (where side = 1 and c_code > 0) side_1_state,
    count(distinct c_code) filter (where side = 2 and c_code > 0) side_2_state
from initial_participants
group by 1),

anchors as (

select
    a.source_file,
    a.war_num,
    a.war_name,
    a.war_type,
    a.war_type_name,
    a.war_subtype,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war,
    a.battle_deaths
from initial_participants a
join war_side_counts b on a.war_num = b.war_num
                       and (
                           (
                               b.side_1_total = 1
                               and a.side = 1
                           )
                           or (
                               b.side_1_total <> 1
                               and b.side_2_total = 1
                               and a.side = 2
                           )
                           or (
                               b.side_1_total <> 1
                               and b.side_2_total <> 1
                               and b.side_1_non_state = 1
                               and a.side = 1
                               and a.c_code = -8
                           )
                           or (
                               b.side_1_total <> 1
                               and b.side_2_total <> 1
                               and b.side_1_non_state <> 1
                               and b.side_2_non_state = 1
                               and a.side = 2
                               and a.c_code = -8
                           )
                           or (
                               b.side_1_state = 1
                               and a.side = 1
                               and a.c_code > 0
                           )
                           or (
                               b.side_1_state <> 1
                               and b.side_2_state = 1
                               and a.side = 2
                               and a.c_code > 0
                           )
                       )
),

inferred_dyads as (

select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(a.war_type_name) war_type_name,
    any_value(a.war_subtype) war_subtype,
    a.c_code c_code_a,
    b.c_code c_code_b,
    a.participant participant_a,
    b.participant participant_b,
    null::double battle_deaths_a,
    null::double battle_deaths_b,
    0 battle_deaths_est_a,
    0 battle_deaths_est_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date
from anchors a
join initial_participants b on a.war_num = b.war_num
                            and (
                                (a.side = 1 and b.side = 2)
                                or (a.side = 2 and b.side = 1)
                            )
                            and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
group by 1, 6, 7, 8, 9, 14, 15),

group_dyads as (

select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(a.war_type_name) war_type_name,
    any_value(a.war_subtype) war_subtype,
    b.c_code c_code_a,
    a.c_code_b,
    b.participant participant_a,
    a.participant_b,
    null::double battle_deaths_a,
    null::double battle_deaths_b,
    0 battle_deaths_est_a,
    0 battle_deaths_est_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date
from war_dyads a
join initial_participants b on a.war_num = b.war_num
                            and a.side_a = b.side
                            and b.c_code <> -8
                            and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
where a.c_code_a = -8
group by 1, 6, 7, 8, 9, 14, 15
union all
select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(a.war_type_name) war_type_name,
    any_value(a.war_subtype) war_subtype,
    a.c_code_a,
    b.c_code c_code_b,
    a.participant_a,
    b.participant participant_b,
    null::double battle_deaths_a,
    null::double battle_deaths_b,
    0 battle_deaths_est_a,
    0 battle_deaths_est_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date
from war_dyads a
join initial_participants b on a.war_num = b.war_num
                            and a.side_b = b.side
                            and b.c_code <> -8
                            and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
where a.c_code_b = -8
group by 1, 6, 7, 8, 9, 14, 15),

dyads_after_inference as (

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
    battle_deaths_a,
    battle_deaths_b,
    battle_deaths_est_a,
    battle_deaths_est_b,
    start_date,
    end_date
from dyads_after_mid
union all
select
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code_b c_code_a,
    c_code_a c_code_b,
    participant_b participant_a,
    participant_a participant_b,
    battle_deaths_b battle_deaths_a,
    battle_deaths_a battle_deaths_b,
    battle_deaths_est_b battle_deaths_est_a,
    battle_deaths_est_a battle_deaths_est_b,
    start_date,
    end_date
from dyads_after_mid
union all
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
    battle_deaths_a,
    battle_deaths_b,
    battle_deaths_est_a,
    battle_deaths_est_b,
    start_date,
    end_date
from inferred_dyads
union all
select
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code_b c_code_a,
    c_code_a c_code_b,
    participant_b participant_a,
    participant_a participant_b,
    battle_deaths_b battle_deaths_a,
    battle_deaths_a battle_deaths_b,
    battle_deaths_est_b battle_deaths_est_a,
    battle_deaths_est_a battle_deaths_est_b,
    start_date,
    end_date
from inferred_dyads
union all
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
    battle_deaths_a,
    battle_deaths_b,
    battle_deaths_est_a,
    battle_deaths_est_b,
    start_date,
    end_date
from group_dyads
union all
select
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code_b c_code_a,
    c_code_a c_code_b,
    participant_b participant_a,
    participant_a participant_b,
    battle_deaths_b battle_deaths_a,
    battle_deaths_a battle_deaths_b,
    battle_deaths_est_b battle_deaths_est_a,
    battle_deaths_est_a battle_deaths_est_b,
    start_date,
    end_date
from group_dyads)

select
    a.war_num,
    a.c_code_a,
    a.c_code_b,
    a.participant_a,
    a.participant_b,
    a.battle_deaths_a,
    a.battle_deaths_b,
    a.battle_deaths_est_a,
    a.battle_deaths_est_b,
    a.start_date,
    a.end_date,
    b.range::integer "year"
from dyads_after_inference a
join range(1500, 2100) b on b.range between extract(year from a.start_date)::integer and extract(year from a.end_date)::integer
where
    a.c_code_a <> -8
    and a.c_code_b <> -8
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;
