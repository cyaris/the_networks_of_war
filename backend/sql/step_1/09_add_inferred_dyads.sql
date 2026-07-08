create or replace table dyads_after_inference as
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
from participants_after_dyads
group by 1),

primary_anchors as (

select a.*
from participants_after_dyads a
join war_side_counts b on a.war_num = b.war_num
where (
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
    )),

state_anchors as (

select a.*
from participants_after_dyads a
join war_side_counts b on a.war_num = b.war_num
where (
        b.side_1_state = 1
        and a.side = 1
        and a.c_code > 0
    )
    or (
        b.side_1_state <> 1
        and b.side_2_state = 1
        and a.side = 2
        and a.c_code > 0
    )),

manual_anchors as (

select *
from participants_after_dyads
where
    war_num = 820
    and participant in ('France', 'Democratic Republic of the Congo')),

anchors as (

select *
from primary_anchors
union all
select *
from state_anchors
union all
select *
from manual_anchors),

inferred_dyads as (

select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(a.war_type_name) war_type_name,
    any_value(a.war_subtype) war_subtype,
    null::double disno,
    a.c_code c_code_a,
    a.participant participant_a,
    b.c_code c_code_b,
    b.participant participant_b,
    null::double battle_deaths_a,
    null::double battle_deaths_b,
    0 battle_deaths_est_a,
    0 battle_deaths_est_b,
    greatest(a.start_date, b.start_date) start_date,
    extract(year from greatest(a.start_date, b.start_date))::integer start_year,
    least(a.end_date, b.end_date) end_date,
    extract(year from least(a.end_date, b.end_date))::integer end_year
from anchors a
join participants_after_dyads b on a.war_num = b.war_num
                                and ((a.side = 1 and b.side = 2)
                                    or (a.side = 2 and b.side = 1
                                )
where least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
group by 1, 7, 8, 9, 10, 15, 16, 17, 18),

combined_dyads as (

select *
from dyads_after_mid
union all
select *
from inferred_dyads),

combined_dyads_directed as (

select *
from combined_dyads
union all
select
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    disno,
    c_code_b c_code_a,
    participant_b participant_a,
    c_code_a c_code_b,
    participant_a participant_b,
    battle_deaths_b battle_deaths_a,
    battle_deaths_a battle_deaths_b,
    battle_deaths_est_b battle_deaths_est_a,
    battle_deaths_est_a battle_deaths_est_b,
    start_date,
    start_year,
    end_date,
    end_year
from combined_dyads)

select distinct *
from combined_dyads_directed;
