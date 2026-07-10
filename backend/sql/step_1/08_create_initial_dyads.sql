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
    a.war_num,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war
from initial_participants a
join war_side_counts b on a.war_num = b.war_num
                       and b.side_1_total = 1
where a.side = 1
union distinct
select
    a.war_num,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war
from initial_participants a
join war_side_counts b on a.war_num = b.war_num
                       and b.side_2_total = 1
where a.side = 2
union distinct
select
    a.war_num,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war
from initial_participants a
join war_side_counts b on a.war_num = b.war_num
                       and b.side_1_non_state = 1
where
    a.side = 1
    and a.c_code = -8
union distinct
select
    a.war_num,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war
from initial_participants a
join war_side_counts b on a.war_num = b.war_num
                       and b.side_2_non_state = 1
where
    a.side = 2
    and a.c_code = -8
union distinct
select
    a.war_num,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war
from initial_participants a
join war_side_counts b on a.war_num = b.war_num
                       and b.side_1_state = 1
where
    a.side = 1
    and a.c_code > 0
union distinct
select
    a.war_num,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war
from initial_participants a
join war_side_counts b on a.war_num = b.war_num
                       and b.side_2_state = 1
where
    a.side = 2
    and a.c_code > 0),

inferred_dyads as (

select
    a.war_num,
    a.c_code c_code_a,
    b.c_code c_code_b,
    a.participant participant_a,
    b.participant participant_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date,
    greatest(if(a.start_date >= b.start_date, coalesce(a.start_date_estimated, 0), 0), if(b.start_date >= a.start_date, coalesce(b.start_date_estimated, 0), 0)) start_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.end_date_estimated, 0), 0), if(b.end_date <= a.end_date, coalesce(b.end_date_estimated, 0), 0)) end_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.ongoing_war, 0), 0), if(b.end_date <= a.end_date, coalesce(b.ongoing_war, 0), 0)) ongoing_war
from initial_participants a
join anchors b on a.war_num = b.war_num
               and a.side != b.side
               and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),

group_dyads as (

select
    a.war_num,
    b.c_code c_code_a,
    a.c_code_b,
    b.participant participant_a,
    a.participant_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date,
    greatest(if(a.start_date >= b.start_date, coalesce(a.start_date_estimated, 0), 0), if(b.start_date >= a.start_date, coalesce(b.start_date_estimated, 0), 0)) start_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.end_date_estimated, 0), 0), if(b.end_date <= a.end_date, coalesce(b.end_date_estimated, 0), 0)) end_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.ongoing_war, 0), 0), if(b.end_date <= a.end_date, coalesce(b.ongoing_war, 0), 0)) ongoing_war
from war_dyads a
join initial_participants b on a.war_num = b.war_num
                            and a.side_a = b.side
                            and b.c_code <> -8
                            and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
where a.c_code_a = -8
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
union all
select
    a.war_num,
    a.c_code_a,
    b.c_code c_code_b,
    a.participant_a,
    b.participant participant_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date,
    greatest(if(a.start_date >= b.start_date, coalesce(a.start_date_estimated, 0), 0), if(b.start_date >= a.start_date, coalesce(b.start_date_estimated, 0), 0)) start_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.end_date_estimated, 0), 0), if(b.end_date <= a.end_date, coalesce(b.end_date_estimated, 0), 0)) end_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.ongoing_war, 0), 0), if(b.end_date <= a.end_date, coalesce(b.ongoing_war, 0), 0)) ongoing_war
from war_dyads a
join initial_participants b on a.war_num = b.war_num
                            and a.side_b = b.side
                            and b.c_code <> -8
                            and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
where a.c_code_b = -8
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

select
    war_num,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    ongoing_war
from dyads_after_mid
union distinct
select
    war_num,
    c_code_b c_code_a,
    c_code_a c_code_b,
    participant_b participant_a,
    participant_a participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    ongoing_war
from dyads_after_mid
union distinct
select
    war_num,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    ongoing_war
from inferred_dyads
union distinct
select
    war_num,
    c_code_b c_code_a,
    c_code_a c_code_b,
    participant_b participant_a,
    participant_a participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    ongoing_war
from inferred_dyads
union distinct
select
    war_num,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    ongoing_war
from group_dyads
union distinct
select
    war_num,
    c_code_b c_code_a,
    c_code_a c_code_b,
    participant_b participant_a,
    participant_a participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    ongoing_war
from group_dyads;
