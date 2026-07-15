create or replace table dyads as

with

war_side_counts as (

select
    war_id,
    count(distinct participant) filter (where side = 1) side_1_total,
    count(distinct participant) filter (where side = 2) side_2_total,
    count(distinct participant) filter (where side = 1 and c_code = -8) side_1_non_state,
    count(distinct participant) filter (where side = 2 and c_code = -8) side_2_non_state,
    count(distinct c_code) filter (where side = 1 and c_code > 0) side_1_state,
    count(distinct c_code) filter (where side = 2 and c_code > 0) side_2_state
from participants
group by 1),

anchors as (

select
    a.war_id,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated
from participants a
join war_side_counts b on a.war_id = b.war_id
                       and b.side_1_total = 1
where a.side = 1
union
select
    a.war_id,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated
from participants a
join war_side_counts b on a.war_id = b.war_id
                       and b.side_2_total = 1
where a.side = 2
union
select
    a.war_id,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated
from participants a
join war_side_counts b on a.war_id = b.war_id
                       and b.side_1_non_state = 1
where
    a.side = 1
    and a.c_code = -8
union
select
    a.war_id,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated
from participants a
join war_side_counts b on a.war_id = b.war_id
                       and b.side_2_non_state = 1
where
    a.side = 2
    and a.c_code = -8
union
select
    a.war_id,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated
from participants a
join war_side_counts b on a.war_id = b.war_id
                       and b.side_1_state = 1
where
    a.side = 1
    and a.c_code > 0
union
select
    a.war_id,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated
from participants a
join war_side_counts b on a.war_id = b.war_id
                       and b.side_2_state = 1
where
    a.side = 2
    and a.c_code > 0),

inferred_dyads as (

select
    a.war_id,
    a.c_code c_code_a,
    b.c_code c_code_b,
    a.participant participant_a,
    b.participant participant_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date,
    greatest(if(a.start_date >= b.start_date, coalesce(a.start_date_estimated, 0), 0), if(b.start_date >= a.start_date, coalesce(b.start_date_estimated, 0), 0)) start_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.end_date_estimated, 0), 0), if(b.end_date <= a.end_date, coalesce(b.end_date_estimated, 0), 0)) end_date_estimated
from participants a
join anchors b on a.war_id = b.war_id
               and a.side != b.side
               and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9),

group_dyads as (

select
    a.war_id,
    b.c_code c_code_a,
    a.c_code_b,
    b.participant participant_a,
    a.participant_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date,
    greatest(if(a.start_date >= b.start_date, coalesce(a.start_date_estimated, 0), 0), if(b.start_date >= a.start_date, coalesce(b.start_date_estimated, 0), 0)) start_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.end_date_estimated, 0), 0), if(b.end_date <= a.end_date, coalesce(b.end_date_estimated, 0), 0)) end_date_estimated
from war_dyads a
join participants b on a.war_id = b.war_id
                            and a.side_a = b.side
                            and b.c_code <> -8
                            and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
where a.c_code_a = -8
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
union all
select
    a.war_id,
    a.c_code_a,
    b.c_code c_code_b,
    a.participant_a,
    b.participant participant_b,
    greatest(a.start_date, b.start_date) start_date,
    least(a.end_date, b.end_date) end_date,
    greatest(if(a.start_date >= b.start_date, coalesce(a.start_date_estimated, 0), 0), if(b.start_date >= a.start_date, coalesce(b.start_date_estimated, 0), 0)) start_date_estimated,
    greatest(if(a.end_date <= b.end_date, coalesce(a.end_date_estimated, 0), 0), if(b.end_date <= a.end_date, coalesce(b.end_date_estimated, 0), 0)) end_date_estimated
from war_dyads a
join participants b on a.war_id = b.war_id
                            and a.side_b = b.side
                            and b.c_code <> -8
                            and least(a.end_date, b.end_date) > greatest(a.start_date, b.start_date)
where a.c_code_b = -8
group by 1, 2, 3, 4, 5, 6, 7, 8, 9),

all_dyads as (

select
    war_id,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated
from dyads_after_mid
union all
select
    war_id,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated
from inferred_dyads
union all
select
    war_id,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated
from group_dyads),

canonical_dyads as (

select
    war_id,
    if(c_code_a::varchar || '/' || participant_a <= c_code_b::varchar || '/' || participant_b, c_code_a, c_code_b) c_code_a,
    if(c_code_a::varchar || '/' || participant_a <= c_code_b::varchar || '/' || participant_b, c_code_b, c_code_a) c_code_b,
    if(c_code_a::varchar || '/' || participant_a <= c_code_b::varchar || '/' || participant_b, participant_a, participant_b) participant_a,
    if(c_code_a::varchar || '/' || participant_a <= c_code_b::varchar || '/' || participant_b, participant_b, participant_a) participant_b,
    start_date,
    end_date,
    start_date_estimated,
    end_date_estimated,
    least(c_code_a::varchar || '/' || participant_a, c_code_b::varchar || '/' || participant_b) dyad_key_a,
    greatest(c_code_a::varchar || '/' || participant_a, c_code_b::varchar || '/' || participant_b) dyad_key_b
from all_dyads)

select
    war_id,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    min(start_date) over (partition by war_id, dyad_key_a, dyad_key_b) start_date,
    max(end_date) over (partition by war_id, dyad_key_a, dyad_key_b) end_date,
    max(start_date_estimated) over (partition by war_id, dyad_key_a, dyad_key_b) start_date_estimated,
    max(end_date_estimated) over (partition by war_id, dyad_key_a, dyad_key_b) end_date_estimated
from canonical_dyads
qualify row_number() over (partition by war_id, dyad_key_a, dyad_key_b order by start_date, end_date desc, start_date_estimated desc, end_date_estimated desc) = 1;
