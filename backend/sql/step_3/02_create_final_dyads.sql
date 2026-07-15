create or replace table final_dyads as

with

final_dyad_rows as (

select
    a.war_num,
    a.war_name,
    a.c_code_a,
    a.c_code_b,
    a.participant_a,
    a.participant_b,
    a.start_date,
    a.end_date,
    a.start_year,
    a.end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    if(columns('.*_[xyz]$') in (-9, -8) or (columns('.*_[xyz]$') is null and (a.c_code_a <= 0 or a.c_code_b <= 0)), null, coalesce(columns('.*_[xyz]$'), 0))
from dyadic_descriptives a)

select
    a.*,
    b.id as source,
    c.id as target
from final_dyad_rows a
join final_participants b on a.war_num = b.war_num
                         and if(a.c_code_a > 0, a.c_code_a::varchar, a.participant_a) = b.node_key
join final_participants c on a.war_num = c.war_num
                         and if(a.c_code_b > 0, a.c_code_b::varchar, a.participant_b) = c.node_key;
