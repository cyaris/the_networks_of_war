create or replace table final_dyads as

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
from dyadic_descriptives a;
