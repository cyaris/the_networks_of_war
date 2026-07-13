create or replace table dyad_years as

select
    a.war_num,
    a.c_code_a,
    a.c_code_b,
    a.participant_a,
    a.participant_b,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    b.range::integer "year"
from dyads a
join range(1500, 2100) b on b.range between extract(year from a.start_date)::integer and extract(year from a.end_date)::integer;
