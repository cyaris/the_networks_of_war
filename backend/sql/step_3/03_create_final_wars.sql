create or replace table final_wars as

select
    a.war_num,
    a.war_name,
    a.war_type war_type_code,
    a.war_type_name war_type,
    a.war_subtype,
    a.total_participants,
    a.total_dyads,
    a.start_date,
    a.end_date,
    extract(year from a.start_date)::integer start_year,
    extract(year from a.end_date)::integer end_year,
    a.ongoing_war ongoing_conflict,
    a.start_date_estimated,
    a.end_date_estimated,
    a.lagging_war,
    a.leading_war,
    date_diff('day', a.start_date, a.end_date) total_days_in_war,
    'war_num_' || replace(a.war_num::varchar, '.', '_') || '.json' file_name
from wars a;
