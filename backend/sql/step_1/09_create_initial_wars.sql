create or replace table initial_wars as

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    count(*) total_participants,
    min(start_date) start_date,
    min(start_year) start_year,
    max(end_date) end_date,
    max(end_year) end_year,
    max(ongoing_war) ongoing_war
from initial_participants
where war_type is not null
group by 1;
