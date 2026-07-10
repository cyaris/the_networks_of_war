create or replace table initial_wars as

with

participant_counts as (

select
    war_num,
    count(*) total_participants,
    min(start_date) start_date,
    max(end_date) end_date,
    max(start_date_estimated) start_date_estimated,
    max(end_date_estimated) end_date_estimated,
    max(ongoing_war) ongoing_war
from initial_participants
group by 1),

source_transition_rows as (

select
    war_num,
    lagging_war,
    leading_war
from source_interstate_wars
union all
select
    war_num,
    lagging_war,
    leading_war
from source_extrastate_wars
union all
select
    war_num,
    lagging_war,
    leading_war
from source_intrastate_wars),

source_transitions as (

select
    war_num,
    coalesce(max(lagging_war), -8) lagging_war,
    coalesce(max(leading_war), -8) leading_war
from source_transition_rows
group by 1),

war_metadata as (

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype
from war_participants
where war_type is not null
group by 1)

select
    a.war_num,
    b.war_name,
    b.war_type,
    b.war_type_name,
    b.war_subtype,
    a.total_participants,
    a.start_date,
    a.end_date,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war,
    coalesce(c.lagging_war, -8) lagging_war,
    coalesce(c.leading_war, -8) leading_war
from participant_counts a
join war_metadata b on a.war_num = b.war_num
left join source_transitions c on a.war_num = c.war_num;
