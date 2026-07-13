create or replace table wars as

with

participant_counts as (

select
    war_num,
    count(*) total_participants,
    min(start_date) start_date,
    max(end_date) end_date,
    max(start_date_estimated) start_date_estimated,
    max(end_date_estimated) end_date_estimated
from participants
group by 1),

dyad_counts as (

select
    war_num,
    count(*) total_dyads,
    min(start_date) start_date,
    max(end_date) end_date,
    max(start_date_estimated) start_date_estimated,
    max(end_date_estimated) end_date_estimated
from dyads
group by 1),

war_nums as (

select war_num
from participant_counts
union
select war_num
from dyad_counts),

source_war_rows as (

select
    war_num,
    lagging_war,
    leading_war,
    greatest(ongoing_war(end_year_1), ongoing_war(end_year_2)) ongoing_war
from source_interstate_wars
union all
select
    war_num,
    lagging_war,
    leading_war,
    greatest(ongoing_war(end_year_1), ongoing_war(end_year_2)) ongoing_war
from source_extrastate_wars
union all
select
    war_num,
    lagging_war,
    leading_war,
    greatest(ongoing_war(end_year_1), ongoing_war(end_year_2), ongoing_war(end_year_3), ongoing_war(end_year_4)) ongoing_war
from source_intrastate_wars),

source_wars as (

select
    war_num,
    max(lagging_war) lagging_war,
    max(leading_war) leading_war,
    max(ongoing_war) ongoing_war
from source_war_rows
group by 1),

war_metadata_rows as (

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype
from war_participants
where war_type is not null
group by 1
union all
select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(b.war_type_name) war_type_name,
    any_value(b.war_subtype) war_subtype
from source_interstate_wars a
left join war_types b on a.war_type = b.war_type
where a.war_type is not null
group by 1
union all
select
    a.war_num,
    any_value(a.war_name) war_name,
    any_value(a.war_type) war_type,
    any_value(b.war_type_name) war_type_name,
    any_value(b.war_subtype) war_subtype
from source_interstate_war_metadata_adjustments a
join source_file_versions c on a.source_key = c.source_key
                            and a.source_version = c.source_version
left join war_types b on a.war_type = b.war_type
group by 1),

war_metadata as (

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype
from war_metadata_rows
group by 1)

select
    w.war_num,
    b.war_name,
    b.war_type,
    b.war_type_name,
    b.war_subtype,
    coalesce(a.total_participants, 0) total_participants,
    coalesce(d.total_dyads, 0) total_dyads,
    coalesce(a.start_date, d.start_date) start_date,
    coalesce(a.end_date, d.end_date) end_date,
    coalesce(a.start_date_estimated, d.start_date_estimated) start_date_estimated,
    coalesce(a.end_date_estimated, d.end_date_estimated) end_date_estimated,
    coalesce(c.ongoing_war, 0) ongoing_war,
    c.lagging_war,
    c.leading_war
from war_nums w
left join participant_counts a on w.war_num = a.war_num
join war_metadata b on w.war_num = b.war_num
left join source_wars c on w.war_num = c.war_num
left join dyad_counts d on w.war_num = d.war_num;
