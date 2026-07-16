create or replace table wars as

with

participant_counts as (

select
    war_id,
    count(*) total_participants,
    min(start_date) start_date,
    max(end_date) end_date,
    max(start_date_estimated) start_date_estimated,
    max(end_date_estimated) end_date_estimated
from participants
group by 1),

dyad_counts as (

select
    war_id,
    count(*) total_dyads,
    min(start_date) start_date,
    max(end_date) end_date,
    max(start_date_estimated) start_date_estimated,
    max(end_date_estimated) end_date_estimated
from dyads
group by 1),

war_ids as (

select war_id
from participant_counts
union
select war_id
from dyad_counts),

source_war_rows as (

select
    war_id,
    lagging_war,
    leading_war,
    greatest(ongoing_war(end_year_1), ongoing_war(end_year_2)) ongoing_war
from source_interstate_wars
union all
select
    war_id,
    lagging_war,
    leading_war,
    greatest(ongoing_war(end_year_1), ongoing_war(end_year_2)) ongoing_war
from source_extrastate_wars
union all
select
    war_id,
    lagging_war,
    leading_war,
    greatest(ongoing_war(end_year_1), ongoing_war(end_year_2), ongoing_war(end_year_3), ongoing_war(end_year_4)) ongoing_war
from source_intrastate_wars),

source_wars as (

select
    war_id,
    max(lagging_war) lagging_war,
    max(leading_war) leading_war,
    max(ongoing_war) ongoing_war
from source_war_rows
group by 1),

war_metadata_rows as (

select
    war_id,
    any_value(war_name) war_name,
    any_value(war_type_id) war_type_id,
    any_value(war_type) war_type,
    any_value(war_subtype) war_subtype
from war_participants
where war_type_id is not null
group by 1
union all
select
    a.war_id,
    any_value(a.war_name) war_name,
    any_value(a.war_type_id) war_type_id,
    any_value(b.war_type) war_type,
    any_value(b.war_subtype) war_subtype
from source_interstate_wars a
left join war_types b on a.war_type_id = b.war_type_id
where a.war_type_id is not null
group by 1
union all
select
    a.war_id,
    any_value(a.war_name) war_name,
    any_value(a.war_type_id) war_type_id,
    any_value(b.war_type) war_type,
    any_value(b.war_subtype) war_subtype
from source_interstate_war_metadata_adjustments a
join source_file_versions c on a.source_key = c.source_key
                            and a.source_version = c.source_version
left join war_types b on a.war_type_id = b.war_type_id
group by 1),

war_metadata as (

select
    war_id,
    any_value(war_name) war_name,
    any_value(war_type_id) war_type_id,
    any_value(war_type) war_type,
    any_value(war_subtype) war_subtype
from war_metadata_rows
group by 1)

select
    a.war_id,
    c.war_name,
    c.war_type_id,
    c.war_type,
    c.war_subtype,
    coalesce(b.total_participants, 0) total_participants,
    coalesce(e.total_dyads, 0) total_dyads,
    coalesce(b.start_date, e.start_date) start_date,
    coalesce(b.end_date, e.end_date) end_date,
    coalesce(b.start_date_estimated, e.start_date_estimated) start_date_estimated,
    coalesce(b.end_date_estimated, e.end_date_estimated) end_date_estimated,
    coalesce(d.ongoing_war, 0) ongoing_war,
    d.lagging_war,
    d.leading_war
from war_ids a
left join participant_counts b on a.war_id = b.war_id
join war_metadata c on a.war_id = c.war_id
left join source_wars d on a.war_id = d.war_id
left join dyad_counts e on a.war_id = e.war_id;
