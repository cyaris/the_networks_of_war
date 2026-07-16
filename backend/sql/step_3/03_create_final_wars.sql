create or replace table final_wars as

with

available_node_fields as (

select
    a.war_id,
    a.field original_field,
    regexp_replace(a.field, '_[xyz]$', '') field,
    if(right(a.field, 1) = 'z', 'all_years', if(right(a.field, 1) = 'x', 'first_year', 'last_year')) timeframe
from final_participants
unpivot include nulls (value for field in (columns('.*_[xyz]$'))) a
join wars b on a.war_id = b.war_id
where
    right(a.field, 1) = 'z'
    or extract(year from b.start_date)::integer != extract(year from b.end_date)::integer
group by 1, 2, 3, 4
having
    max(greatest(coalesce(value, 0), 0)) > 0
    and count(*) filter (where value is null)::double / count(*) < 0.5
    and count(distinct greatest(coalesce(value, 0), 0)) > 1),

node_field_json as (

select
    a.war_id,
    a.id,
    b.timeframe,
    to_json(map(list(b.field order by b.field), list(a.value order by b.field))) payload
from final_participants
unpivot include nulls (value for field in (columns('.*_[xyz]$'))) a
join available_node_fields b on a.war_id = b.war_id
                             and a.field = b.original_field
group by 1, 2, 3),

node_descriptor_json as (

select
    war_id,
    id,
    json_group_object(timeframe, json(payload)) payload
from node_field_json
group by 1, 2),

node_rows as (

select
    a.war_id,
    a.id,
    json_merge_patch(
        to_json(struct_pack(
            id := a.id,
            war_id := a.war_id,
            war_name := a.war_name,
            war_type_id := a.war_type_id,
            war_type := a.war_type,
            war_subtype := a.war_subtype,
            c_code := a.c_code,
            participant := a.participant,
            side := a.side,
            node_key := a.node_key,
            battle_deaths := a.battle_deaths,
            battle_deaths_estimated := a.battle_deaths_estimated,
            start_date := a.start_date,
            end_date := a.end_date,
            start_year := a.start_year,
            end_year := a.end_year,
            ongoing_war := a.ongoing_war,
            start_date_estimated := a.start_date_estimated,
            end_date_estimated := a.end_date_estimated,
            lagging_war := a.lagging_war,
            leading_war := a.leading_war
        )),
        coalesce(b.payload, json('{{}}'))
    ) payload
from final_participants a
left join node_descriptor_json b on a.war_id = b.war_id
                                and a.id = b.id),

node_json as (

select
    war_id,
    to_json(array_agg(payload order by id)) payload
from node_rows
group by 1),

available_link_fields as (

select
    a.war_id,
    a.field original_field,
    regexp_replace(a.field, '_[xyz]$', '') field,
    if(right(a.field, 1) = 'z', 'all_years', if(right(a.field, 1) = 'x', 'first_year', 'last_year')) timeframe
from final_dyads
unpivot include nulls (value for field in (columns('.*_[xyz]$'))) a
join wars b on a.war_id = b.war_id
where
    right(a.field, 1) = 'z'
    or extract(year from b.start_date)::integer != extract(year from b.end_date)::integer
group by 1, 2, 3, 4
having max(if(value > 0, 1, 0)) = 1),

link_field_json as (

select
    a.war_id,
    a.source,
    a.target,
    b.timeframe,
    to_json(map(list(b.field order by b.field), list(a.value order by b.field))) payload
from final_dyads
unpivot include nulls (value for field in (columns('.*_[xyz]$'))) a
join available_link_fields b on a.war_id = b.war_id
                             and a.field = b.original_field
group by 1, 2, 3, 4),

link_descriptor_json as (

select
    war_id,
    source,
    target,
    json_group_object(timeframe, json(payload)) payload
from link_field_json
group by 1, 2, 3),

link_rows as (

select
    a.war_id,
    a.source,
    a.target,
    json_merge_patch(
        to_json(struct_pack(
            war_id := a.war_id,
            war_name := a.war_name,
            c_code_a := a.c_code_a,
            c_code_b := a.c_code_b,
            participant_a := a.participant_a,
            participant_b := a.participant_b,
            start_date := a.start_date,
            end_date := a.end_date,
            start_year := a.start_year,
            end_year := a.end_year,
            start_date_estimated := a.start_date_estimated,
            end_date_estimated := a.end_date_estimated,
            source := a.source,
            target := a.target
        )),
        coalesce(b.payload, json('{{}}'))
    ) payload
from final_dyads a
left join link_descriptor_json b on a.war_id = b.war_id
                                and a.source = b.source
                                and a.target = b.target),

link_json as (

select
    war_id,
    to_json(array_agg(payload order by source, target)) payload
from link_rows
group by 1)

select
    a.war_id,
    a.war_name,
    a.war_type_id,
    a.war_type,
    a.war_subtype,
    a.total_participants,
    a.total_dyads,
    a.start_date,
    a.end_date,
    extract(year from a.start_date)::integer start_year,
    extract(year from a.end_date)::integer end_year,
    a.ongoing_war,
    a.start_date_estimated,
    a.end_date_estimated,
    a.lagging_war,
    a.leading_war,
    date_diff('day', a.start_date, a.end_date) total_days_in_war,
    json_object(
        'war',
        json(to_json([struct_pack(
            war_id := a.war_id,
            war_name := a.war_name,
            war_type_id := a.war_type_id,
            war_type := a.war_type,
            war_subtype := a.war_subtype,
            total_participants := a.total_participants,
            total_dyads := a.total_dyads,
            start_date := a.start_date,
            end_date := a.end_date,
            start_year := extract(year from a.start_date)::integer,
            end_year := extract(year from a.end_date)::integer,
            ongoing_war := a.ongoing_war,
            start_date_estimated := a.start_date_estimated,
            end_date_estimated := a.end_date_estimated,
            lagging_war := a.lagging_war,
            leading_war := a.leading_war,
            total_days_in_war := date_diff('day', a.start_date, a.end_date)
        )])),
        'nodes',
        json(coalesce(b.payload, '[]')),
        'links',
        json(coalesce(c.payload, '[]'))
    ) graph_json
from wars a
left join node_json b on a.war_id = b.war_id
left join link_json c on a.war_id = c.war_id;
