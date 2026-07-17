create or replace table final_wars as

with

node_rows as (

select
    war_id,
    id,
    json_merge_patch(
        to_json(struct_pack(
            id := id,
            war_id := war_id,
            war_name := war_name,
            war_type_id := war_type_id,
            war_type := war_type,
            war_subtype := war_subtype,
            c_code := c_code,
            participant := participant,
            side := side,
            node_key := node_key,
            battle_deaths := battle_deaths,
            battle_deaths_estimated := battle_deaths_estimated,
            start_date := start_date,
            end_date := end_date,
            start_year := start_year,
            end_year := end_year,
            ongoing_war := ongoing_war,
            start_date_estimated := start_date_estimated,
            end_date_estimated := end_date_estimated,
            lagging_war := lagging_war,
            leading_war := leading_war,
            metrics := coalesce(metric_timeframes, json('{{}}'))
        )),
        coalesce(descriptor_timeframes, json('{{}}'))
    ) payload
from final_participants),

node_json as (

select
    war_id,
    to_json(array_agg(payload order by id)) payload
from node_rows
group by 1),

link_rows as (

select
    war_id,
    source,
    target,
    json_merge_patch(
        to_json(struct_pack(
            war_id := war_id,
            war_name := war_name,
            c_code_a := c_code_a,
            c_code_b := c_code_b,
            participant_a := participant_a,
            participant_b := participant_b,
            start_date := start_date,
            end_date := end_date,
            start_year := start_year,
            end_year := end_year,
            start_date_estimated := start_date_estimated,
            end_date_estimated := end_date_estimated,
            source := source,
            target := target
        )),
        coalesce(descriptor_timeframes, json('{{}}'))
    ) payload
from final_dyads),

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
