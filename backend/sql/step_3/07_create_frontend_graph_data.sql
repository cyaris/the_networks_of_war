create or replace table frontend_graph_data as

with

frontend_wars as (

select
    war_num,
    war_name,
    war_type_code,
    war_type,
    war_subtype,
    total_participants,
    total_dyads,
    start_date,
    end_date,
    start_year,
    end_year,
    ongoing_conflict,
    start_date_estimated,
    end_date_estimated,
    lagging_war,
    leading_war,
    total_days_in_war,
    file_name
from final_wars),

frontend_war_nodes as (

select
    war_num,
    id,
    c_code,
    participant,
    side,
    battle_deaths,
    battle_deaths_estimated,
    start_year,
    end_year,
    ongoing_conflict,
    node_key
from d3_war_nodes),

frontend_war_links as (

select
    war_num,
    "source",
    target,
    start_date_estimated,
    end_date_estimated
from d3_war_links),

war_json as (

select
    to_json(array_agg(a order by start_year nulls last, war_num)) payload,
    count(*) war_count
from frontend_wars a),

node_json as (

select
    war_num,
    to_json(array_agg(struct_pack(
        id := id,
        c_code := c_code,
        participant := participant,
        side := side,
        battle_deaths := battle_deaths,
        battle_deaths_estimated := battle_deaths_estimated,
        start_year := start_year,
        end_year := end_year,
        ongoing_conflict := ongoing_conflict,
        node_key := node_key
    ) order by id)) payload
from frontend_war_nodes
group by 1),

link_json as (

select
    war_num,
    to_json(array_agg(struct_pack(
        "source" := "source",
        target := target,
        start_date_estimated := start_date_estimated,
        end_date_estimated := end_date_estimated
    ) order by "source", target)) payload
from frontend_war_links
group by 1),

graph_json as (

select
    json_group_object(
        a.war_num::varchar,
        json_object('nodes', json(coalesce(b.payload, '[]')), 'links', json(coalesce(c.payload, '[]')))
    ) payload
from frontend_wars a
left join node_json b on a.war_num = b.war_num
left join link_json c on a.war_num = c.war_num)

select
    'graphData.json' file_name,
    a.war_count,
    json_object(
        'source',
        json_object(
            'database',
            'backend/the_networks_of_war.duckdb',
            'tables',
            json('["final_wars","d3_war_nodes","d3_war_links"]'),
            'notes',
            'd3_war_json remains available for API-by-war_num loading; this static snapshot uses normalized Step 3 tables.'
        ),
        'wars',
        json(a.payload),
        'graphsByWarNum',
        json(b.payload)
    ) graph_data_json
from war_json a
cross join graph_json b;
