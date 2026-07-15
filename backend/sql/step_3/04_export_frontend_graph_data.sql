with

frontend_wars as (

select * exclude (graph_json)
from final_wars),

frontend_war_nodes as (

select *
from final_participants),

frontend_war_links as (

select *
from final_dyads),

war_json as (

select
    to_json(array_agg(a order by start_year nulls last, war_num)) payload,
    count(*) war_count
from frontend_wars a),

node_json as (

select
    war_num,
    to_json(array_agg(a order by id)) payload
from frontend_war_nodes a
group by 1),

link_json as (

select
    war_num,
    to_json(array_agg(a order by source, target)) payload
from frontend_war_links a
group by 1),

graph_json as (

select
    json_group_object(
        if(a.war_num = floor(a.war_num), a.war_num::bigint::varchar, a.war_num::varchar),
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
            json('["final_wars","final_participants","final_dyads"]'),
            'notes',
            'This static snapshot uses final Step 3 tables.'
        ),
        'wars',
        json(a.payload),
        'graphsByWarNum',
        json(b.payload)
    ) graph_data_json
from war_json a
cross join graph_json b
