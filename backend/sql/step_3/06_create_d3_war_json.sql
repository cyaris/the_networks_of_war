create or replace table d3_war_json as

with

war_json as (

select
    war_num,
    any_value(file_name) file_name,
    to_json(array_agg(a order by war_num)) payload
from final_wars a
group by 1),

node_json as (

select
    war_num,
    to_json(array_agg(a order by id)) payload
from d3_war_nodes a
group by 1),

link_json as (

select
    war_num,
    to_json(array_agg(a order by "source", "target")) payload
from d3_war_links a
group by 1)

select
    a.war_num,
    a.file_name,
    json_object('war', json(a.payload), 'nodes', json(coalesce(b.payload, '[]')), 'links', json(coalesce(c.payload, '[]'))) graph_json
from war_json a
left join node_json b on a.war_num = b.war_num
left join link_json c on a.war_num = c.war_num;
