with

war_json as (

select
    to_json(array_agg(struct_pack(
        war_num := war_num,
        war_name := war_name,
        war_type_code := war_type_code,
        war_type := war_type,
        war_subtype := war_subtype,
        total_participants := total_participants,
        total_dyads := total_dyads,
        start_date := start_date,
        end_date := end_date,
        start_year := start_year,
        end_year := end_year,
        ongoing_conflict := ongoing_conflict,
        start_date_estimated := start_date_estimated,
        end_date_estimated := end_date_estimated,
        lagging_war := lagging_war,
        leading_war := leading_war,
        total_days_in_war := total_days_in_war
    ) order by start_year nulls last, war_num)) payload,
    count(*) war_count
from final_wars),

node_json as (

select
    war_num,
    to_json(array_agg(a order by id)) payload
from final_participants a
group by 1),

link_json as (

select
    war_num,
    to_json(array_agg(a order by source, target)) payload
from final_dyads a
group by 1),

graph_json as (

select
    json_group_object(
        if(a.war_num = floor(a.war_num), a.war_num::bigint::varchar, a.war_num::varchar),
        json_object('nodes', json(coalesce(b.payload, '[]')), 'links', json(coalesce(c.payload, '[]')))
    ) payload
from final_wars a
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
