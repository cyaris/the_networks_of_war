select
    'graphData.json' file_name,
    count(*) war_count,
    json_pretty(to_json(struct_pack(
        source := struct_pack(
            database := 'backend/the_networks_of_war.duckdb',
            tables := json('["final_wars","final_participants","final_dyads"]'),
            notes := 'This static snapshot uses final Step 3 tables.'
        ),
        wars := json(to_json(array_agg(struct_pack(
            war_id := war_id,
            war_name := war_name,
            war_type_id := war_type_id,
            war_type := war_type,
            war_subtype := war_subtype,
            total_participants := total_participants,
            total_dyads := total_dyads,
            start_date := start_date,
            end_date := end_date,
            start_year := start_year,
            end_year := end_year,
            ongoing_war := ongoing_war,
            start_date_estimated := start_date_estimated,
            end_date_estimated := end_date_estimated,
            lagging_war := lagging_war,
            leading_war := leading_war,
            total_days_in_war := total_days_in_war
        ) order by start_year nulls last, war_id))),
        graphsByWarId := json(json_group_object(
            if(war_id = floor(war_id), war_id::bigint::varchar, war_id::varchar),
            to_json(struct_pack(
                nodes := json_extract(graph_json, '$.nodes'),
                links := json_extract(graph_json, '$.links')
            ))
        ))
    ))) graph_data_json
from final_wars
