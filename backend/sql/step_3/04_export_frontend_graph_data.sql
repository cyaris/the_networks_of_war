with

war_json as (

select
    to_json(array_agg(struct_pack(
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
    ) order by start_year nulls last, war_id)) payload,
    count(*) war_count
from final_wars),

available_node_fields as (

select
    war_id,
    field
from final_participants
unpivot include nulls (value for field in (columns('.*_[xyz]$')))
group by 1, 2
having
    max(greatest(coalesce(value, 0), 0)) > 0
    and count(*) filter (where value is null)::double / count(*) < 0.5
    and count(distinct greatest(coalesce(value, 0), 0)) > 1),

node_field_json as (

select
    a.war_id,
    a.id,
    to_json(map(list(a.field order by a.field), list(a.value order by a.field))) payload
from final_participants
unpivot include nulls (value for field in (columns('.*_[xyz]$'))) a
join available_node_fields b on a.war_id = b.war_id
                             and a.field = b.field
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
left join node_field_json b on a.war_id = b.war_id
                            and a.id = b.id),

node_json as (

select
    war_id,
    to_json(array_agg(payload order by id)) payload
from node_rows
group by 1),

available_link_fields as (

select
    war_id,
    field
from final_dyads a
unpivot include nulls (value for field in (columns('.*_[xyz]$')))
group by 1, 2
having max(if(value > 0, 1, 0)) = 1),

link_field_json as (

select
    a.war_id,
    a.source,
    a.target,
    to_json(map(list(a.field order by a.field), list(a.value order by a.field))) payload
from final_dyads
unpivot include nulls (value for field in (columns('.*_[xyz]$'))) a
join available_link_fields b on a.war_id = b.war_id
                             and a.field = b.field
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
left join link_field_json b on a.war_id = b.war_id
                            and a.source = b.source
                            and a.target = b.target),

link_json as (

select
    war_id,
    to_json(array_agg(payload order by source, target)) payload
from link_rows
group by 1),

graph_json as (

select
    json_group_object(
        if(a.war_id = floor(a.war_id), a.war_id::bigint::varchar, a.war_id::varchar),
        to_json(struct_pack(nodes := json(coalesce(b.payload, '[]')), links := json(coalesce(c.payload, '[]'))))
    ) payload
from final_wars a
left join node_json b on a.war_id = b.war_id
left join link_json c on a.war_id = c.war_id)

select
    'graphData.json' file_name,
    a.war_count,
    to_json(struct_pack(
        source := struct_pack(
            database := 'backend/the_networks_of_war.duckdb',
            tables := json('["final_wars","final_participants","final_dyads"]'),
            notes := 'This static snapshot uses final Step 3 tables.'
        ),
        wars := json(a.payload),
        graphsByWarId := json(b.payload)
    )) graph_data_json
from war_json a
cross join graph_json b
