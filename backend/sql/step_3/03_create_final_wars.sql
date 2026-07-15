create or replace table final_wars as

with

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
group by 1)

select
    a.war_num,
    a.war_name,
    a.war_type war_type_code,
    a.war_type_name war_type,
    a.war_subtype,
    a.total_participants,
    a.total_dyads,
    a.start_date,
    a.end_date,
    extract(year from a.start_date)::integer start_year,
    extract(year from a.end_date)::integer end_year,
    a.ongoing_war ongoing_conflict,
    a.start_date_estimated,
    a.end_date_estimated,
    a.lagging_war,
    a.leading_war,
    date_diff('day', a.start_date, a.end_date) total_days_in_war,
    json_object(
        'war',
        json(to_json([struct_pack(
            war_num := a.war_num,
            war_name := a.war_name,
            war_type_code := a.war_type,
            war_type := a.war_type_name,
            war_subtype := a.war_subtype,
            total_participants := a.total_participants,
            total_dyads := a.total_dyads,
            start_date := a.start_date,
            end_date := a.end_date,
            start_year := extract(year from a.start_date)::integer,
            end_year := extract(year from a.end_date)::integer,
            ongoing_conflict := a.ongoing_war,
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
left join node_json b on a.war_num = b.war_num
left join link_json c on a.war_num = c.war_num;
