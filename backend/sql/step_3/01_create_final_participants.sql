create or replace table final_participants as

with

participant_rows as (

select
    row_number() over (partition by a.war_id order by a.side nulls last, a.c_code, a.participant) - 1 id,
    a.war_id,
    b.war_name,
    b.war_type_id,
    b.war_type,
    b.war_subtype,
    a.c_code,
    a.participant,
    a.side,
    if(a.c_code > 0, a.c_code::varchar, a.participant) node_key,
    a.battle_deaths,
    a.battle_deaths_estimated,
    a.start_date,
    if(b.ongoing_war = 1, null, a.end_date) end_date,
    extract(year from a.start_date)::integer start_year,
    if(b.ongoing_war = 1, null, extract(year from a.end_date)::integer) end_year,
    b.ongoing_war,
    a.start_date_estimated,
    a.end_date_estimated,
    b.lagging_war,
    b.leading_war
from participants a
join wars b on a.war_id = b.war_id),

participant_timeframes as (

select
    war_id,
    timeframe
from participant_descriptives
group by 1, 2),

participant_descriptor_values as (

select
    b.war_id,
    b.c_code,
    b.participant,
    c.timeframe,
    if(columns('^(terrorism_deaths|mid_dyads|mid_dyads_initiated|mid_dyads_targeted|mid_dyads_joined|allied_countries|trade_countries|urban_population_growth_rate|cinc_score|arms_technologies_used|co2_emissions_per_capita|land_mass_exchange_gain|population_exchange_gain|land_mass_exchange_loss|population_exchange_loss|concurrent_wars)$') in (-9, -8), null, columns('^(terrorism_deaths|mid_dyads|mid_dyads_initiated|mid_dyads_targeted|mid_dyads_joined|allied_countries|trade_countries|urban_population_growth_rate|cinc_score|arms_technologies_used|co2_emissions_per_capita|land_mass_exchange_gain|population_exchange_gain|land_mass_exchange_loss|population_exchange_loss|concurrent_wars)$')),
    if(columns('^(money_flow_in|money_flow_out|imports|exports)$') in (-9, -8), null, columns('^(money_flow_in|money_flow_out|imports|exports)$') * 1000000),
    if(columns('^(military_expenditure|military_personnel|population|urban_population|refugees_originated|refugees_hosted|internally_displaced_persons)$') in (-9, -8), null, columns('^(military_expenditure|military_personnel|population|urban_population|refugees_originated|refugees_hosted|internally_displaced_persons)$') * 1000),
    if(columns('^(iron_steel_production|energy_consumption)$') in (-9, -8), null, columns('^(iron_steel_production|energy_consumption)$') * 1000)
from participant_rows b
join participant_timeframes c on b.war_id = c.war_id
left join participant_descriptives a on b.war_id = a.war_id
                                     and b.c_code = a.c_code
                                     and b.participant = a.participant
                                     and c.timeframe = a.timeframe),

available_node_fields as (

select
    war_id,
    timeframe,
    field
from participant_descriptor_values
unpivot include nulls (value for field in (columns('^(terrorism_deaths|mid_dyads|mid_dyads_initiated|mid_dyads_targeted|mid_dyads_joined|allied_countries|trade_countries|money_flow_in|money_flow_out|imports|exports|military_expenditure|military_personnel|iron_steel_production|energy_consumption|population|urban_population|urban_population_growth_rate|cinc_score|arms_technologies_used|co2_emissions_per_capita|land_mass_exchange_gain|population_exchange_gain|land_mass_exchange_loss|population_exchange_loss|refugees_originated|refugees_hosted|internally_displaced_persons|concurrent_wars)$')))
group by 1, 2, 3
having
    max(greatest(coalesce(value, 0), 0)) > 0
    and count(*) filter (where value is null)::double / count(*) < 0.5
    and count(distinct greatest(coalesce(value, 0), 0)) > 1),

node_field_json as (

select
    a.war_id,
    a.c_code,
    a.participant,
    a.timeframe,
    to_json(map(list(a.field order by a.field), list(a.value order by a.field))) payload
from participant_descriptor_values
unpivot include nulls (value for field in (columns('^(terrorism_deaths|mid_dyads|mid_dyads_initiated|mid_dyads_targeted|mid_dyads_joined|allied_countries|trade_countries|money_flow_in|money_flow_out|imports|exports|military_expenditure|military_personnel|iron_steel_production|energy_consumption|population|urban_population|urban_population_growth_rate|cinc_score|arms_technologies_used|co2_emissions_per_capita|land_mass_exchange_gain|population_exchange_gain|land_mass_exchange_loss|population_exchange_loss|refugees_originated|refugees_hosted|internally_displaced_persons|concurrent_wars)$'))) a
join available_node_fields b on a.war_id = b.war_id
                             and a.timeframe = b.timeframe
                             and a.field = b.field
group by 1, 2, 3, 4),

node_descriptor_json as (

select
    war_id,
    c_code,
    participant,
    json_group_object(lower(replace(timeframe, ' ', '_')), json(payload)) payload
from node_field_json
group by 1, 2, 3)

select
    a.*,
    b.payload descriptor_timeframes
from participant_rows a
left join node_descriptor_json b on a.war_id = b.war_id
                                 and a.c_code = b.c_code
                                 and a.participant = b.participant;
