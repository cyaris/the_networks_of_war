create or replace table final_participants as

select
    row_number() over (partition by a.war_num order by a.side nulls last, a.c_code, a.participant) - 1 id,
    a.war_num,
    b.war_name,
    b.war_type war_type_code,
    b.war_type_name war_type,
    b.war_subtype,
    a.c_code,
    a.participant,
    a.side,
    if(a.c_code > 0, a.c_code::varchar, a.participant) node_key,
    a.battle_deaths,
    a.battle_deaths_estimated,
    a.start_date,
    if(b.ongoing_war = 1, null, a.end_date) end_date,
    a.start_year,
    if(b.ongoing_war = 1, null, a.end_year) end_year,
    b.ongoing_war ongoing_conflict,
    a.start_date_estimated,
    a.end_date_estimated,
    b.lagging_war,
    b.leading_war,
    if(columns('^(terrorism_deaths|mid_dyads|mid_dyads_initiated|mid_dyads_targeted|mid_dyads_joined|allied_countries|trade_countries|imports|exports|urban_population_growth_rate|cinc_score|land_mass_exchange_gain|population_exchange_gain|land_mass_exchange_loss|population_exchange_loss|concurrent_wars)_[xyz]$') in (-9, -8) or (columns('^(terrorism_deaths|mid_dyads|mid_dyads_initiated|mid_dyads_targeted|mid_dyads_joined|allied_countries|trade_countries|imports|exports|urban_population_growth_rate|cinc_score|land_mass_exchange_gain|population_exchange_gain|land_mass_exchange_loss|population_exchange_loss|concurrent_wars)_[xyz]$') is null and a.c_code <= 0), null, coalesce(columns('^(terrorism_deaths|mid_dyads|mid_dyads_initiated|mid_dyads_targeted|mid_dyads_joined|allied_countries|trade_countries|imports|exports|urban_population_growth_rate|cinc_score|land_mass_exchange_gain|population_exchange_gain|land_mass_exchange_loss|population_exchange_loss|concurrent_wars)_[xyz]$'), 0)),
    if(columns('^(money_flow_in|money_flow_out)_[xyz]$') in (-9, -8) or (columns('^(money_flow_in|money_flow_out)_[xyz]$') is null and a.c_code <= 0), null, coalesce(columns('^(money_flow_in|money_flow_out)_[xyz]$'), 0) * 1000000),
    if(columns('^(military_expenditure|military_personnel|population|urban_population|refugees_originated|refugees_hosted|internally_displaced_persons)_[xyz]$') in (-9, -8) or (columns('^(military_expenditure|military_personnel|population|urban_population|refugees_originated|refugees_hosted|internally_displaced_persons)_[xyz]$') is null and a.c_code <= 0), null, coalesce(columns('^(military_expenditure|military_personnel|population|urban_population|refugees_originated|refugees_hosted|internally_displaced_persons)_[xyz]$'), 0) * 1000),
    if(columns('^(iron_steel_production|energy_consumption)_[xyz]$') in (-9, -8) or (columns('^(iron_steel_production|energy_consumption)_[xyz]$') is null and a.c_code <= 0), null, coalesce(columns('^(iron_steel_production|energy_consumption)_[xyz]$'), 0) * 2000000)
from participant_descriptives a
join wars b on a.war_num = b.war_num;
