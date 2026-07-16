create or replace table participant_descriptives as

select
    a.war_id,
    a.war_name,
    a.c_code,
    a.participant,
    'First Year' timeframe,
    round(avg(terrorism_deaths), 2) terrorism_deaths,
    round(avg(mid_dyads), 2) mid_dyads,
    round(avg(mid_dyads_initiated), 2) mid_dyads_initiated,
    round(avg(mid_dyads_targeted), 2) mid_dyads_targeted,
    round(avg(mid_dyads_joined), 2) mid_dyads_joined,
    round(avg(allied_countries), 2) allied_countries,
    round(avg(trade_countries), 2) trade_countries,
    round(avg(money_flow_in), 2) money_flow_in,
    round(avg(money_flow_out), 2) money_flow_out,
    round(avg(imports), 2) imports,
    round(avg(exports), 2) exports,
    round(avg(military_expenditure), 2) military_expenditure,
    round(avg(military_personnel), 2) military_personnel,
    round(avg(iron_steel_production), 2) iron_steel_production,
    round(avg(energy_consumption), 2) energy_consumption,
    round(avg(population), 2) population,
    round(avg(urban_population), 2) urban_population,
    avg(urban_population_growth_rate) urban_population_growth_rate,
    avg(cinc_score) cinc_score,
    round(greatest(avg(land_mass_exchange_gain), 0), 2) land_mass_exchange_gain,
    round(greatest(avg(population_exchange_gain), 0), 2) population_exchange_gain,
    round(greatest(avg(land_mass_exchange_loss), 0), 2) land_mass_exchange_loss,
    round(greatest(avg(population_exchange_loss), 0), 2) population_exchange_loss,
    round(avg(refugees_originated), 2) refugees_originated,
    round(avg(refugees_hosted), 2) refugees_hosted,
    round(avg(internally_displaced_persons), 2) internally_displaced_persons,
    round(avg(concurrent_wars), 2) concurrent_wars
from participant_year_descriptives a
join wars b on a.war_id = b.war_id
where
    a.start_year = a.year
    and extract(year from b.start_date)::integer != extract(year from b.end_date)::integer
group by 1, 2, 3, 4, 5
union all
select
    a.war_id,
    a.war_name,
    a.c_code,
    a.participant,
    'Last Year' timeframe,
    round(avg(terrorism_deaths), 2) terrorism_deaths,
    round(avg(mid_dyads), 2) mid_dyads,
    round(avg(mid_dyads_initiated), 2) mid_dyads_initiated,
    round(avg(mid_dyads_targeted), 2) mid_dyads_targeted,
    round(avg(mid_dyads_joined), 2) mid_dyads_joined,
    round(avg(allied_countries), 2) allied_countries,
    round(avg(trade_countries), 2) trade_countries,
    round(avg(money_flow_in), 2) money_flow_in,
    round(avg(money_flow_out), 2) money_flow_out,
    round(avg(imports), 2) imports,
    round(avg(exports), 2) exports,
    round(avg(military_expenditure), 2) military_expenditure,
    round(avg(military_personnel), 2) military_personnel,
    round(avg(iron_steel_production), 2) iron_steel_production,
    round(avg(energy_consumption), 2) energy_consumption,
    round(avg(population), 2) population,
    round(avg(urban_population), 2) urban_population,
    avg(urban_population_growth_rate) urban_population_growth_rate,
    avg(cinc_score) cinc_score,
    round(greatest(avg(land_mass_exchange_gain), 0), 2) land_mass_exchange_gain,
    round(greatest(avg(population_exchange_gain), 0), 2) population_exchange_gain,
    round(greatest(avg(land_mass_exchange_loss), 0), 2) land_mass_exchange_loss,
    round(greatest(avg(population_exchange_loss), 0), 2) population_exchange_loss,
    round(avg(refugees_originated), 2) refugees_originated,
    round(avg(refugees_hosted), 2) refugees_hosted,
    round(avg(internally_displaced_persons), 2) internally_displaced_persons,
    round(avg(concurrent_wars), 2) concurrent_wars
from participant_year_descriptives a
join wars b on a.war_id = b.war_id
where
    a.end_year = a.year
    and extract(year from b.start_date)::integer != extract(year from b.end_date)::integer
group by 1, 2, 3, 4, 5
union all
select
    war_id,
    war_name,
    c_code,
    participant,
    'All Years' timeframe,
    round(avg(terrorism_deaths), 2) terrorism_deaths,
    round(avg(mid_dyads), 2) mid_dyads,
    round(avg(mid_dyads_initiated), 2) mid_dyads_initiated,
    round(avg(mid_dyads_targeted), 2) mid_dyads_targeted,
    round(avg(mid_dyads_joined), 2) mid_dyads_joined,
    round(avg(allied_countries), 2) allied_countries,
    round(avg(trade_countries), 2) trade_countries,
    round(avg(money_flow_in), 2) money_flow_in,
    round(avg(money_flow_out), 2) money_flow_out,
    round(avg(imports), 2) imports,
    round(avg(exports), 2) exports,
    round(avg(military_expenditure), 2) military_expenditure,
    round(avg(military_personnel), 2) military_personnel,
    round(avg(iron_steel_production), 2) iron_steel_production,
    round(avg(energy_consumption), 2) energy_consumption,
    round(avg(population), 2) population,
    round(avg(urban_population), 2) urban_population,
    avg(urban_population_growth_rate) urban_population_growth_rate,
    avg(cinc_score) cinc_score,
    round(greatest(avg(land_mass_exchange_gain), 0), 2) land_mass_exchange_gain,
    round(greatest(avg(population_exchange_gain), 0), 2) population_exchange_gain,
    round(greatest(avg(land_mass_exchange_loss), 0), 2) land_mass_exchange_loss,
    round(greatest(avg(population_exchange_loss), 0), 2) population_exchange_loss,
    round(avg(refugees_originated), 2) refugees_originated,
    round(avg(refugees_hosted), 2) refugees_hosted,
    round(avg(internally_displaced_persons), 2) internally_displaced_persons,
    round(avg(concurrent_wars), 2) concurrent_wars
from participant_year_descriptives
group by 1, 2, 3, 4, 5;
