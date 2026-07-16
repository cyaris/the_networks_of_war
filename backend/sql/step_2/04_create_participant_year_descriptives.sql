create or replace table participant_year_descriptives as

with

participant_years as (

select
    a.war_id,
    b.war_name,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    extract(year from a.start_date)::integer start_year,
    extract(year from a.end_date)::integer end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths,
    a.battle_deaths_estimated,
    c.range::integer "year"
from participants a
join wars b on a.war_id = b.war_id
join range(1500, 2100) c on c.range between extract(year from a.start_date)::integer and extract(year from a.end_date)::integer),

concurrent_wars as (

select
    year,
    c_code,
    count(distinct war_id) - 1 concurrent_wars
from participant_years
where c_code > 0
group by 1, 2
having count(distinct war_id) - 1 > 0)

select
    a.war_id,
    a.war_name,
    a.c_code,
    a.participant,
    a.side,
    a.start_date,
    a.end_date,
    a.start_year,
    a.end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    a.battle_deaths,
    a.battle_deaths_estimated,
    a.year,
    b.terrorism_deaths,
    b.mid_dyads,
    b.mid_dyads_initiated,
    b.mid_dyads_targeted,
    b.mid_dyads_joined,
    b.allied_countries,
    b.trade_countries,
    b.money_flow_in,
    b.money_flow_out,
    b.imports,
    b.exports,
    b.military_expenditure,
    b.military_personnel,
    b.iron_steel_production,
    b.energy_consumption,
    b.population,
    b.urban_population,
    b.urban_population_growth_rate,
    b.cinc_score,
    b.co2_emissions_per_capita,
    b.land_mass_exchange_gain,
    b.population_exchange_gain,
    b.land_mass_exchange_loss,
    b.population_exchange_loss,
    b.refugees_originated,
    b.refugees_hosted,
    b.internally_displaced_persons,
    coalesce(c.concurrent_wars, 0) concurrent_wars
from participant_years a
left join country_year_descriptives b on a.c_code = b.c_code
                                      and a.year = b.year
left join concurrent_wars c on a.c_code = c.c_code
                            and a.year = c.year
where a.c_code > 0;
