create or replace table country_year_descriptives as

with

terrorism_country_years as (

select
    c.c_code,
    a.year,
    sum(if(a.killed >= 0, a.killed, null)) terrorism_deaths
from source_global_terrorism_database a
left join participant_name_replacements b on a.country_name = b.source
left join source_country_codes c on if(a.country_name = 'Hong Kong' and a.year <= 1997, 'United Kingdom', coalesce(b.replacement, a.country_name)) = c.state_name
group by 1, 2),

mid_country_year_rows as (

select
    source_year "year",
    c_code_a c_code,
    dyindex,
    role_a participant_role
from source_interstate_mid_dyads
group by 1, 2, 3, 4
union
select
    source_year "year",
    c_code_b c_code,
    dyindex,
    role_b participant_role
from source_interstate_mid_dyads
group by 1, 2, 3, 4),

mid_country_years as (

select
    year,
    c_code,
    count(distinct dyindex) mid_dyads,
    count(distinct dyindex) filter (where participant_role = 1) mid_dyads_initiated,
    count(distinct dyindex) filter (where participant_role = 3) mid_dyads_targeted,
    count(distinct dyindex) filter (where participant_role in (2, 4)) mid_dyads_joined
from mid_country_year_rows
where c_code is not null
group by 1, 2),

alliance_country_year_rows as (

select
    year,
    c_code_a,
    c_code_b
from source_formal_alliances_directed_yearly
where coalesce(defense, 0) + coalesce(neutrality, 0) + coalesce(nonaggression, 0) + coalesce(entente, 0) > 0
group by 1, 2, 3
union
select
    year,
    c_code_b c_code_a,
    c_code_a c_code_b
from source_formal_alliances_directed_yearly
where coalesce(defense, 0) + coalesce(neutrality, 0) + coalesce(nonaggression, 0) + coalesce(entente, 0) > 0
group by 1, 2, 3),

alliance_country_years as (

select
    year,
    c_code_a c_code,
    count(distinct c_code_b) allied_countries
from alliance_country_year_rows
group by 1, 2),

dyadic_trade_country_year_rows as (

select
    year,
    c_code_a,
    c_code_b,
    if(flow_1 >= 0, flow_1, null) money_flow_in,
    if(flow_2 >= 0, flow_2, null) money_flow_out
from source_cow_trade_dyadic
where flow_1 > 0 or flow_2 > 0
group by 1, 2, 3, 4, 5
union
select
    year,
    c_code_b c_code_a,
    c_code_a c_code_b,
    if(flow_2 >= 0, flow_2, null) money_flow_in,
    if(flow_1 >= 0, flow_1, null) money_flow_out
from source_cow_trade_dyadic
where flow_1 > 0 or flow_2 > 0
group by 1, 2, 3, 4, 5),

dyadic_trade_country_years as (

select
    year,
    c_code_a c_code,
    if(count(*) filter (where money_flow_in is null) > 0, null, sum(money_flow_in)) money_flow_in,
    if(count(*) filter (where money_flow_out is null) > 0, null, sum(money_flow_out)) money_flow_out,
    count(distinct c_code_b) trade_countries
from dyadic_trade_country_year_rows
group by 1, 2),

national_trade_country_years as (

select
    year,
    c_code,
    if(count(*) filter (where imports is null or imports < 0) > 0, null, sum(greatest(imports, 0))) imports,
    if(count(*) filter (where exports is null or exports < 0) > 0, null, sum(greatest(exports, 0))) exports
from source_cow_trade_national
group by 1, 2),

national_capability_country_year_rows as (

select
    year,
    c_code,
    any_value(if(military_expenditures in (-9, -8), null, military_expenditures)) military_expenditure,
    any_value(if(military_personnel in (-9, -8), null, military_personnel)) military_personnel,
    any_value(if(iron_and_steel_production in (-9, -8), null, iron_and_steel_production)) iron_steel_production,
    any_value(if(primary_energy_consumption in (-9, -8), null, primary_energy_consumption)) energy_consumption,
    any_value(if(total_population in (-9, -8), null, total_population)) population,
    any_value(if(urban_population in (-9, -8), null, urban_population)) urban_population,
    any_value(if(urban_population_growth in (-9, -8), null, urban_population_growth)) urban_population_growth_rate
from source_national_material_capabilities
group by 1, 2),

arms_technology_country_years as (

select
    year,
    c_code,
    count(*) filter (where used in (1, 9)) arms_technologies_used
from source_arms_technology
where
    c_code > 0
    and technology_name != 'Adopted technologies'
group by 1, 2),

co2_country_years as (

select
    c.c_code,
    a.year,
    avg(a.co2_emissions_per_capita) co2_emissions_per_capita
from source_co_emissions_per_capita a
left join participant_name_replacements b on a.country_name = b.source
join country_codes c on coalesce(b.replacement, a.country_name) = c.state_name
where a.co2_emissions_per_capita is not null
group by 1, 2),

territorial_change_country_year_rows as (

select
    year,
    gainer c_code,
    if(count(*) filter (where area is null or area < 0) > 0, null, sum(greatest(area, 0))) land_mass_exchange_gain,
    if(count(*) filter (where area is null or area < 0) > 0, null, sum(greatest(area, 0))) * -1 land_mass_exchange_loss,
    if(count(*) filter (where population is null or population < 0) > 0, null, sum(greatest(population, 0))) population_exchange_gain,
    if(count(*) filter (where population is null or population < 0) > 0, null, sum(greatest(population, 0))) * -1 population_exchange_loss
from source_territorial_changes
group by 1, 2
union
select
    year,
    loser c_code,
    if(count(*) filter (where area is null or area < 0) > 0, null, sum(greatest(area, 0))) * -1 land_mass_exchange_gain,
    if(count(*) filter (where area is null or area < 0) > 0, null, sum(greatest(area, 0))) land_mass_exchange_loss,
    if(count(*) filter (where population is null or population < 0) > 0, null, sum(greatest(population, 0))) * -1 population_exchange_gain,
    if(count(*) filter (where population is null or population < 0) > 0, null, sum(greatest(population, 0))) population_exchange_loss
from source_territorial_changes
group by 1, 2),

territorial_change_country_years as (

select
    year,
    c_code,
    sum(land_mass_exchange_gain) land_mass_exchange_gain,
    sum(land_mass_exchange_loss) land_mass_exchange_loss,
    sum(population_exchange_gain) population_exchange_gain,
    sum(population_exchange_loss) population_exchange_loss
from territorial_change_country_year_rows
group by 1, 2),

displaced_population_country_years as (

select
    year,
    c_code,
    if(count(*) filter (where source is null) > 0, null, sum(source)) refugees_originated,
    if(count(*) filter (where hosted_refugees is null) > 0, null, sum(hosted_refugees)) refugees_hosted,
    if(count(*) filter (where internally_displaced_persons is null) > 0, null, sum(internally_displaced_persons)) internally_displaced_persons
from source_forcibly_displaced_populations
group by 1, 2),

country_year_keys as (

select
    c_code,
    year
from terrorism_country_years
union
select
    c_code,
    year
from mid_country_years
union
select
    c_code,
    year
from alliance_country_years
union
select
    c_code,
    year
from dyadic_trade_country_years
union
select
    c_code,
    year
from national_trade_country_years
union
select
    c_code,
    year
from national_capability_country_year_rows
union
select
    c_code,
    year
from arms_technology_country_years
union
select
    c_code,
    year
from co2_country_years
union
select
    c_code,
    year
from territorial_change_country_years
union
select
    c_code,
    year
from displaced_population_country_years)

select
    a.c_code,
    a.year,
    c.terrorism_deaths,
    d.mid_dyads,
    d.mid_dyads_initiated,
    d.mid_dyads_targeted,
    d.mid_dyads_joined,
    e.allied_countries,
    f.trade_countries,
    f.money_flow_in,
    f.money_flow_out,
    g.imports,
    g.exports,
    h.military_expenditure,
    h.military_personnel,
    h.iron_steel_production,
    h.energy_consumption,
    h.population,
    h.urban_population,
    h.urban_population_growth_rate,
    if(h.military_expenditure is null or h.military_personnel is null or h.iron_steel_production is null or h.energy_consumption is null or h.population is null or h.urban_population is null, null, ((h.military_expenditure / nullif(sum(h.military_expenditure) over (partition by a.year), 0)) + (h.military_personnel / nullif(sum(h.military_personnel) over (partition by a.year), 0)) + (h.iron_steel_production / nullif(sum(h.iron_steel_production) over (partition by a.year), 0)) + (h.energy_consumption / nullif(sum(h.energy_consumption) over (partition by a.year), 0)) + (h.population / nullif(sum(h.population) over (partition by a.year), 0)) + (h.urban_population / nullif(sum(h.urban_population) over (partition by a.year), 0))) / 6) cinc_score,
    i.arms_technologies_used,
    j.co2_emissions_per_capita,
    k.land_mass_exchange_gain,
    k.population_exchange_gain,
    k.land_mass_exchange_loss,
    k.population_exchange_loss,
    l.refugees_originated,
    l.refugees_hosted,
    l.internally_displaced_persons
from country_year_keys a
join country_codes b on a.c_code = b.c_code
left join terrorism_country_years c on a.c_code = c.c_code
                                    and a.year = c.year
left join mid_country_years d on a.c_code = d.c_code
                              and a.year = d.year
left join alliance_country_years e on a.c_code = e.c_code
                                   and a.year = e.year
left join dyadic_trade_country_years f on a.c_code = f.c_code
                                       and a.year = f.year
left join national_trade_country_years g on a.c_code = g.c_code
                                         and a.year = g.year
left join national_capability_country_year_rows h on a.c_code = h.c_code
                                                  and a.year = h.year
left join arms_technology_country_years i on a.c_code = i.c_code
                                          and a.year = i.year
left join co2_country_years j on a.c_code = j.c_code
                              and a.year = j.year
left join territorial_change_country_years k on a.c_code = k.c_code
                                             and a.year = k.year
left join displaced_population_country_years l on a.c_code = l.c_code
                                               and a.year = l.year;
