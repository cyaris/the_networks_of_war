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
    b.terrorism_deaths,
    c.mid_dyads,
    c.mid_dyads_initiated,
    c.mid_dyads_targeted,
    c.mid_dyads_joined,
    d.allied_countries,
    e.trade_countries,
    e.money_flow_in,
    e.money_flow_out,
    f.imports,
    f.exports,
    g.military_expenditure,
    g.military_personnel,
    g.iron_steel_production,
    g.energy_consumption,
    g.population,
    g.urban_population,
    g.urban_population_growth_rate,
    if(g.military_expenditure is null or g.military_personnel is null or g.iron_steel_production is null or g.energy_consumption is null or g.population is null or g.urban_population is null, null, ((g.military_expenditure / nullif(sum(g.military_expenditure) over (partition by a.year), 0)) + (g.military_personnel / nullif(sum(g.military_personnel) over (partition by a.year), 0)) + (g.iron_steel_production / nullif(sum(g.iron_steel_production) over (partition by a.year), 0)) + (g.energy_consumption / nullif(sum(g.energy_consumption) over (partition by a.year), 0)) + (g.population / nullif(sum(g.population) over (partition by a.year), 0)) + (g.urban_population / nullif(sum(g.urban_population) over (partition by a.year), 0))) / 6) cinc_score,
    h.arms_technologies_used,
    i.co2_emissions_per_capita,
    j.land_mass_exchange_gain,
    j.population_exchange_gain,
    j.land_mass_exchange_loss,
    j.population_exchange_loss,
    l.refugees_originated,
    l.refugees_hosted,
    l.internally_displaced_persons
from country_year_keys a
join country_codes k on a.c_code = k.c_code
left join terrorism_country_years b on a.c_code = b.c_code
                                    and a.year = b.year
left join mid_country_years c on a.c_code = c.c_code
                              and a.year = c.year
left join alliance_country_years d on a.c_code = d.c_code
                                   and a.year = d.year
left join dyadic_trade_country_years e on a.c_code = e.c_code
                                       and a.year = e.year
left join national_trade_country_years f on a.c_code = f.c_code
                                         and a.year = f.year
left join national_capability_country_year_rows g on a.c_code = g.c_code
                                                   and a.year = g.year
left join arms_technology_country_years h on a.c_code = h.c_code
                                          and a.year = h.year
left join co2_country_years i on a.c_code = i.c_code
                              and a.year = i.year
left join territorial_change_country_years j on a.c_code = j.c_code
                                             and a.year = j.year
left join displaced_population_country_years l on a.c_code = l.c_code
                                               and a.year = l.year;
