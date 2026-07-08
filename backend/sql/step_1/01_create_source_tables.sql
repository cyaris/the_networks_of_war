create or replace table source_country_codes as

select *
from read_csv_auto({country_codes_path}, normalize_names = false)
limit 0;

create or replace table source_directed_dyadic_war as

select *
from read_csv_auto({directed_dyadic_war_path}, normalize_names = false, encoding = 'latin-1')
limit 0;

create or replace table source_dyadic_mid as

select *
from read_csv_auto({dyadic_mid_path}, normalize_names = false, encoding = 'latin-1')
limit 0;

create or replace table source_extrastate_wars as

select *
from read_csv_auto({extrastate_wars_path}, normalize_names = false)
limit 0;

create or replace table source_interstate_wars as

select *
from read_csv_auto({interstate_wars_path}, normalize_names = false, encoding = 'latin-1')
limit 0;

create or replace table source_intrastate_wars as

select *
from read_csv_auto({intrastate_wars_path}, normalize_names = false, encoding = 'latin-1')
limit 0;

create or replace table source_war_types as

select *
from read_csv_auto({war_types_path}, normalize_names = false)
limit 0;
