insert into source_country_codes
select *
from read_csv_auto({country_codes_path}, normalize_names = false);

insert into source_directed_dyadic_war
select *
from read_csv_auto({directed_dyadic_war_path}, normalize_names = false, encoding = 'latin-1');

insert into source_dyadic_mid
select *
from read_csv_auto({dyadic_mid_path}, normalize_names = false, encoding = 'latin-1');

insert into source_extrastate_wars
select *
from read_csv_auto({extrastate_wars_path}, normalize_names = false);

insert into source_interstate_wars
select *
from read_csv_auto({interstate_wars_path}, normalize_names = false, encoding = 'latin-1');

insert into source_intrastate_wars
select *
from read_csv_auto({intrastate_wars_path}, normalize_names = false, encoding = 'latin-1');

insert into source_war_types
select *
from read_csv_auto({war_types_path}, normalize_names = false);
