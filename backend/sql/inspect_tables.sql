select
    'source_country_codes' table_name,
    count(*) row_count
from source_country_codes
union all
select
    'source_interstate_war_dyads' table_name,
    count(*) row_count
from source_interstate_war_dyads
union all
select
    'source_interstate_mid_dyads' table_name,
    count(*) row_count
from source_interstate_mid_dyads
union all
select
    'source_extrastate_wars' table_name,
    count(*) row_count
from source_extrastate_wars
union all
select
    'source_interstate_wars' table_name,
    count(*) row_count
from source_interstate_wars
union all
select
    'source_intrastate_wars' table_name,
    count(*) row_count
from source_intrastate_wars
union all
select
    'war_participants' table_name,
    count(*) row_count
from war_participants
union all
select
    'dyads_after_sources' table_name,
    count(*) row_count
from dyads_after_sources
union all
select
    'participants' table_name,
    count(*) row_count
from participants
union all
select
    'dyads' table_name,
    count(*) row_count
from dyads
union all
select
    'dyad_years' table_name,
    count(*) row_count
from dyad_years
union all
select
    'wars' table_name,
    count(*) row_count
from wars;
