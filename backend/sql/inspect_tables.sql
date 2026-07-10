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
    'source_war_types' table_name,
    count(*) row_count
from source_war_types
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
    'initial_participants' table_name,
    count(*) row_count
from initial_participants
union all
select
    'initial_dyads' table_name,
    count(*) row_count
from initial_dyads
union all
select
    'initial_dyad_years' table_name,
    count(*) row_count
from initial_dyad_years
union all
select
    'initial_wars' table_name,
    count(*) row_count
from initial_wars;
