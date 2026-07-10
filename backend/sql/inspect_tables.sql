select 'source_country_codes' table_name, count(*) row_count from source_country_codes
union all
select 'source_interstate_war_dyads', count(*) from source_interstate_war_dyads
union all
select 'source_interstate_mid_dyads', count(*) from source_interstate_mid_dyads
union all
select 'source_extrastate_wars', count(*) from source_extrastate_wars
union all
select 'source_interstate_wars', count(*) from source_interstate_wars
union all
select 'source_intrastate_wars', count(*) from source_intrastate_wars
union all
select 'source_war_types', count(*) from source_war_types
union all
select 'war_participants', count(*) from war_participants
union all
select 'dyads_after_sources', count(*) from dyads_after_sources
union all
select 'initial_participants', count(*) from initial_participants
union all
select 'initial_dyads', count(*) from initial_dyads
union all
select 'initial_wars', count(*) from initial_wars;
