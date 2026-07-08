create or replace table war_participants as
with

interstate_prepared as (

select
    *,
    least(
        cow_date("StartYear1", "StartMonth1", "StartDay1", 1, 1),
        cow_date("StartYear2", "StartMonth2", "StartDay2", 1, 1)
    ) start_date,
    greatest(
        cow_end_date("EndYear1", "EndMonth1", "EndDay1"),
        cow_end_date("EndYear2", "EndMonth2", "EndDay2")
    ) end_date,
    greatest(
        date_estimated("StartYear1", "StartMonth1", "StartDay1"),
        date_estimated("StartYear2", "StartMonth2", "StartDay2")
    ) start_date_estimated,
    greatest(
        date_estimated("EndYear1", "EndMonth1", "EndDay1"),
        date_estimated("EndYear2", "EndMonth2", "EndDay2")
    ) end_date_estimated,
    greatest(ongoing_war("EndYear1"), ongoing_war("EndYear2")) ongoing_war
from source_interstate_wars),

participant_rows as (

select
    'Inter-StateWarData_v4.0.csv' source_file,
    clean_number(a."WarNum") war_num,
    clean_text(a."WarName") war_name,
    clean_int(a."WarType") war_type,
    c.war_type_name,
    c.war_subtype,
    clean_int(a.ccode) c_code,
    coalesce(d.state_name, clean_text(a."StateName")) participant,
    clean_int(a."Side") side,
    a.start_date,
    extract(year from a.start_date)::integer start_year,
    a.end_date,
    extract(year from a.end_date)::integer end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war,
    clean_int(a."BatDeath")::double battle_deaths
from interstate_prepared a
left join war_types c on clean_int(a."WarType") = c.war_type
left join country_codes d on clean_int(a.ccode) = d.c_code
union all
select
    source_file,
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code_a c_code,
    participant_a participant,
    side_a side,
    start_date,
    start_year,
    end_date,
    end_year,
    start_date_estimated,
    end_date_estimated,
    ongoing_war,
    battle_deaths_a battle_deaths
from war_dyads
where source_file in ('Extra-StateWarData_v4.0.csv', 'INTRA-STATE_State_participants v5.1.csv')
    and participant_a is not null
union all
select
    source_file,
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code_b c_code,
    participant_b participant,
    side_b side,
    start_date,
    start_year,
    end_date,
    end_year,
    start_date_estimated,
    end_date_estimated,
    ongoing_war,
    battle_deaths_b battle_deaths
from war_dyads
where source_file in ('Extra-StateWarData_v4.0.csv', 'INTRA-STATE_State_participants v5.1.csv')
    and participant_b is not null),

cleaned_participant_rows as (

select
    source_file,
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code,
    clean_participant(participant) participant,
    side,
    start_date,
    start_year,
    end_date,
    end_year,
    start_date_estimated,
    end_date_estimated,
    ongoing_war,
    battle_deaths
from participant_rows)

select
    string_agg(distinct source_file, ', ' order by source_file) source_file,
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    c_code,
    participant,
    case
        when war_num = 139 and c_code in (365, 375) then 3
        when min(side) = 1 and max(side) = 2 then 3
        else max(side)
    end side,
    min(start_date) start_date,
    min(start_year) start_year,
    max(end_date) end_date,
    max(end_year) end_year,
    max(start_date_estimated) start_date_estimated,
    max(end_date_estimated) end_date_estimated,
    max(ongoing_war) ongoing_war,
    sum(battle_deaths) battle_deaths
from cleaned_participant_rows
where participant is not null
group by 2, 7, 8;
