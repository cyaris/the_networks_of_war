create or replace table initial_participants as

select
    source_file,
    war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    c_code,
    participant,
    side,
    start_date,
    start_year,
    end_date,
    end_year,
    start_date_estimated,
    end_date_estimated,
    ongoing_war,
    battle_deaths
from participants_after_dyads;

create or replace table initial_dyads as

select
    a.war_num,
    a.c_code_a,
    a.c_code_b,
    a.participant_a,
    a.participant_b,
    a.battle_deaths_a,
    a.battle_deaths_b,
    a.battle_deaths_est_a,
    a.battle_deaths_est_b,
    a.start_date,
    a.start_year,
    a.end_date,
    a.end_year,
    b.range::integer "year"
from dyads_after_inference a
join range(1500, 2100) b on b.range between a.start_year and a.end_year
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14;

create or replace table initial_wars as

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    count(*) total_participants,
    min(start_date) start_date,
    min(start_year) start_year,
    max(end_date) end_date,
    max(end_year) end_year,
    max(ongoing_war) ongoing_war
from initial_participants
where war_type is not null
group by 1;
