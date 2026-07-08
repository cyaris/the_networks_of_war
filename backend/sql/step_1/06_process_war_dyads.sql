create or replace table dyads_after_sources as
with

war_dyads_directed as (

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    disno,
    c_code_a,
    clean_participant(participant_a) participant_a,
    c_code_b,
    clean_participant(participant_b) participant_b,
    battle_deaths_a,
    battle_deaths_b,
    start_date,
    start_year,
    end_date,
    end_year
from war_dyads
where
    participant_a is not null
    and participant_b is not null
group by 1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
union all
select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    disno,
    c_code_b c_code_a,
    clean_participant(participant_b) participant_a,
    c_code_a c_code_b,
    clean_participant(participant_a) participant_b,
    battle_deaths_b battle_deaths_a,
    battle_deaths_a battle_deaths_b,
    start_date,
    start_year,
    end_date,
    end_year
from war_dyads
where
    participant_a is not null
    and participant_b is not null
group by 1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    any_value(disno) disno,
    c_code_a,
    participant_a,
    c_code_b,
    participant_b,
    sum(iff(battle_deaths_a >= 0, battle_deaths_a, null)) battle_deaths_a,
    sum(iff(battle_deaths_b >= 0, battle_deaths_b, null)) battle_deaths_b,
    0 battle_deaths_est_a,
    0 battle_deaths_est_b,
    min(start_date) start_date,
    min(start_year) start_year,
    max(end_date) end_date,
    max(end_year) end_year
from war_dyads_directed
group by war_num, disno, c_code_a, participant_a, c_code_b, participant_b;
