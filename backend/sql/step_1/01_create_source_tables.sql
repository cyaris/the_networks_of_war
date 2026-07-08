create or replace table source_country_codes (
    state_abbreviation varchar,
    c_code integer,
    state_name varchar
);

create or replace table source_directed_dyadic_war (
    war_num double,
    disno double,
    dyindex double,
    c_code_a integer,
    c_code_b integer,
    start_month_1 integer,
    start_day_1 integer,
    start_year_1 integer,
    end_month_1 integer,
    end_day_1 integer,
    end_year_1 integer,
    side_a integer,
    side_b integer,
    outcome_a integer,
    battle_deaths_a double,
    battle_deaths_b double,
    battle_deaths_total double
);

create or replace table source_dyadic_mid (
    disno double,
    c_code_a integer,
    c_code_b integer,
    start_day_1 integer,
    start_month_1 integer,
    start_year_1 integer,
    end_day_1 integer,
    end_month_1 integer,
    end_year_1 integer,
    battle_deaths_est_a double,
    battle_deaths_est_b double,
    war integer
);

create or replace table source_extrastate_wars (
    war_num double,
    war_name varchar,
    war_type integer,
    c_code_a integer,
    c_code_b integer,
    participant_a varchar,
    participant_b varchar,
    start_month_1 integer,
    start_day_1 integer,
    start_year_1 integer,
    end_month_1 integer,
    end_day_1 integer,
    end_year_1 integer,
    start_month_2 integer,
    start_day_2 integer,
    start_year_2 integer,
    end_month_2 integer,
    end_day_2 integer,
    end_year_2 integer,
    outcome_a integer,
    battle_deaths_a double,
    battle_deaths_b double
);

create or replace table source_interstate_wars (
    war_num double,
    war_name varchar,
    war_type integer,
    c_code integer,
    participant varchar,
    side integer,
    start_month_1 integer,
    start_day_1 integer,
    start_year_1 integer,
    end_month_1 integer,
    end_day_1 integer,
    end_year_1 integer,
    start_month_2 integer,
    start_day_2 integer,
    start_year_2 integer,
    end_month_2 integer,
    end_day_2 integer,
    end_year_2 integer,
    battle_deaths double
);

create or replace table source_intrastate_wars (
    war_num double,
    war_name varchar,
    war_type integer,
    c_code_a integer,
    c_code_b integer,
    participant_a varchar,
    participant_b varchar,
    start_month_1 integer,
    start_day_1 integer,
    start_year_1 integer,
    end_month_1 integer,
    end_day_1 integer,
    end_year_1 integer,
    start_month_2 integer,
    start_day_2 integer,
    start_year_2 integer,
    end_month_2 integer,
    end_day_2 integer,
    end_year_2 integer,
    start_month_3 integer,
    start_day_3 integer,
    start_year_3 integer,
    end_month_3 integer,
    end_day_3 integer,
    end_year_3 integer,
    start_month_4 integer,
    start_day_4 integer,
    start_year_4 integer,
    end_month_4 integer,
    end_day_4 integer,
    end_year_4 integer,
    outcome_a integer,
    battle_deaths_a double,
    battle_deaths_b double,
    battle_deaths_total double
);

create or replace table source_war_types (
    war_type integer,
    war_type_name varchar,
    war_subtype varchar
);
