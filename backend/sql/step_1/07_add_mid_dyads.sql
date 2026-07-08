create or replace table dyads_after_mid as

with

mid_wars_directed as (

select
    disno,
    war,
    c_code_a,
    c_code_b,
    case battle_deaths_est_a
        when 0 then 0
        when 1 then 25
        when 2 then 100
        when 3 then 250
        when 4 then 500
        when 5 then 999
        when 6 then 1000
    end battle_deaths_est_a,
    case battle_deaths_est_b
        when 0 then 0
        when 1 then 25
        when 2 then 100
        when 3 then 250
        when 4 then 500
        when 5 then 999
        when 6 then 1000
    end battle_deaths_est_b,
    cow_date(start_year_1, start_month_1, start_day_1, 1, 1) start_date,
    extract(year from cow_date(start_year_1, start_month_1, start_day_1, 1, 1))::integer start_year,
    cow_end_date(end_year_1, end_month_1, end_day_1) end_date,
    extract(year from cow_end_date(end_year_1, end_month_1, end_day_1))::integer end_year
from source_dyadic_mid
where war = 1
union all
select
    disno,
    war,
    c_code_b c_code_a,
    c_code_a c_code_b,
    case battle_deaths_est_b
        when 0 then 0
        when 1 then 25
        when 2 then 100
        when 3 then 250
        when 4 then 500
        when 5 then 999
        when 6 then 1000
    end battle_deaths_est_a,
    case battle_deaths_est_a
        when 0 then 0
        when 1 then 25
        when 2 then 100
        when 3 then 250
        when 4 then 500
        when 5 then 999
        when 6 then 1000
    end battle_deaths_est_b,
    cow_date(start_year_1, start_month_1, start_day_1, 1, 1) start_date,
    extract(year from cow_date(start_year_1, start_month_1, start_day_1, 1, 1))::integer start_year,
    cow_end_date(end_year_1, end_month_1, end_day_1) end_date,
    extract(year from cow_end_date(end_year_1, end_month_1, end_day_1))::integer end_year
from source_dyadic_mid
where war = 1),

disno_war_nums as (

select
    disno,
    any_value(war_num) war_num,
    any_value(war_name) war_name
from dyads_after_sources
where disno is not null
group by 1),

assigned_dyads as (

select
    case
        when war_num = -1 and start_year <= 1945 then 139
        when war_num = -1
            and (
                (c_code_a = 483 and c_code_b in (500, 517))
                or (c_code_a = 490 and c_code_b in (500, 517))
                or (c_code_a = 500 and c_code_b in (483, 490, 540, 552, 565))
                or (c_code_a = 517 and c_code_b in (483, 490, 540, 552, 565))
                or (c_code_a = 540 and c_code_b in (500, 517))
                or (c_code_a = 552 and c_code_b in (500, 517))
                or (c_code_a = 565 and c_code_b in (500, 517))
            )
        then 905
        else war_num
    end war_num,
    war_name,
    war_type,
    war_type_name,
    war_subtype,
    disno,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    battle_deaths_a,
    battle_deaths_b,
    battle_deaths_est_a,
    battle_deaths_est_b,
    start_date,
    start_year,
    end_date,
    end_year
from dyads_after_sources
union all
select
    case
        when coalesce(any_value(b.war_num), -1) = -1 and min(a.start_year) <= 1945 then 139
        when coalesce(any_value(b.war_num), -1) = -1
            and (
                (a.c_code_a = 483 and a.c_code_b in (500, 517))
                or (a.c_code_a = 490 and a.c_code_b in (500, 517))
                or (a.c_code_a = 500 and a.c_code_b in (483, 490, 540, 552, 565))
                or (a.c_code_a = 517 and a.c_code_b in (483, 490, 540, 552, 565))
                or (a.c_code_a = 540 and a.c_code_b in (500, 517))
                or (a.c_code_a = 552 and a.c_code_b in (500, 517))
                or (a.c_code_a = 565 and a.c_code_b in (500, 517))
            )
        then 905
        else coalesce(any_value(b.war_num), -1)
    end war_num,
    any_value(b.war_name) war_name,
    null::integer war_type,
    null::varchar war_type_name,
    null::varchar war_subtype,
    a.disno,
    a.c_code_a,
    a.c_code_b,
    clean_participant(c.state_name) participant_a,
    clean_participant(d.state_name) participant_b,
    null::double battle_deaths_a,
    null::double battle_deaths_b,
    sum(greatest(coalesce(a.battle_deaths_est_a, 0), 0)) battle_deaths_est_a,
    sum(greatest(coalesce(a.battle_deaths_est_b, 0), 0)) battle_deaths_est_b,
    min(a.start_date) start_date,
    min(a.start_year) start_year,
    max(a.end_date) end_date,
    max(a.end_year) end_year
from mid_wars_directed a
left join disno_war_nums b on a.disno = b.disno
left join country_codes c on a.c_code_a = c.c_code
left join country_codes d on a.c_code_b = d.c_code
group by 6, 7, 8, 9, 10)

select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    any_value(disno) disno,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    coalesce(nullif(sum(coalesce(battle_deaths_a, 0)), 0), sum(coalesce(battle_deaths_est_a, 0)), 0) battle_deaths_a,
    coalesce(nullif(sum(coalesce(battle_deaths_b, 0)), 0), sum(coalesce(battle_deaths_est_b, 0)), 0) battle_deaths_b,
    if(nullif(sum(coalesce(battle_deaths_a, 0)), 0) is null and sum(coalesce(battle_deaths_est_a, 0)) > 0, 1, 0) battle_deaths_est_a,
    if(nullif(sum(coalesce(battle_deaths_b, 0)), 0) is null and sum(coalesce(battle_deaths_est_b, 0)) > 0, 1, 0) battle_deaths_est_b,
    min(start_date) start_date,
    min(start_year) start_year,
    max(end_date) end_date,
    max(end_year) end_year
from assigned_dyads
group by 1, 7, 8, 9, 10
union all
select
    war_num,
    any_value(war_name) war_name,
    any_value(war_type) war_type,
    any_value(war_type_name) war_type_name,
    any_value(war_subtype) war_subtype,
    disno,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    sum(coalesce(battle_deaths_a, 0)) battle_deaths_a,
    sum(coalesce(battle_deaths_b, 0)) battle_deaths_b,
    0 battle_deaths_est_a,
    0 battle_deaths_est_b,
    min(start_date) start_date,
    min(start_year) start_year,
    max(end_date) end_date,
    max(end_year) end_year
from assigned_dyads
where
    war_num = 139
    and disno = 2581
    and (
        (c_code_a = 220 and c_code_b = 255)
        or (c_code_a = 255 and c_code_b = 220)
    )
group by 1, 6, 7, 8, 9, 10;
