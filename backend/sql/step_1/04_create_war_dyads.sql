create or replace table war_dyads as
with

interstate_wars as (

select
    clean_number("WarNum") war_num,
    any_value(clean_text("WarName")) war_name,
    any_value(clean_int("WarType")) war_type
from source_interstate_wars
group by 1),

directed_prepared as (

select
    * replace (
        iff(clean_int(warstrtmnth) = 24, '12', warstrtmnth) as warstrtmnth,
        iff(clean_int(warendyr) = 19118, '1918', warendyr) as warendyr
    )
from source_directed_dyadic_war),

directed_dyads as (

select
    'directed_dyadic_war.csv' source_file,
    clean_number(a.warnum) war_num,
    b.war_name,
    b.war_type,
    c.war_type_name,
    c.war_subtype,
    clean_int(a.disno)::double disno,
    clean_int(a.dyindex)::double dyindex,
    clean_int(a.statea) c_code_a,
    d.state_name participant_a,
    clean_int(a.warolea) side_a,
    clean_int(a.stateb) c_code_b,
    e.state_name participant_b,
    clean_int(a.waroleb) side_b,
    cow_date(a.warstrtyr, a.warstrtmnth, a.warstrtday, 1, 1) start_date,
    clean_int(a.warstrtyr) start_year,
    cow_end_date(a.warendyr, a.warendmnth, a.warenday) end_date,
    extract(year from cow_end_date(a.warendyr, a.warendmnth, a.warenday))::integer end_year,
    date_estimated(a.warstrtyr, a.warstrtmnth, a.warstrtday) start_date_estimated,
    date_estimated(a.warendyr, a.warendmnth, a.warenday) end_date_estimated,
    ongoing_war(a.warendyr) ongoing_war,
    clean_int(a.batdtha)::double battle_deaths_a,
    clean_int(a.batdthb)::double battle_deaths_b,
    clean_int(a.batdths)::double battle_deaths_total,
    clean_int(a.outcomea) outcome_a,
    null::integer outcome_b
from directed_prepared a
left join interstate_wars b on clean_number(a.warnum) = b.war_num
left join war_types c on b.war_type = c.war_type
left join country_codes d on clean_int(a.statea) = d.c_code
left join country_codes e on clean_int(a.stateb) = e.c_code),

extra_prepared as (

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
from source_extrastate_wars),

extra_dyads as (

select
    'Extra-StateWarData_v4.0.csv' source_file,
    clean_number(a."WarNum") war_num,
    clean_text(a."WarName") war_name,
    clean_int(a."WarType") war_type,
    c.war_type_name,
    c.war_subtype,
    null::double disno,
    null::double dyindex,
    clean_int(a."ccode1") c_code_a,
    coalesce(d.state_name, clean_text(a."SideA")) participant_a,
    1 side_a,
    clean_int(a."ccode2") c_code_b,
    coalesce(e.state_name, clean_text(a."SideB")) participant_b,
    2 side_b,
    a.start_date,
    extract(year from a.start_date)::integer start_year,
    a.end_date,
    extract(year from a.end_date)::integer end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war,
    clean_int(a."BatDeath")::double battle_deaths_a,
    clean_int(a."NonStateDeaths")::double battle_deaths_b,
    clean_int(a."BatDeath")::double + clean_int(a."NonStateDeaths")::double battle_deaths_total,
    clean_int(a."Outcome") outcome_a,
    null::integer outcome_b
from extra_prepared a
left join war_types c on clean_int(a."WarType") = c.war_type
left join country_codes d on clean_int(a."ccode1") = d.c_code
left join country_codes e on clean_int(a."ccode2") = e.c_code),

intra_prepared as (

select
    * replace (
        iff(clean_number("WarNum") = 977, '979', "WarNum") as "WarNum",
        iff(clean_number("WarNum") = 976, '2011', "StartYr1") as "StartYr1",
        iff(clean_number("WarNum") in (942, 990.4, 991, 991.4, 992.5), '-7', "EndYr1") as "EndYr1"
    )
from source_intrastate_wars),

intra_dates as (

select
    *,
    least(
        cow_date("StartYr1", "StartMo1", "StartDy1", 1, 1),
        cow_date("StartYr2", "StartMo2", "StartDy2", 1, 1),
        cow_date("StartYr3", "StartMo3", "StartDy3", 1, 1),
        cow_date("StartYr4", "StartMo4", "StartDy4", 1, 1)
    ) start_date,
    greatest(
        cow_end_date("EndYr1", "EndMo1", "EndDy1"),
        cow_end_date("EndYr2", "EndMo2", "EndDy2"),
        cow_end_date("EndYr3", "EndMo3", "EndDy3"),
        cow_end_date("EndYr4", "EndMo4", "EndDy4")
    ) end_date,
    greatest(
        date_estimated("StartYr1", "StartMo1", "StartDy1"),
        date_estimated("StartYr2", "StartMo2", "StartDy2"),
        date_estimated("StartYr3", "StartMo3", "StartDy3"),
        date_estimated("StartYr4", "StartMo4", "StartDy4")
    ) start_date_estimated,
    greatest(
        date_estimated("EndYr1", "EndMo1", "EndDy1"),
        date_estimated("EndYr2", "EndMo2", "EndDy2"),
        date_estimated("EndYr3", "EndMo3", "EndDy3"),
        date_estimated("EndYr4", "EndMo4", "EndDy4")
    ) end_date_estimated,
    greatest(
        ongoing_war("EndYr1"),
        ongoing_war("EndYr2"),
        ongoing_war("EndYr3"),
        ongoing_war("EndYr4")
    ) ongoing_war
from intra_prepared),

intra_dyads as (

select
    'INTRA-STATE_State_participants v5.1.csv' source_file,
    clean_number(a."WarNum") war_num,
    clean_text(a."WarName") war_name,
    clean_int(a."WarType") war_type,
    c.war_type_name,
    c.war_subtype,
    null::double disno,
    null::double dyindex,
    clean_int(a."CcodeA") c_code_a,
    coalesce(d.state_name, clean_text(a."SideA")) participant_a,
    1 side_a,
    clean_int(a."CcodeB") c_code_b,
    coalesce(e.state_name, clean_text(a."SideB")) participant_b,
    2 side_b,
    a.start_date,
    extract(year from a.start_date)::integer start_year,
    a.end_date,
    extract(year from a.end_date)::integer end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    a.ongoing_war,
    clean_int(a."Deaths A")::double battle_deaths_a,
    clean_int(a."Deaths B")::double battle_deaths_b,
    clean_int(a."TotalBDeaths")::double battle_deaths_total,
    clean_int(a."Outcome") outcome_a,
    null::integer outcome_b
from intra_dates a
left join war_types c on clean_int(a."WarType") = c.war_type
left join country_codes d on clean_int(a."CcodeA") = d.c_code
left join country_codes e on clean_int(a."CcodeB") = e.c_code)

select *
from directed_dyads
union all
select *
from extra_dyads
union all
select *
from intra_dyads;
