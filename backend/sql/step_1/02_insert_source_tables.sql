insert into source_country_codes

select
    clean_text("StateAbb") state_abbreviation,
    clean_int("CCode") c_code,
    clean_text("StateNme") state_name
from read_csv_auto({country_codes_path}, normalize_names = false);

insert into source_directed_dyadic_war

select
    clean_number(warnum) war_num,
    clean_number(disno) disno,
    clean_number(dyindex) dyindex,
    clean_int(statea) c_code_a,
    clean_int(stateb) c_code_b,
    if(clean_int(warstrtmnth) = 24, 12, clean_int(warstrtmnth)) start_month_1,
    clean_int(warstrtday) start_day_1,
    clean_int(warstrtyr) start_year_1,
    clean_int(warendmnth) end_month_1,
    clean_int(warenday) end_day_1,
    if(clean_int(warendyr) = 19118, 1918, clean_int(warendyr)) end_year_1,
    clean_int(warolea) side_a,
    clean_int(waroleb) side_b,
    clean_int(outcomea) outcome_a,
    if(clean_number(warnum) = 139 and clean_int(statea) = 800 and clean_int(stateb) = 710, 5569, clean_number(batdtha)) battle_deaths_a,
    clean_number(batdthb) battle_deaths_b,
    if(clean_number(warnum) = 139 and clean_int(statea) = 800 and clean_int(stateb) = 710, 6569, clean_number(batdths)) battle_deaths_total
from read_csv_auto({directed_dyadic_war_path}, normalize_names = false, encoding = 'latin-1');

insert into source_dyadic_mid

select
    clean_number(disno) disno,
    clean_int(statea) c_code_a,
    clean_int(stateb) c_code_b,
    clean_int(strtday) start_day_1,
    clean_int(strtmnth) start_month_1,
    clean_int(strtyr) start_year_1,
    clean_int(endday) end_day_1,
    clean_int(endmnth) end_month_1,
    clean_int(endyear) end_year_1,
    clean_number(fatleva) battle_deaths_est_a,
    clean_number(fatlevb) battle_deaths_est_b,
    clean_int(war) war
from read_csv_auto({dyadic_mid_path}, normalize_names = false, encoding = 'latin-1');

insert into source_extrastate_wars

select
    clean_number("WarNum") war_num,
    clean_text("WarName") war_name,
    clean_int("WarType") war_type,
    clean_int("ccode1") c_code_a,
    clean_int("ccode2") c_code_b,
    clean_text("SideA") participant_a,
    clean_text("SideB") participant_b,
    clean_int("StartMonth1") start_month_1,
    clean_int("StartDay1") start_day_1,
    clean_int("StartYear1") start_year_1,
    clean_int("EndMonth1") end_month_1,
    clean_int("EndDay1") end_day_1,
    clean_int("EndYear1") end_year_1,
    clean_int("StartMonth2") start_month_2,
    clean_int("StartDay2") start_day_2,
    clean_int("StartYear2") start_year_2,
    clean_int("EndMonth2") end_month_2,
    clean_int("EndDay2") end_day_2,
    clean_int("EndYear2") end_year_2,
    clean_int("Outcome") outcome_a,
    clean_number("BatDeath") battle_deaths_a,
    clean_number("NonStateDeaths") battle_deaths_b
from read_csv_auto({extrastate_wars_path}, normalize_names = false);

insert into source_interstate_wars

select
    clean_number("WarNum") war_num,
    clean_text("WarName") war_name,
    clean_int("WarType") war_type,
    clean_int(ccode) c_code,
    clean_text("StateName") participant,
    clean_int("Side") side,
    clean_int("StartMonth1") start_month_1,
    clean_int("StartDay1") start_day_1,
    clean_int("StartYear1") start_year_1,
    clean_int("EndMonth1") end_month_1,
    clean_int("EndDay1") end_day_1,
    clean_int("EndYear1") end_year_1,
    clean_int("StartMonth2") start_month_2,
    clean_int("StartDay2") start_day_2,
    clean_int("StartYear2") start_year_2,
    clean_int("EndMonth2") end_month_2,
    clean_int("EndDay2") end_day_2,
    clean_int("EndYear2") end_year_2,
    clean_number("BatDeath") battle_deaths
from read_csv_auto({interstate_wars_path}, normalize_names = false, encoding = 'latin-1');

insert into source_intrastate_wars

select
    if(clean_number("WarNum") = 977, 979, clean_number("WarNum")) war_num,
    clean_text("WarName") war_name,
    clean_int("WarType") war_type,
    clean_int("CcodeA") c_code_a,
    clean_int("CcodeB") c_code_b,
    clean_text("SideA") participant_a,
    clean_text("SideB") participant_b,
    clean_int("StartMo1") start_month_1,
    clean_int("StartDy1") start_day_1,
    if(clean_number("WarNum") = 976, 2011, clean_int("StartYr1")) start_year_1,
    clean_int("EndMo1") end_month_1,
    clean_int("EndDy1") end_day_1,
    -- TODO: fix this line to instead address all ongoing wars, instead of hard-coding the wars that are ongoing.
    if(clean_number("WarNum") in (942, 990.4, 991, 991.4, 992.5), -7, clean_int("EndYr1")) end_year_1,
    clean_int("StartMo2") start_month_2,
    clean_int("StartDy2") start_day_2,
    clean_int("StartYr2") start_year_2,
    clean_int("EndMo2") end_month_2,
    clean_int("EndDy2") end_day_2,
    clean_int("EndYr2") end_year_2,
    clean_int("StartMo3") start_month_3,
    clean_int("StartDy3") start_day_3,
    clean_int("StartYr3") start_year_3,
    clean_int("EndMo3") end_month_3,
    clean_int("EndDy3") end_day_3,
    clean_int("EndYr3") end_year_3,
    clean_int("StartMo4") start_month_4,
    clean_int("StartDy4") start_day_4,
    clean_int("StartYr4") start_year_4,
    clean_int("EndMo4") end_month_4,
    clean_int("EndDy4") end_day_4,
    clean_int("EndYr4") end_year_4,
    clean_int("Outcome") outcome_a,
    clean_number("Deaths A") battle_deaths_a,
    clean_number("Deaths B") battle_deaths_b,
    clean_number("TotalBDeaths") battle_deaths_total
from read_csv_auto({intrastate_wars_path}, normalize_names = false, encoding = 'latin-1');

insert into source_war_types

select
    clean_int("war_type_code") war_type,
    clean_text("war_type") war_type_name,
    clean_text("war_subtype") war_subtype
from read_csv_auto({war_types_path}, normalize_names = false);
