insert into source_country_codes

select
    clean_text(StateAbb) state_abbreviation,
    clean_int(CCode) c_code,
    clean_text(StateNme) state_name
from read_csv_auto({country_codes_path}, normalize_names = false);

insert into source_interstate_war_dyads

select
    clean_number(warnum) war_num,
    clean_number(disno) disno,
    clean_number(dyindex) dyindex,
    clean_int(statea) c_code_a,
    clean_int(stateb) c_code_b,
    clean_date_day(warstrtday) start_day_1,
    clean_date_month(warstrtmnth) start_month_1,
    clean_date_year(warstrtyr) start_year_1,
    clean_date_day(warenday) end_day_1,
    clean_date_month(warendmnth) end_month_1,
    if(clean_int(warendyr) = 19118, 1918, clean_end_year(warendyr)) end_year_1,
    clean_int(year) source_year,
    clean_int(warolea) role_a,
    clean_int(waroleb) role_b,
    clean_int(wardyadrolea) dyad_role_a,
    clean_int(wardyadroleb) dyad_role_b,
    clean_int(outcomea) outcome_a,
    if(clean_number(warnum) = 139 and clean_int(statea) = 800 and clean_int(stateb) = 710, 5569, clean_number(batdtha)) battle_deaths_a,
    clean_number(batdthb) battle_deaths_b,
    clean_int(changes_1) changes_1,
    clean_int(changes_2) changes_2
from read_csv_auto({interstate_war_dyads_path}, normalize_names = false, encoding = 'latin-1');

insert into source_interstate_mid_dyads

select
    clean_number(disno) disno,
    clean_number(dyindex) dyindex,
    clean_int(statea) c_code_a,
    clean_int(stateb) c_code_b,
    clean_text(namea) name_a,
    clean_text(nameb) name_b,
    clean_date_day(strtday) start_day_1,
    clean_date_month(strtmnth) start_month_1,
    clean_date_year(strtyr) start_year_1,
    clean_date_day(endday) end_day_1,
    clean_date_month(endmnth) end_month_1,
    clean_end_year(endyear) end_year_1,
    clean_int(outcome) outcome,
    clean_int(settlmnt) settlement,
    clean_int(fatlev) fatality_level,
    clean_int(highact) highest_action,
    clean_int(hihost) highest_hostility,
    clean_int(recip) reciprocated,
    clean_int(noinit) no_initiator,
    clean_int(notarg) no_target,
    clean_int(sideaa) side_a_a,
    clean_int(sideab) side_a_b,
    clean_int(revstata) revisionist_state_a,
    clean_int(revstatb) revisionist_state_b,
    clean_int(revtypea) revisionist_type_a,
    clean_int(revtypeb) revisionist_type_b,
    case clean_int(fatleva)
        when 0 then 0
        when 1 then 25
        when 2 then 100
        when 3 then 250
        when 4 then 500
        when 5 then 999
        when 6 then 1000
    end battle_deaths_estimated_a,
    case clean_int(fatlevb)
        when 0 then 0
        when 1 then 25
        when 2 then 100
        when 3 then 250
        when 4 then 500
        when 5 then 999
        when 6 then 1000
    end battle_deaths_estimated_b,
    clean_int(highmcaa) highest_action_a,
    clean_int(highmcab) highest_action_b,
    clean_int(hihosta) highest_hostility_a,
    clean_int(hihostb) highest_hostility_b,
    clean_int(orignata) originator_a,
    clean_int(orignatb) originator_b,
    clean_int(rolea) role_a,
    clean_int(roleb) role_b,
    clean_int(dyad_rolea) dyad_role_a,
    clean_int(dyad_roleb) dyad_role_b,
    clean_int(war) war,
    clean_int(mid5hiact) mid5_highest_action,
    clean_int(mid5hiacta) mid5_highest_action_a,
    clean_int(mid5hiactb) mid5_highest_action_b,
    clean_int(severity) severity,
    clean_int(severitya) severity_a,
    clean_int(severityb) severity_b,
    clean_int(year) source_year,
    clean_int(ongo2014) ongoing_2014,
    clean_int(new) new_record,
    clean_int(change) change_flag,
    clean_int(changetype_1) change_type_1,
    clean_int(changetype_2) change_type_2
from read_csv_auto({interstate_mid_dyads_path}, normalize_names = false, encoding = 'latin-1');

insert into source_extrastate_wars

select
    clean_number(WarNum) war_num,
    clean_text(WarName) war_name,
    clean_war_type(WarType) war_type,
    clean_int(ccode1) c_code_a,
    clean_int(ccode2) c_code_b,
    clean_text(SideA) participant_a,
    clean_text(SideB) participant_b,
    clean_date_day(StartDay1) start_day_1,
    clean_date_month(StartMonth1) start_month_1,
    clean_date_year(StartYear1) start_year_1,
    clean_date_day(StartDay2) start_day_2,
    clean_date_month(StartMonth2) start_month_2,
    clean_date_year(StartYear2) start_year_2,
    clean_date_day(EndDay1) end_day_1,
    clean_date_month(EndMonth1) end_month_1,
    clean_end_year(EndYear1) end_year_1,
    clean_date_day(EndDay2) end_day_2,
    clean_date_month(EndMonth2) end_month_2,
    clean_end_year(EndYear2) end_year_2,
    clean_int(Initiator) initiator,
    clean_int(Interven) intervention,
    clean_war_reference(TransFrom) lagging_war,
    clean_int(Outcome) outcome,
    clean_war_reference(TransTo) leading_war,
    clean_int(WhereFought) where_fought,
    clean_number(BatDeath) battle_deaths_a,
    clean_number(NonStateDeaths) battle_deaths_b,
    clean_number(Version) source_version
from read_csv_auto({extrastate_wars_path}, normalize_names = false);

insert into source_interstate_wars

select
    clean_number(WarNum) war_num,
    clean_text(WarName) war_name,
    clean_war_type(WarType) war_type,
    clean_int(ccode) c_code,
    clean_text(StateName) participant,
    clean_int(Side) side,
    clean_date_day(StartDay1) start_day_1,
    clean_date_month(StartMonth1) start_month_1,
    clean_date_year(StartYear1) start_year_1,
    clean_date_day(StartDay2) start_day_2,
    clean_date_month(StartMonth2) start_month_2,
    clean_date_year(StartYear2) start_year_2,
    clean_date_day(EndDay1) end_day_1,
    clean_date_month(EndMonth1) end_month_1,
    clean_end_year(EndYear1) end_year_1,
    clean_date_day(EndDay2) end_day_2,
    clean_date_month(EndMonth2) end_month_2,
    clean_end_year(EndYear2) end_year_2,
    clean_war_reference(TransFrom) lagging_war,
    clean_int(WhereFought) where_fought,
    clean_int(Initiator) initiator,
    clean_int(Outcome) outcome,
    clean_war_reference(TransTo) leading_war,
    clean_number(BatDeath) battle_deaths,
    clean_number(Version) source_version
from read_csv_auto({interstate_wars_path}, normalize_names = false, encoding = 'latin-1');

insert into source_intrastate_wars

select
    if(clean_number(WarNum) = 977, 979, clean_number(WarNum)) war_num,
    clean_text(WarName) war_name,
    clean_int(V5Region) v5_region,
    clean_war_type(WarType) war_type,
    clean_int(CcodeA) c_code_a,
    clean_int(CcodeB) c_code_b,
    clean_text(SideA) participant_a,
    clean_text(SideB) participant_b,
    clean_int(Intnl) internationalized,
    clean_date_day(StartDy1) start_day_1,
    clean_date_month(StartMo1) start_month_1,
    if(clean_number(WarNum) = 976, 2011, clean_date_year(StartYr1)) start_year_1,
    clean_date_day(StartDy2) start_day_2,
    clean_date_month(StartMo2) start_month_2,
    clean_date_year(StartYr2) start_year_2,
    clean_date_day(StartDy3) start_day_3,
    clean_date_month(StartMo3) start_month_3,
    clean_date_year(StartYr3) start_year_3,
    clean_date_day(StartDy4) start_day_4,
    clean_date_month(StartMo4) start_month_4,
    clean_date_year(StartYr4) start_year_4,
    clean_date_day(EndDy1) end_day_1,
    clean_date_month(EndMo1) end_month_1,
    if(clean_number(WarNum) in (942, 990.4, 991, 991.4, 992.5), -7, clean_end_year(EndYr1)) end_year_1,
    clean_date_day(EndDy2) end_day_2,
    clean_date_month(EndMo2) end_month_2,
    clean_end_year(EndYr2) end_year_2,
    clean_date_day(EndDy3) end_day_3,
    clean_date_month(EndMo3) end_month_3,
    clean_end_year(EndYr3) end_year_3,
    clean_date_day(EndDy4) end_day_4,
    clean_date_month(EndMo4) end_month_4,
    clean_end_year(EndYr4) end_year_4,
    clean_war_reference(TransFrom) lagging_war,
    clean_int(Initiator) initiator,
    clean_int(Outcome) outcome,
    clean_war_reference(TransTo) leading_war,
    clean_number("Deaths A") battle_deaths_a,
    clean_number("Deaths B") battle_deaths_b,
    clean_number(SideAPeakTotForces) side_a_peak_total_forces,
    clean_number(SideBPeakTotForces) side_b_peak_total_forces,
    clean_number("SideAPeak TheatForces") side_a_peak_theater_forces,
    clean_number(SideBPeakTheatForces) side_b_peak_theater_forces,
    clean_number(Version) source_version
from read_csv_auto({intrastate_wars_path}, normalize_names = false, encoding = 'latin-1');
