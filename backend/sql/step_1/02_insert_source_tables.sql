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
    clean_int(warstrtday) start_day_1,
    if(clean_int(warstrtmnth) = 24, 12, clean_int(warstrtmnth)) start_month_1,
    clean_int(warstrtyr) start_year_1,
    clean_int(warenday) end_day_1,
    clean_int(warendmnth) end_month_1,
    if(clean_int(warendyr) = 19118, 1918, clean_int(warendyr)) end_year_1,
    clean_int(year) source_year,
    clean_int(warolea) side_a,
    clean_int(waroleb) side_b,
    clean_int(wardyadrolea) war_dyad_role_a,
    clean_int(wardyadroleb) war_dyad_role_b,
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
    clean_int(strtday) start_day_1,
    clean_int(strtmnth) start_month_1,
    clean_int(strtyr) start_year_1,
    clean_int(endday) end_day_1,
    clean_int(endmnth) end_month_1,
    clean_int(endyear) end_year_1,
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
    clean_int(changetype_2) change_type_2,
    clean_text(dyad) dyad,
    clean_text(abbreva) abbrev_a,
    clean_text(abbrevb) abbrev_b,
    clean_int(lastobs) last_observation,
    clean_int(newar) newar
from read_csv_auto({interstate_mid_dyads_path}, normalize_names = false, encoding = 'latin-1');

insert into source_extrastate_wars

select
    clean_number(WarNum) war_num,
    clean_text(WarName) war_name,
    clean_int(WarType) war_type,
    clean_int(ccode1) c_code_a,
    clean_int(ccode2) c_code_b,
    clean_text(SideA) participant_a,
    clean_text(SideB) participant_b,
    clean_int(StartDay1) start_day_1,
    clean_int(StartMonth1) start_month_1,
    clean_int(StartYear1) start_year_1,
    clean_int(StartDay2) start_day_2,
    clean_int(StartMonth2) start_month_2,
    clean_int(StartYear2) start_year_2,
    clean_int(EndDay1) end_day_1,
    clean_int(EndMonth1) end_month_1,
    clean_int(EndYear1) end_year_1,
    clean_int(EndDay2) end_day_2,
    clean_int(EndMonth2) end_month_2,
    clean_int(EndYear2) end_year_2,
    clean_int(Initiator) initiator,
    clean_int(Interven) intervention,
    clean_int(TransFrom) trans_from,
    clean_int(Outcome) outcome,
    clean_int(TransTo) trans_to,
    clean_int(WhereFought) where_fought,
    clean_number(BatDeath) battle_deaths_a,
    clean_number(NonStateDeaths) battle_deaths_b,
    clean_number(Version) source_version
from read_csv_auto({extrastate_wars_path}, normalize_names = false);

insert into source_interstate_wars

select
    clean_number(WarNum) war_num,
    clean_text(WarName) war_name,
    clean_int(WarType) war_type,
    clean_int(ccode) c_code,
    clean_text(StateName) participant,
    clean_int(Side) side,
    clean_int(StartDay1) start_day_1,
    clean_int(StartMonth1) start_month_1,
    clean_int(StartYear1) start_year_1,
    clean_int(StartDay2) start_day_2,
    clean_int(StartMonth2) start_month_2,
    clean_int(StartYear2) start_year_2,
    clean_int(EndDay1) end_day_1,
    clean_int(EndMonth1) end_month_1,
    clean_int(EndYear1) end_year_1,
    clean_int(EndDay2) end_day_2,
    clean_int(EndMonth2) end_month_2,
    clean_int(EndYear2) end_year_2,
    clean_int(TransFrom) trans_from,
    clean_int(WhereFought) where_fought,
    clean_int(Initiator) initiator,
    clean_int(Outcome) outcome,
    clean_int(TransTo) trans_to,
    clean_number(BatDeath) battle_deaths,
    clean_number(Version) source_version
from read_csv_auto({interstate_wars_path}, normalize_names = false, encoding = 'latin-1');

insert into source_intrastate_wars

select
    if(clean_number(WarNum) = 977, 979, clean_number(WarNum)) war_num,
    clean_text(WarName) war_name,
    clean_int(V5Region) v5_region,
    clean_int(WarType) war_type,
    clean_int(CcodeA) c_code_a,
    clean_int(CcodeB) c_code_b,
    clean_text(SideA) participant_a,
    clean_text(SideB) participant_b,
    clean_int(Intnl) internationalized,
    clean_int(StartDy1) start_day_1,
    clean_int(StartMo1) start_month_1,
    if(clean_number(WarNum) = 976, 2011, clean_int(StartYr1)) start_year_1,
    clean_int(StartDy2) start_day_2,
    clean_int(StartMo2) start_month_2,
    clean_int(StartYr2) start_year_2,
    clean_int(StartDy3) start_day_3,
    clean_int(StartMo3) start_month_3,
    clean_int(StartYr3) start_year_3,
    clean_int(StartDy4) start_day_4,
    clean_int(StartMo4) start_month_4,
    clean_int(StartYr4) start_year_4,
    clean_int(EndDy1) end_day_1,
    clean_int(EndMo1) end_month_1,
    -- TODO: fix this line to instead address all ongoing wars, instead of hard-coding the wars that are ongoing.
    if(clean_number(WarNum) in (942, 990.4, 991, 991.4, 992.5), -7, clean_int(EndYr1)) end_year_1,
    clean_int(EndDy2) end_day_2,
    clean_int(EndMo2) end_month_2,
    clean_int(EndYr2) end_year_2,
    clean_int(EndDy3) end_day_3,
    clean_int(EndMo3) end_month_3,
    clean_int(EndYr3) end_year_3,
    clean_int(EndDy4) end_day_4,
    clean_int(EndMo4) end_month_4,
    clean_int(EndYr4) end_year_4,
    clean_int(TransFrom) trans_from,
    clean_int(Initiator) initiator,
    clean_int(Outcome) outcome,
    clean_int(TransTo) trans_to,
    clean_number("Deaths A") battle_deaths_a,
    clean_number("Deaths B") battle_deaths_b,
    clean_number(SideAPeakTotForces) side_a_peak_total_forces,
    clean_number(SideBPeakTotForces) side_b_peak_total_forces,
    clean_number("SideAPeak TheatForces") side_a_peak_theater_forces,
    clean_number(SideBPeakTheatForces) side_b_peak_theater_forces,
    clean_number(Version) source_version
from read_csv_auto({intrastate_wars_path}, normalize_names = false, encoding = 'latin-1');

insert into source_war_types

select
    clean_int(war_type_code) war_type,
    clean_text(war_type) war_type_name,
    clean_text(war_subtype) war_subtype
from read_csv_auto({war_types_path}, normalize_names = false);
