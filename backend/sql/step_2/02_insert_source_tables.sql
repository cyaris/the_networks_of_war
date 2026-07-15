insert into source_global_terrorism_database

select
    'globalterrorismdb_0522dist.csv' source_file,
    clean_text(eventid) event_id,
    clean_int(iyear) "year",
    clean_text(country_txt) country_name,
    clean_number(nkill) killed,
    clean_number(nwound) wounded,
    clean_number(nhostkid) hostages_kidnapped
from read_csv_auto({global_terrorism_database_path_1}, normalize_names = false, all_varchar = true)
union all
select
    'globalterrorismdb_2021Jan-June_1222dist.csv' source_file,
    clean_text(eventid) event_id,
    clean_int(iyear) "year",
    clean_text(country_txt) country_name,
    clean_number(nkill) killed,
    clean_number(nwound) wounded,
    clean_number(nhostkid) hostages_kidnapped
from read_csv_auto({global_terrorism_database_path_2}, normalize_names = false, all_varchar = true);

insert into source_formal_alliances_directed_yearly

select
    clean_int(version4id) version_4_id,
    clean_int(ccode1) c_code_a,
    clean_int(ccode2) c_code_b,
    clean_text(state_name1) state_name_a,
    clean_text(state_name2) state_name_b,
    clean_int(dyad_st_day) dyad_start_day,
    clean_int(dyad_st_month) dyad_start_month,
    clean_int(dyad_st_year) dyad_start_year,
    clean_int(dyad_end_day) dyad_end_day,
    clean_int(dyad_end_month) dyad_end_month,
    clean_int(dyad_end_year) dyad_end_year,
    clean_int(left_censor) left_censor,
    clean_int(right_censor) right_censor,
    clean_int(defense) defense,
    clean_int(neutrality) neutrality,
    clean_int(nonaggression) nonaggression,
    clean_int(entente) entente,
    clean_int(year) "year",
    clean_number(version) source_version
from read_csv_auto({formal_alliances_directed_yearly_path}, normalize_names = false, all_varchar = true);

insert into source_territorial_changes

select
    clean_int(year) "year",
    clean_int(month) "month",
    clean_int(gainer) gainer,
    clean_int(gaintype) gain_type,
    clean_int(procedur) "procedure",
    clean_text(entity) entity,
    clean_int(contgain) contiguous_gain,
    clean_number(area) area,
    clean_number(pop) population,
    clean_int(portion) portion,
    clean_int(loser) loser,
    clean_int(losetype) lose_type,
    clean_int(contlose) contiguous_loss,
    clean_int(entry) entry,
    clean_int(exit) exit,
    clean_int(number) number,
    clean_int(indep) independent,
    clean_int(conflict) "conflict",
    clean_number(version) source_version
from read_csv_auto({territorial_changes_path}, normalize_names = false, all_varchar = true);

insert into source_forcibly_displaced_populations

select
    clean_int(ccode) c_code,
    clean_text(scode) state_abbreviation,
    clean_text(country) country_name,
    clean_int(year) "year",
    clean_number("source") "source",
    clean_number(idp) internally_displaced_persons,
    clean_number(host) hosted_refugees
from read_csv_auto({forcibly_displaced_populations_path}, normalize_names = false, all_varchar = true);

insert into source_colonial_dependency_contiguity

select
    clean_int(dyad) dyad,
    clean_int(statelno) c_code_a,
    clean_int(statehno) c_code_b,
    clean_text(statelab) state_abbreviation_a,
    clean_text(statehab) state_abbreviation_b,
    clean_int(year) "year",
    clean_int(land) land,
    clean_int(sea) sea,
    clean_int(total) total,
    clean_number(version) source_version
from read_csv_auto({colonial_dependency_contiguity_path}, normalize_names = false, all_varchar = true);

insert into source_direct_contiguity

select
    clean_int(dyad) dyad,
    clean_int(state1no) c_code_a,
    clean_int(state2no) c_code_b,
    clean_text(state1ab) state_abbreviation_a,
    clean_text(state2ab) state_abbreviation_b,
    clean_int(year) "year",
    clean_int(conttype) contiguity_type,
    clean_number(version) source_version
from read_csv_auto({direct_contiguity_path}, normalize_names = false, all_varchar = true);

insert into source_defense_cooperation_agreements

select
    clean_int(ccode1) c_code_a,
    clean_int(ccode2) c_code_b,
    clean_text(abbrev1) state_abbreviation_a,
    clean_text(abbrev2) state_abbreviation_b,
    clean_int(year) "year",
    clean_int(dcaGeneralV1) dca_general_v1,
    clean_int(dcaGeneralV2) dca_general_v2,
    clean_int(dcaSectorV1) dca_sector_v1,
    clean_int(dcaSectorV2) dca_sector_v2,
    clean_int(dcaAnyV1) dca_any_v1,
    clean_int(dcaAnyV2) dca_any_v2
from read_csv_auto({defense_cooperation_agreements_path}, normalize_names = false, all_varchar = true);

insert into source_intergovernmental_organizations_dyadic

select
    clean_int(ccode1) c_code_a,
    clean_int(ccode2) c_code_b,
    clean_text(country1) country_name_a,
    clean_text(country2) country_name_b,
    clean_int(year) "year",
    clean_int(state) shared_igo_count
from read_csv_auto({intergovernmental_organizations_dyadic_path}, normalize_names = false, all_varchar = true);

insert into source_diplomatic_exchange

select
    clean_int(ccode1) c_code_a,
    clean_int(ccode2) c_code_b,
    clean_int(year) "year",
    clean_int(DR_at_1) diplomatic_representation_at_1,
    clean_int(DR_at_2) diplomatic_representation_at_2,
    clean_int(DE) diplomatic_exchange,
    clean_number(version) source_version
from read_csv_auto({diplomatic_exchange_path}, normalize_names = false, all_varchar = true);

insert into source_dd_revisited

select
    clean_int(cowcode) c_code_a,
    clean_int(cowcode2) c_code_b,
    clean_int(year) "year",
    clean_int(emil) e_military_leader,
    clean_int(nmil) n_military_leader,
    clean_int(royal) royal_leader,
    clean_int(comm) communist_leader,
    clean_int(democracy) democratic_regime,
    clean_int(collect) collective_leadership,
    clean_int(regime) regime_type,
    clean_int(incumb) incumbent_type,
    clean_int(exselec) election_type,
    clean_int(legselec) legislature_type_1,
    clean_int(closed) legislature_type_2,
    clean_int(lparty) legislature_party_status,
    clean_int(dejure) party_legal_status,
    clean_int(defacto) party_existence_1,
    clean_int(defacto2) party_existence_2,
    clean_int(edeath) leader_died,
    clean_int(flageh) new_leader,
    clean_int(ttd) transition_to_democracy,
    clean_int(tta) transition_to_dictatorship
from read_csv_auto({dd_revisited_path}, normalize_names = false, all_varchar = true);

insert into source_co_emissions_per_capita

select
    clean_text(entity) country_name,
    clean_text(code) country_code,
    clean_int(year) "year",
    clean_number(emissions_total_per_capita) co2_emissions_per_capita
from read_csv_auto({co_emissions_per_capita_path}, normalize_names = false, all_varchar = true);

insert into source_arms_technology

select
    clean_int(ccode) c_code,
    clean_text(stateabb) state_abbreviation,
    clean_text(statename) state_name,
    clean_text(techname) technology_name,
    clean_text(techtype) technology_type,
    clean_int(year) "year",
    clean_int("use") used,
    clean_int(total_use) total_use,
    clean_number(version) source_version
from read_csv_auto({arms_technology_path}, normalize_names = false, all_varchar = true);

insert into source_atop_dyadic_years

select
    clean_int(ddyad) dyad,
    clean_int(year) "year",
    clean_int(atopally) atop_alliance,
    clean_int(defense) defense,
    clean_int(offense) offense,
    clean_int(neutral) neutrality,
    clean_int(nonagg) nonaggression,
    clean_int(consul) consultation,
    clean_int(shareob) share_obligation,
    clean_int(transyr) transition_year,
    clean_int(bilatno) bilateral_number,
    clean_int(multino) multilateral_number,
    clean_int(number) number,
    clean_int(asymm) asymmetric_alliance,
    clean_int(atopid1) atop_id_1,
    clean_int(atopid2) atop_id_2,
    clean_int(atopid3) atop_id_3,
    clean_int(atopid4) atop_id_4,
    clean_int(atopid5) atop_id_5,
    clean_int(atopid6) atop_id_6,
    clean_int(atopid7) atop_id_7,
    clean_int(atopid8) atop_id_8,
    clean_int(atopid9) atop_id_9,
    clean_int(stateA) c_code_a,
    clean_int(stateB) c_code_b,
    clean_number(version) source_version
from read_csv_auto({atop_dyadic_years_path}, normalize_names = false, all_varchar = true);

insert into source_mtops_dyadic

select
    clean_int(state1) c_code_a,
    clean_int(state2) c_code_b,
    clean_int(year) "year",
    clean_int(pacsettg) pacific_settlement_general,
    clean_int(pacsettr) pacific_settlement_regional,
    clean_int(pacsett) pacific_settlement,
    clean_int(tergen) territorial_general,
    clean_int(terviol) territorial_violence,
    clean_int(tertot) territorial_total
from read_csv_auto({mtops_dyadic_path}, normalize_names = false, all_varchar = true);

insert into source_cow_trade_dyadic

select
    clean_int(ccode1) c_code_a,
    clean_int(ccode2) c_code_b,
    clean_int(year) "year",
    clean_int(importer1) importer_1,
    clean_int(importer2) importer_2,
    clean_number(flow1) flow_1,
    clean_number(flow2) flow_2,
    clean_number(smoothflow1) smooth_flow_1,
    clean_number(smoothflow2) smooth_flow_2,
    clean_number(smoothtotrade) smooth_total_trade,
    clean_int(spike1) spike_1,
    clean_int(spike2) spike_2,
    clean_int(dip1) dip_1,
    clean_int(dip2) dip_2,
    clean_int(trdspike) trade_spike,
    clean_int(tradedip) trade_dip,
    clean_number(bel_lux_alt_flow1) bel_lux_alt_flow_1,
    clean_number(bel_lux_alt_flow2) bel_lux_alt_flow_2,
    clean_number(china_alt_flow1) china_alt_flow_1,
    clean_number(china_alt_flow2) china_alt_flow_2,
    clean_text(source1) source_1,
    clean_text(source2) source_2,
    clean_number(version) source_version
from read_csv_auto({cow_trade_dyadic_path}, normalize_names = false, all_varchar = true);

insert into source_cow_trade_national

select
    clean_int(ccode) c_code,
    clean_text(statename) state_name,
    clean_text(stateabb) state_abbreviation,
    clean_int(year) "year",
    clean_number(imports) imports,
    clean_number(exports) exports,
    clean_number(alt_imports) alt_imports,
    clean_number(alt_exports) alt_exports,
    clean_text(source1) source_1,
    clean_text(source2) source_2,
    clean_number(version) source_version
from read_csv_auto({cow_trade_national_path}, normalize_names = false, all_varchar = true);

insert into source_national_material_capabilities

select
    clean_text(statenme) state_name,
    clean_text(stateabb) state_abbreviation,
    clean_int(ccode) c_code,
    clean_int(year) "year",
    clean_number(milex) military_expenditures,
    clean_text(milexsource) military_expenditures_source,
    clean_text(milexnote) military_expenditures_note,
    clean_number(milper) military_personnel,
    clean_text(milpersource) military_personnel_source,
    clean_text(milpernote) military_personnel_note,
    clean_number(irst) iron_and_steel_production,
    clean_text(irstsource) iron_and_steel_production_source,
    clean_text(irstnote) iron_and_steel_production_note,
    clean_int(irstqualitycode) iron_and_steel_quality_code,
    clean_int(irstanomalycode) iron_and_steel_anomaly_code,
    clean_number(pec) primary_energy_consumption,
    clean_text(pecsource) primary_energy_consumption_source,
    clean_text(pecnote) primary_energy_consumption_note,
    clean_int(pecqualitycode) primary_energy_consumption_quality_code,
    clean_int(pecanomalycode) primary_energy_consumption_anomaly_code,
    clean_number(tpop) total_population,
    clean_text(tpopsource) total_population_source,
    clean_text(tpopnote) total_population_note,
    clean_int(tpopqualitycode) total_population_quality_code,
    clean_int(tpopanomalycode) total_population_anomaly_code,
    clean_number(upop) urban_population,
    clean_text(upopsource) urban_population_source,
    clean_text(upopnote) urban_population_note,
    clean_int(upopqualitycode) urban_population_quality_code,
    clean_int(upopanomalycode) urban_population_anomaly_code,
    clean_number(upopgrowth) urban_population_growth,
    clean_text(upopgrowthsource) urban_population_growth_source,
    clean_number(cinc) composite_index_of_national_capability,
    clean_number(version) source_version
from read_csv_auto({national_material_capabilities_path}, normalize_names = false, all_varchar = true);
