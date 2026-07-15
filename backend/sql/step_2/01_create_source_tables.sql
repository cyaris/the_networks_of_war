create or replace table source_global_terrorism_database (
    source_file varchar,
    event_id varchar,
    year integer,
    country_name varchar,
    killed double,
    wounded double,
    hostages_kidnapped double
);

create or replace table source_formal_alliances_directed_yearly (
    version_4_id integer,
    c_code_a integer,
    c_code_b integer,
    state_name_a varchar,
    state_name_b varchar,
    dyad_start_day integer,
    dyad_start_month integer,
    dyad_start_year integer,
    dyad_end_day integer,
    dyad_end_month integer,
    dyad_end_year integer,
    left_censor integer,
    right_censor integer,
    defense integer,
    neutrality integer,
    nonaggression integer,
    entente integer,
    year integer,
    source_version double
);

create or replace table source_territorial_changes (
    year integer,
    month integer,
    gainer integer,
    gain_type integer,
    procedure integer,
    entity varchar,
    contiguous_gain integer,
    area double,
    population double,
    portion integer,
    loser integer,
    lose_type integer,
    contiguous_loss integer,
    entry integer,
    exit integer,
    number integer,
    independent integer,
    conflict integer,
    source_version double
);

create or replace table source_forcibly_displaced_populations (
    c_code integer,
    state_abbreviation varchar,
    country_name varchar,
    year integer,
    source double,
    internally_displaced_persons double,
    hosted_refugees double
);

create or replace table source_colonial_dependency_contiguity (
    dyad integer,
    c_code_a integer,
    c_code_b integer,
    state_abbreviation_a varchar,
    state_abbreviation_b varchar,
    year integer,
    land integer,
    sea integer,
    total integer,
    source_version double
);

create or replace table source_direct_contiguity (
    dyad integer,
    c_code_a integer,
    c_code_b integer,
    state_abbreviation_a varchar,
    state_abbreviation_b varchar,
    year integer,
    contiguity_type integer,
    source_version double
);

create or replace table source_defense_cooperation_agreements (
    c_code_a integer,
    c_code_b integer,
    state_abbreviation_a varchar,
    state_abbreviation_b varchar,
    year integer,
    dca_general_v1 integer,
    dca_general_v2 integer,
    dca_sector_v1 integer,
    dca_sector_v2 integer,
    dca_any_v1 integer,
    dca_any_v2 integer
);

create or replace table source_intergovernmental_organizations_dyadic (
    c_code_a integer,
    c_code_b integer,
    country_name_a varchar,
    country_name_b varchar,
    year integer,
    shared_igo_count integer
);

create or replace table source_diplomatic_exchange (
    c_code_a integer,
    c_code_b integer,
    year integer,
    diplomatic_representation_at_1 integer,
    diplomatic_representation_at_2 integer,
    diplomatic_exchange integer,
    source_version double
);

create or replace table source_dd_revisited (
    c_code_a integer,
    c_code_b integer,
    year integer,
    e_military_leader integer,
    n_military_leader integer,
    royal_leader integer,
    communist_leader integer,
    democratic_regime integer,
    collective_leadership integer,
    regime_type integer,
    incumbent_type integer,
    election_type integer,
    legislature_type_1 integer,
    legislature_type_2 integer,
    legislature_party_status integer,
    party_legal_status integer,
    party_existence_1 integer,
    party_existence_2 integer,
    leader_died integer,
    new_leader integer,
    transition_to_democracy integer,
    transition_to_dictatorship integer
);

create or replace table source_co_emissions_per_capita (
    country_name varchar,
    country_code varchar,
    year integer,
    co2_emissions_per_capita double
);

create or replace table source_arms_technology (
    c_code integer,
    state_abbreviation varchar,
    state_name varchar,
    technology_name varchar,
    technology_type varchar,
    year integer,
    used integer,
    total_use integer,
    source_version double
);

create or replace table source_atop_dyadic_years (
    dyad integer,
    year integer,
    atop_alliance integer,
    defense integer,
    offense integer,
    neutrality integer,
    nonaggression integer,
    consultation integer,
    share_obligation integer,
    transition_year integer,
    bilateral_number integer,
    multilateral_number integer,
    number integer,
    asymmetric_alliance integer,
    atop_id_1 integer,
    atop_id_2 integer,
    atop_id_3 integer,
    atop_id_4 integer,
    atop_id_5 integer,
    atop_id_6 integer,
    atop_id_7 integer,
    atop_id_8 integer,
    atop_id_9 integer,
    c_code_a integer,
    c_code_b integer,
    source_version double
);

create or replace table source_mtops_dyadic (
    c_code_a integer,
    c_code_b integer,
    year integer,
    pacific_settlement_general integer,
    pacific_settlement_regional integer,
    pacific_settlement integer,
    territorial_general integer,
    territorial_violence integer,
    territorial_total integer
);

create or replace table source_cow_trade_dyadic (
    c_code_a integer,
    c_code_b integer,
    year integer,
    importer_1 integer,
    importer_2 integer,
    flow_1 double,
    flow_2 double,
    smooth_flow_1 double,
    smooth_flow_2 double,
    smooth_total_trade double,
    spike_1 integer,
    spike_2 integer,
    dip_1 integer,
    dip_2 integer,
    trade_spike integer,
    trade_dip integer,
    bel_lux_alt_flow_1 double,
    bel_lux_alt_flow_2 double,
    china_alt_flow_1 double,
    china_alt_flow_2 double,
    source_1 varchar,
    source_2 varchar,
    source_version double
);

create or replace table source_cow_trade_national (
    c_code integer,
    state_name varchar,
    state_abbreviation varchar,
    year integer,
    imports double,
    exports double,
    alt_imports double,
    alt_exports double,
    source_1 varchar,
    source_2 varchar,
    source_version double
);

create or replace table source_national_material_capabilities (
    state_name varchar,
    state_abbreviation varchar,
    c_code integer,
    year integer,
    military_expenditures double,
    military_expenditures_source varchar,
    military_expenditures_note varchar,
    military_personnel double,
    military_personnel_source varchar,
    military_personnel_note varchar,
    iron_and_steel_production double,
    iron_and_steel_production_source varchar,
    iron_and_steel_production_note varchar,
    iron_and_steel_quality_code integer,
    iron_and_steel_anomaly_code integer,
    primary_energy_consumption double,
    primary_energy_consumption_source varchar,
    primary_energy_consumption_note varchar,
    primary_energy_consumption_quality_code integer,
    primary_energy_consumption_anomaly_code integer,
    total_population double,
    total_population_source varchar,
    total_population_note varchar,
    total_population_quality_code integer,
    total_population_anomaly_code integer,
    urban_population double,
    urban_population_source varchar,
    urban_population_note varchar,
    urban_population_quality_code integer,
    urban_population_anomaly_code integer,
    urban_population_growth double,
    urban_population_growth_source varchar,
    composite_index_of_national_capability double,
    source_version double
);
