create or replace table source_file_versions (
    source_key varchar,
    source_file varchar,
    source_version varchar
);

create or replace table source_interstate_mid_war_num_adjustments (
    source_key varchar,
    source_version varchar,
    disno double,
    war_num double
);

create or replace table source_interstate_war_metadata_adjustments (
    source_key varchar,
    source_version varchar,
    war_num double,
    war_name varchar,
    war_type integer
);

create or replace table source_participant_side_adjustments (
    source_key varchar,
    source_version varchar,
    war_num double,
    c_code integer,
    participant varchar,
    side integer
);
