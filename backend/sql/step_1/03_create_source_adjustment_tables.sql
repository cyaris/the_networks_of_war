create or replace table source_file_versions (
    source_key varchar,
    source_file varchar,
    source_version varchar
);

create or replace table source_interstate_mid_war_id_adjustments (
    source_key varchar,
    source_version varchar,
    disno double,
    war_id double
);

create or replace table source_interstate_war_metadata_adjustments (
    source_key varchar,
    source_version varchar,
    war_id double,
    war_name varchar,
    war_type_id integer
);

create or replace table source_interstate_war_dyad_adjustments (
    source_key varchar,
    source_version varchar,
    war_id double,
    c_code_a integer,
    c_code_b integer,
    start_date date,
    end_date date
);

create or replace table source_participant_side_adjustments (
    source_key varchar,
    source_version varchar,
    war_id double,
    c_code integer,
    participant varchar,
    side integer
);
