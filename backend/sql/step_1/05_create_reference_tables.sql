create or replace table country_codes as

select
    c_code,
    state_abbreviation,
    state_name
from source_country_codes
where c_code is not null
qualify row_number() over (partition by c_code) = 1;

create or replace table war_types (
    war_type integer,
    war_type_name varchar,
    war_subtype varchar
);
