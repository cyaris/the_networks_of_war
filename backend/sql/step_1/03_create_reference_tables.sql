create or replace table country_codes as

select
    clean_int("CCode") c_code,
    clean_text("StateAbb") state_abbreviation,
    clean_text("StateNme") state_name
from source_country_codes
where clean_int("CCode") is not null
qualify row_number() over (partition by clean_int("CCode") order by clean_text("StateNme"), clean_text("StateAbb")) = 1;

create or replace table war_types as

select
    clean_int("war_type_code") war_type,
    clean_text("war_type") war_type_name,
    clean_text("war_subtype") war_subtype
from source_war_types;
