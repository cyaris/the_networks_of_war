create or replace table participant_name_replacements as

select
    clean_text(source) as source,
    clean_text(replacement) as replacement
from read_json_auto({participant_name_replacements_path})
where
    clean_text(source) is not null
    and clean_text(replacement) is not null;

create or replace macro clean_participant(value, replacement) as (
    coalesce(replacement, clean_text(value))
);
