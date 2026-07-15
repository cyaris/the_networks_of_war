create or replace table d3_war_nodes as

select
    row_number() over (partition by war_num order by side nulls last, c_code, participant) - 1 id,
    war_num,
    war_type_code,
    c_code,
    participant,
    side,
    battle_deaths,
    battle_deaths_estimated,
    start_date,
    end_date,
    start_year,
    end_year,
    ongoing_conflict,
    start_date_estimated,
    end_date_estimated,
    lagging_war,
    leading_war,
    if(c_code > 0, c_code::varchar, participant) node_key,
    columns('.*_[xyz]$')
from final_participants;
