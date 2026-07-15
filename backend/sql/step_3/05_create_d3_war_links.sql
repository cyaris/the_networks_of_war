create or replace table d3_war_links as

select
    a.war_num,
    b.id "source",
    c.id "target",
    a.start_date_estimated,
    a.end_date_estimated,
    columns('.*_[xyz]$')
from final_dyads a
join d3_war_nodes b on a.war_num = b.war_num
                    and if(a.c_code_a > 0, a.c_code_a::varchar, a.participant_a) = b.node_key
join d3_war_nodes c on a.war_num = c.war_num
                    and if(a.c_code_b > 0, a.c_code_b::varchar, a.participant_b) = c.node_key;
