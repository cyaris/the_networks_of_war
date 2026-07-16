create or replace table final_dyads as

with

dyad_rows as (

select
    a.war_id,
    b.war_name,
    a.c_code_a,
    a.c_code_b,
    a.participant_a,
    a.participant_b,
    a.start_date,
    if(b.ongoing_war = 1, null, a.end_date) end_date,
    extract(year from a.start_date)::integer start_year,
    if(b.ongoing_war = 1, null, extract(year from a.end_date)::integer) end_year,
    a.start_date_estimated,
    a.end_date_estimated
from dyads a
join wars b on a.war_id = b.war_id),

dyad_descriptor_values as (

select
    war_id,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    timeframe,
    if(columns('^(territory_exchange|colonial_contiguity|contiguity|alliance|defense_cooperation_agreements|inter_governmental_organizations|diplomatic_exchange|trade_relations|same_leader_type|military_leaders|communist_leaders|royal_leaders|democratic_incumbent|unconstitutional_incumbent|democratic_regimes|dictatorships|collective_leaderships|direct_election|indirect_election|non_elected_leaders|no_legislature|non_elective_legislature|elective_legislature|no_partisan_legislature_legal|no_non_regime_legislature_parties_legal|multi_party_legislature_legal|all_parties_illegal|single_party_state_exists|multi_party_state_exists|no_parties_exist|one_party_exists|no_non_regime_parties_exist|leader_died|new_leader|transition_to_democracy|transition_to_dictatorship|atop|mtops)$') in (-9, -8) or (columns('^(territory_exchange|colonial_contiguity|contiguity|alliance|defense_cooperation_agreements|inter_governmental_organizations|diplomatic_exchange|trade_relations|same_leader_type|military_leaders|communist_leaders|royal_leaders|democratic_incumbent|unconstitutional_incumbent|democratic_regimes|dictatorships|collective_leaderships|direct_election|indirect_election|non_elected_leaders|no_legislature|non_elective_legislature|elective_legislature|no_partisan_legislature_legal|no_non_regime_legislature_parties_legal|multi_party_legislature_legal|all_parties_illegal|single_party_state_exists|multi_party_state_exists|no_parties_exist|one_party_exists|no_non_regime_parties_exist|leader_died|new_leader|transition_to_democracy|transition_to_dictatorship|atop|mtops)$') is null and (c_code_a <= 0 or c_code_b <= 0)), null, coalesce(columns('^(territory_exchange|colonial_contiguity|contiguity|alliance|defense_cooperation_agreements|inter_governmental_organizations|diplomatic_exchange|trade_relations|same_leader_type|military_leaders|communist_leaders|royal_leaders|democratic_incumbent|unconstitutional_incumbent|democratic_regimes|dictatorships|collective_leaderships|direct_election|indirect_election|non_elected_leaders|no_legislature|non_elective_legislature|elective_legislature|no_partisan_legislature_legal|no_non_regime_legislature_parties_legal|multi_party_legislature_legal|all_parties_illegal|single_party_state_exists|multi_party_state_exists|no_parties_exist|one_party_exists|no_non_regime_parties_exist|leader_died|new_leader|transition_to_democracy|transition_to_dictatorship|atop|mtops)$'), 0))
from dyadic_descriptives),

available_link_fields as (

select
    war_id,
    timeframe,
    field
from dyad_descriptor_values
unpivot include nulls (value for field in (columns('^(territory_exchange|colonial_contiguity|contiguity|alliance|defense_cooperation_agreements|inter_governmental_organizations|diplomatic_exchange|trade_relations|same_leader_type|military_leaders|communist_leaders|royal_leaders|democratic_incumbent|unconstitutional_incumbent|democratic_regimes|dictatorships|collective_leaderships|direct_election|indirect_election|non_elected_leaders|no_legislature|non_elective_legislature|elective_legislature|no_partisan_legislature_legal|no_non_regime_legislature_parties_legal|multi_party_legislature_legal|all_parties_illegal|single_party_state_exists|multi_party_state_exists|no_parties_exist|one_party_exists|no_non_regime_parties_exist|leader_died|new_leader|transition_to_democracy|transition_to_dictatorship|atop|mtops)$')))
group by 1, 2, 3
having max(if(value > 0, 1, 0)) = 1),

link_field_json as (

select
    a.war_id,
    a.c_code_a,
    a.c_code_b,
    a.participant_a,
    a.participant_b,
    a.timeframe,
    to_json(map(list(a.field order by a.field), list(a.value order by a.field))) payload
from dyad_descriptor_values
unpivot include nulls (value for field in (columns('^(territory_exchange|colonial_contiguity|contiguity|alliance|defense_cooperation_agreements|inter_governmental_organizations|diplomatic_exchange|trade_relations|same_leader_type|military_leaders|communist_leaders|royal_leaders|democratic_incumbent|unconstitutional_incumbent|democratic_regimes|dictatorships|collective_leaderships|direct_election|indirect_election|non_elected_leaders|no_legislature|non_elective_legislature|elective_legislature|no_partisan_legislature_legal|no_non_regime_legislature_parties_legal|multi_party_legislature_legal|all_parties_illegal|single_party_state_exists|multi_party_state_exists|no_parties_exist|one_party_exists|no_non_regime_parties_exist|leader_died|new_leader|transition_to_democracy|transition_to_dictatorship|atop|mtops)$'))) a
join available_link_fields b on a.war_id = b.war_id
                            and a.timeframe = b.timeframe
                            and a.field = b.field
group by 1, 2, 3, 4, 5, 6),

link_descriptor_json as (

select
    war_id,
    c_code_a,
    c_code_b,
    participant_a,
    participant_b,
    json_group_object(lower(replace(timeframe, ' ', '_')), json(payload)) payload
from link_field_json
group by 1, 2, 3, 4, 5),

final_dyad_rows as (

select
    a.*,
    d.payload descriptor_timeframes
from dyad_rows a
left join link_descriptor_json d on a.war_id = d.war_id
                                and a.c_code_a = d.c_code_a
                                and a.c_code_b = d.c_code_b
                                and a.participant_a = d.participant_a
                                and a.participant_b = d.participant_b)

select
    a.*,
    b.id "source",
    c.id "target"
from final_dyad_rows a
join final_participants b on a.war_id = b.war_id
                         and if(a.c_code_a > 0, a.c_code_a::varchar, a.participant_a) = b.node_key
join final_participants c on a.war_id = c.war_id
                         and if(a.c_code_b > 0, a.c_code_b::varchar, a.participant_b) = c.node_key;
