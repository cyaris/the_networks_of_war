create or replace table dyad_year_descriptives as

with

territory_exchange_years as (

select
    least(gainer, loser) c_code_low,
    greatest(gainer, loser) c_code_high,
    year,
    1 territory_exchange
from source_territorial_changes
where
    gainer > 0
    and loser > 0
group by 1, 2, 3, 4),

colonial_contiguity_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 colonial_contiguity
from source_colonial_dependency_contiguity
where
    c_code_a > 0
    and c_code_b > 0
group by 1, 2, 3, 4),

contiguity_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 contiguity
from source_direct_contiguity
where
    c_code_a > 0
    and c_code_b > 0
group by 1, 2, 3, 4),

alliance_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 alliance
from source_formal_alliances_directed_yearly
where
    c_code_a > 0
    and c_code_b > 0
    and coalesce(defense, 0) + coalesce(neutrality, 0) + coalesce(nonaggression, 0) + coalesce(entente, 0) > 0
group by 1, 2, 3, 4),

defense_cooperation_agreement_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 defense_cooperation_agreements
from source_defense_cooperation_agreements
where
    c_code_a > 0
    and c_code_b > 0
group by 1, 2, 3, 4),

intergovernmental_organization_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 inter_governmental_organizations
from source_intergovernmental_organizations_dyadic
where
    c_code_a > 0
    and c_code_b > 0
    and shared_igo_count > 0
group by 1, 2, 3, 4),

diplomatic_exchange_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 diplomatic_exchange
from source_diplomatic_exchange
where
    c_code_a > 0
    and c_code_b > 0
group by 1, 2, 3, 4),

trade_relation_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 trade_relations
from source_cow_trade_dyadic
where
    c_code_a > 0
    and c_code_b > 0
    and (flow_1 > 0 or flow_2 > 0)
group by 1, 2, 3, 4),

dd_country_years as (

select
    c_code_a c_code,
    year,
    e_military_leader,
    n_military_leader,
    royal_leader,
    communist_leader,
    democratic_regime,
    collective_leadership,
    regime_type,
    incumbent_type,
    election_type,
    legislature_type_1,
    legislature_type_2,
    legislature_party_status,
    party_legal_status,
    party_existence_1,
    party_existence_2,
    leader_died,
    new_leader,
    transition_to_democracy,
    transition_to_dictatorship
from source_dd_revisited
where c_code_a is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
union
select
    c_code_b c_code,
    year,
    e_military_leader,
    n_military_leader,
    royal_leader,
    communist_leader,
    democratic_regime,
    collective_leadership,
    regime_type,
    incumbent_type,
    election_type,
    legislature_type_1,
    legislature_type_2,
    legislature_party_status,
    party_legal_status,
    party_existence_1,
    party_existence_2,
    leader_died,
    new_leader,
    transition_to_democracy,
    transition_to_dictatorship
from source_dd_revisited
where c_code_b is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),

dd_dyad_years as (

select
    c.war_id,
    c.c_code_a,
    c.c_code_b,
    c.participant_a,
    c.participant_b,
    c.year,
    if(a.e_military_leader = b.e_military_leader and a.n_military_leader = b.n_military_leader and a.communist_leader = b.communist_leader and a.royal_leader = b.royal_leader and a.democratic_regime = b.democratic_regime, 1, 0) same_leader_type,
    if((a.e_military_leader = 1 and b.e_military_leader = 1) or (a.n_military_leader = 1 and b.n_military_leader = 1), 1, 0) military_leaders,
    if(a.communist_leader = 1 and b.communist_leader = 1, 1, 0) communist_leaders,
    if(a.royal_leader = 1 and b.royal_leader = 1, 1, 0) royal_leaders,
    if(a.incumbent_type = 1 and b.incumbent_type = 1, 1, 0) democratic_incumbent,
    if(a.incumbent_type = 2 and b.incumbent_type = 2, 1, 0) unconstitutional_incumbent,
    if((a.incumbent_type = 1 or a.democratic_regime = 1 or a.regime_type in (0, 1, 2)) and (b.incumbent_type = 1 or b.democratic_regime = 1 or b.regime_type in (0, 1, 2)), 1, 0) democratic_regimes,
    if(a.regime_type in (3, 4, 5) and b.regime_type in (3, 4, 5), 1, 0) dictatorships,
    if(a.collective_leadership = 1 and b.collective_leadership = 1, 1, 0) collective_leaderships,
    if(a.election_type = 1 and b.election_type = 1, 1, 0) direct_election,
    if(a.election_type = 2 and b.election_type = 2, 1, 0) indirect_election,
    if(a.election_type = 3 and b.election_type = 3, 1, 0) non_elected_leaders,
    if(a.legislature_type_1 = 0 and b.legislature_type_1 = 0, 1, 0) no_legislature,
    if(a.legislature_type_1 = 1 and b.legislature_type_1 = 1, 1, 0) non_elective_legislature,
    if(a.legislature_type_1 = 2 and b.legislature_type_1 = 2, 1, 0) elective_legislature,
    if(a.legislature_party_status = 0 and b.legislature_party_status = 0, 1, 0) no_partisan_legislature_legal,
    if(a.legislature_party_status = 1 and b.legislature_party_status = 1, 1, 0) no_non_regime_legislature_parties_legal,
    if(a.legislature_party_status = 2 and b.legislature_party_status = 2, 1, 0) multi_party_legislature_legal,
    if(a.party_legal_status = 0 and b.party_legal_status = 0, 1, 0) all_parties_illegal,
    if(a.party_legal_status = 1 and b.party_legal_status = 1, 1, 0) single_party_state_exists,
    if(a.party_legal_status = 2 and b.party_legal_status = 2, 1, 0) multi_party_state_exists,
    if(a.party_existence_1 = 0 and b.party_existence_1 = 0 and a.party_existence_2 = 0 and b.party_existence_2 = 0, 1, 0) no_parties_exist,
    if(a.party_existence_1 = 1 and b.party_existence_1 = 1, 1, 0) one_party_exists,
    if(a.party_existence_2 = 1 and b.party_existence_2 = 1, 1, 0) no_non_regime_parties_exist,
    if(a.leader_died = 0 and b.leader_died = 0, 1, 0) leader_died,
    if(a.new_leader = 1 and b.new_leader = 1, 1, 0) new_leader,
    if(a.transition_to_democracy = 1 and b.transition_to_democracy = 1, 1, 0) transition_to_democracy,
    if(a.transition_to_dictatorship = 1 and b.transition_to_dictatorship = 1, 1, 0) transition_to_dictatorship
from dyad_years c
join dd_country_years a on c.c_code_a = a.c_code
                        and c.year = a.year
join dd_country_years b on c.c_code_b = b.c_code
                        and c.year = b.year
where
    c.c_code_a > 0
    and c.c_code_b > 0
    and a.c_code != b.c_code
group by all),

atop_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 atop
from source_atop_dyadic_years
where
    c_code_a > 0
    and c_code_b > 0
group by 1, 2, 3, 4),

mtops_years as (

select
    least(c_code_a, c_code_b) c_code_low,
    greatest(c_code_a, c_code_b) c_code_high,
    year,
    1 mtops
from source_mtops_dyadic
where
    c_code_a > 0
    and c_code_b > 0
    and coalesce(pacific_settlement_general, 0) + coalesce(pacific_settlement_regional, 0) + coalesce(pacific_settlement, 0) + coalesce(territorial_general, 0) + coalesce(territorial_violence, 0) + coalesce(territorial_total, 0) > 0
group by 1, 2, 3, 4),

shared_arms_technology_years as (

select
    least(a.c_code, b.c_code) c_code_low,
    greatest(a.c_code, b.c_code) c_code_high,
    a.year,
    1 shared_arms_technology
from source_arms_technology a
join source_arms_technology b on a.technology_name = b.technology_name
                              and a.year = b.year
                              and a.c_code != b.c_code
where
    a.c_code > 0
    and b.c_code > 0
    and a.technology_name != 'Adopted technologies'
    and a.used in (1, 9)
    and b.used in (1, 9)
group by 1, 2, 3, 4)

select
    a.war_id,
    b.war_name,
    a.c_code_a,
    a.c_code_b,
    a.participant_a,
    a.participant_b,
    a.start_date,
    a.end_date,
    extract(year from a.start_date)::integer start_year,
    extract(year from a.end_date)::integer end_year,
    a.start_date_estimated,
    a.end_date_estimated,
    a.year,
    coalesce(c.territory_exchange, 0) territory_exchange,
    coalesce(d.colonial_contiguity, 0) colonial_contiguity,
    coalesce(e.contiguity, 0) contiguity,
    coalesce(f.alliance, 0) alliance,
    coalesce(g.defense_cooperation_agreements, 0) defense_cooperation_agreements,
    coalesce(h.inter_governmental_organizations, 0) inter_governmental_organizations,
    coalesce(i.diplomatic_exchange, 0) diplomatic_exchange,
    coalesce(j.trade_relations, 0) trade_relations,
    coalesce(k.same_leader_type, 0) same_leader_type,
    coalesce(k.military_leaders, 0) military_leaders,
    coalesce(k.communist_leaders, 0) communist_leaders,
    coalesce(k.royal_leaders, 0) royal_leaders,
    coalesce(k.democratic_incumbent, 0) democratic_incumbent,
    coalesce(k.unconstitutional_incumbent, 0) unconstitutional_incumbent,
    coalesce(k.democratic_regimes, 0) democratic_regimes,
    coalesce(k.dictatorships, 0) dictatorships,
    coalesce(k.collective_leaderships, 0) collective_leaderships,
    coalesce(k.direct_election, 0) direct_election,
    coalesce(k.indirect_election, 0) indirect_election,
    coalesce(k.non_elected_leaders, 0) non_elected_leaders,
    coalesce(k.no_legislature, 0) no_legislature,
    coalesce(k.non_elective_legislature, 0) non_elective_legislature,
    coalesce(k.elective_legislature, 0) elective_legislature,
    coalesce(k.no_partisan_legislature_legal, 0) no_partisan_legislature_legal,
    coalesce(k.no_non_regime_legislature_parties_legal, 0) no_non_regime_legislature_parties_legal,
    coalesce(k.multi_party_legislature_legal, 0) multi_party_legislature_legal,
    coalesce(k.all_parties_illegal, 0) all_parties_illegal,
    coalesce(k.single_party_state_exists, 0) single_party_state_exists,
    coalesce(k.multi_party_state_exists, 0) multi_party_state_exists,
    coalesce(k.no_parties_exist, 0) no_parties_exist,
    coalesce(k.one_party_exists, 0) one_party_exists,
    coalesce(k.no_non_regime_parties_exist, 0) no_non_regime_parties_exist,
    coalesce(k.leader_died, 0) leader_died,
    coalesce(k.new_leader, 0) new_leader,
    coalesce(k.transition_to_democracy, 0) transition_to_democracy,
    coalesce(k.transition_to_dictatorship, 0) transition_to_dictatorship,
    coalesce(l.atop, 0) atop,
    coalesce(m.mtops, 0) mtops,
    coalesce(n.shared_arms_technology, 0) shared_arms_technology
from dyad_years a
join wars b on a.war_id = b.war_id
left join territory_exchange_years c on least(a.c_code_a, a.c_code_b) = c.c_code_low
                                     and greatest(a.c_code_a, a.c_code_b) = c.c_code_high
                                     and a.year = c.year
left join colonial_contiguity_years d on least(a.c_code_a, a.c_code_b) = d.c_code_low
                                      and greatest(a.c_code_a, a.c_code_b) = d.c_code_high
                                      and a.year = d.year
left join contiguity_years e on least(a.c_code_a, a.c_code_b) = e.c_code_low
                             and greatest(a.c_code_a, a.c_code_b) = e.c_code_high
                             and a.year = e.year
left join alliance_years f on least(a.c_code_a, a.c_code_b) = f.c_code_low
                           and greatest(a.c_code_a, a.c_code_b) = f.c_code_high
                           and a.year = f.year
left join defense_cooperation_agreement_years g on least(a.c_code_a, a.c_code_b) = g.c_code_low
                                                and greatest(a.c_code_a, a.c_code_b) = g.c_code_high
                                                and a.year = g.year
left join intergovernmental_organization_years h on least(a.c_code_a, a.c_code_b) = h.c_code_low
                                                 and greatest(a.c_code_a, a.c_code_b) = h.c_code_high
                                                 and a.year = h.year
left join diplomatic_exchange_years i on least(a.c_code_a, a.c_code_b) = i.c_code_low
                                      and greatest(a.c_code_a, a.c_code_b) = i.c_code_high
                                      and a.year = i.year
left join trade_relation_years j on least(a.c_code_a, a.c_code_b) = j.c_code_low
                                 and greatest(a.c_code_a, a.c_code_b) = j.c_code_high
                                 and a.year = j.year
left join dd_dyad_years k on a.war_id = k.war_id
                          and a.c_code_a = k.c_code_a
                          and a.c_code_b = k.c_code_b
                          and a.participant_a = k.participant_a
                          and a.participant_b = k.participant_b
                          and a.year = k.year
left join atop_years l on least(a.c_code_a, a.c_code_b) = l.c_code_low
                       and greatest(a.c_code_a, a.c_code_b) = l.c_code_high
                       and a.year = l.year
left join mtops_years m on least(a.c_code_a, a.c_code_b) = m.c_code_low
                        and greatest(a.c_code_a, a.c_code_b) = m.c_code_high
                        and a.year = m.year
left join shared_arms_technology_years n on least(a.c_code_a, a.c_code_b) = n.c_code_low
                                        and greatest(a.c_code_a, a.c_code_b) = n.c_code_high
                                        and a.year = n.year;
