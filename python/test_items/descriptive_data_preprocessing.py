# import pandas as pd
# import numpy as np
# from copy import deepcopy
# from traceback import format_exc
# from pprint import pprint
# import the_networks_of_war_python_functions
#
# pd.set_option('display.max_columns', None)
# pd.set_option('display.float_format', lambda x: '%.5f' % x)
#
# ################################################################################
#
# ## Descriptive Statistics for Each Country by Year
# ### Note: Applies to states/countries only.
# ### This will be joined to the participants of each war
#
# print('\nDescriptive Statistics for Each Country by Year')
# csv_directory = '/Users/the_networks_of_war/data_sources/csvs/'
# part_df_1 = pd.read_csv(csv_directory + 'alliance_v4.1_by_member_yearly.csv', encoding = 'latin-1')
#
# part_df_1.rename({'ccode': 'c_code',
#                   'defense': 'defense_alliances',
#                   'entente': 'entente_alliances',
#                   'neutrality': 'neutrality_alliances',
#                   'ss_type': 'alliances'}, axis=1, inplace=True)
#
# part_df_1['defense_alliances'] = part_df_1['defense_alliances'].astype(float)
# part_df_1['entente_alliances'] = part_df_1['entente_alliances'].astype(float)
# part_df_1['neutrality_alliances'] = part_df_1['neutrality_alliances'].astype(float)
#
# aggregations = {
#     'alliances': 'count',
#     'defense_alliances': 'sum',
#     'entente_alliances': 'sum',
#     'neutrality_alliances': 'sum',
#     }
#
# part_df_1 = deepcopy(part_df_1.groupby(['c_code', 'year']).agg(aggregations).reset_index())
#
# ## dyadic trade data that will need to be adjusted to be non-dyadic (by country, by year)
# part_df_2_1 = pd.read_csv(csv_directory + 'Dyadic_COW_4.0.csv', encoding = 'utf8')
# part_df_2_1.rename({'ccode1': 'c_code_a',
#                     'ccode2': 'c_code_b',
#                     'flow2': 'money_flow_in_a',
#                     ## money flow out
#                     'flow1': 'money_flow_in_b'}, axis=1, inplace=True)
#
# ## need to union to take summations but won't need to dedupe because there are no duplicates between a and b.
# # this means a can be summed on its own when it's combined with b.
# switched_columns_list = ['c_code_a',
#                          'c_code_b',
#                          'money_flow_in_a',
#                          'money_flow_in_b']
# part_df_2_1 = deepcopy(the_networks_of_war_python_functions.union_opposite_columns(part_df_2_1, switched_columns_list))
# part_df_2_1.rename({'money_flow_in_a': 'money_flow_in',
#                     'money_flow_in_b': 'money_flow_out'}, axis=1, inplace=True)
#
# ################################################################################
#
# aggregations = {'money_flow_in': 'sum',
#                 'money_flow_out': 'sum'}
# part_df_2_1 = part_df_2_1.groupby(['c_code_a', 'year']).agg(aggregations).reset_index()
# part_df_2_1.rename({'c_code_a':'c_code'}, axis=1, inplace=True)
#
# part_df_2_2 = pd.read_csv(csv_directory + 'National_COW_4.0.csv', encoding = 'latin-1')
# part_df_2_2.rename({'ccode': 'c_code'}, axis=1, inplace=True)
#
# part_df_2_2 = deepcopy(part_df_2_2[['c_code', 'year', 'imports', 'exports']])
#
# part_df_2 = deepcopy(pd.merge(part_df_2_1, part_df_2_2, how = 'outer', on = ['c_code', 'year']))
#
# ################################################################################
#
# part_df_3 = pd.read_csv(csv_directory + 'NMC_5_0-wsupplementary.csv', encoding = 'latin-1')
#
# part_df_3.rename({'milex': 'military_expenditure',
#                   'milper': 'military_personnel',
#                   'irst': 'iron_steel_production',
#                   'pec': 'prim_energy_consumption',
#                   'tpop': 'total_population',
#                   'upop': 'urban_population',
#                   'upopgrowth': 'urban_pop_growth_rate',
#                   'ccode': 'c_code',
#                   'cinc': 'cinc_score'}, axis=1, inplace=True)
#
# part_df_3 = part_df_3.sort_values(by = 'year', ascending = True).reset_index(drop = True)
# part_df_3 = deepcopy(part_df_3[['c_code',
#                                 'year',
#                                 'military_expenditure',
#                                 'military_personnel',
#                                 'prim_energy_consumption',
#                                 'iron_steel_production',
#                                 'total_population',
#                                 'urban_population',
#                                 'cinc_score']])
#
# ################################################################################
#
# descriptive_df_1 = deepcopy(pd.merge(part_df_1, part_df_2, how = 'outer', on = ['c_code', 'year']))
# descriptive_df_1 = deepcopy(pd.merge(descriptive_df_1, part_df_3, how = 'outer', on = ['c_code', 'year']))
# descriptive_df_1['year'] = descriptive_df_1['year'].astype(float)
#
# ################################################################################
#
# print('total rows of descriptive participant data: {}'.format(format(len(descriptive_df_1), ',d')))
# pickle_directory = '/Users/the_networks_of_war/data_sources/pickles/'
# descriptive_df_1.to_pickle(pickle_directory + 'participant_descriptive_df.pkl')
#
# ################################################################################
#
# ## Descriptive Statistics for Each Dyad by Year
# ### Note: Applies to states/countries only.
# ### This will be joined to the dyadic pairs for each war
#
# print('\nDescriptive Statistics for Each Dyad by Year')
#
# ### Taking care of the easy ones first
# ## lot's to use in this dataset so I'll start with the basics
# data_source = csv_directory + 'tc2018.csv'
# dy_df_1 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_source(data_source, 'gainer', 'loser', 'year', 'territory_exchange'))
# ## must be dyadic (two states per row)
# dy_df_1 = deepcopy(dy_df_1[(dy_df_1['c_code_a']!=-8) & (dy_df_1['c_code_a']!=-9) & (dy_df_1['c_code_b']!=-8) & (dy_df_1['c_code_b']!=-9)])
#
# ## contiguity dataframe for states of colonial dependencies
# data_source = '/Users/the_networks_of_war/data_sources/csvs/contcold.csv'
# dy_df_2 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_source(data_source, 'statelno', 'statehno', 'year', 'colonial_contiguity'))
#
# data_source = '/Users/the_networks_of_war/data_sources/csvs/contdird.csv'
# dy_df_3 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_source(data_source, 'state1no', 'state2no', 'year', 'contiguity'))
#
# data_source = '/Users/the_networks_of_war/data_sources/csvs/alliance_v4.1_by_dyad_yearly.csv'
# dy_df_4 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_source(data_source, 'ccode1', 'ccode2', 'year', 'alliance'))
#
# data_source = '/Users/the_networks_of_war/data_sources/csvs/DCAD-v1.0-dyadic.csv'
# dy_df_5 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_source(data_source, 'ccode1', 'ccode2', 'year', 'defense_cooperation_agreements'))
#
# data_source = '/Users/the_networks_of_war/data_sources/csvs/dyadic_formatv3.csv'
# dy_df_6 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_source(data_source, 'ccode1', 'ccode2', 'year', 'inter_governmental_organizations'))
#
# ## this one needs to be filled since its only 5 years
# data_source = '/Users/the_networks_of_war/data_sources/csvs/Diplomatic_Exchange_2006v1.csv'
# dy_df_7 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_source(data_source, 'ccode1', 'ccode2', 'year', 'diplomatic_exchange'))
#
# data_source = '/Users/the_networks_of_war/data_sources/csvs/Dyadic_COW_4.0.csv'
# dy_df_8 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_source(data_source, 'ccode1', 'ccode2', 'year', 'trade_relations'))
#
# ################################################################################
#
# # for year in np.arange(1800, 2020):
# #     for row in dy_df_7['year']:
# #         if len(dy_df_7[dy_df_7['year']==year])== 0:
# #             temp_dyad_df = deepcopy(dy_df_7[dy_df_7['year']==year].reset_index())
# #             for i, dyad in enumerate(temp_dyad_df['year']):
# #                 dyad_df_length = deepcopy(len(dy_df_7))
# #                 dy_df_7.loc[dyad_df_length, 'year'] = year
# #                 dy_df_7.loc[dyad_df_length, 'c_code_a'] = temp_dyad_df.loc[i, 'c_code_a']
# #                 dy_df_7.loc[dyad_df_length, 'c_code_b'] = temp_dyad_df.loc[i, 'c_code_b']
# #         else:
# #             current_year = year
#
# # dy_df_7['diplomatic_exchange'] = 1
# # print(len(dy_df_7))
#
# ################################################################################
#
# ### Setting up processing of 'ddrevisited_data_v1'
#
# print('\nSetting up processing of ddrevisited_data_v1')
#
# dd_df_1 = pd.read_csv(csv_directory + 'ddrevisited_data_v1.csv', encoding = 'latin-1')[['cowcode', 'year', 'emil', 'royal', 'comm', 'democracy', 'regime', 'collect']]
# dd_df_1.rename({'cowcode': 'c_code',
#                 'emil': 'military_leader',
#                 'royal': 'royal_leader',
#                 'comm': 'communist_leader',
#                 'democracy': 'democratic_regime',
#                 'collect': 'collective_leadership',
#                 'regime': 'regime_type'}, axis=1, inplace=True)
# dd_df_2 = pd.read_csv('/Users/the_networks_of_war/data_sources/csvs/ddrevisited_data_v1.csv', encoding = 'latin-1')[['cowcode2', 'year', 'emil', 'royal', 'comm', 'democracy', 'regime', 'collect']]
# dd_df_2.rename({'cowcode2': 'c_code',
#                 'emil': 'military_leader',
#                 'royal': 'royal_leader',
#                 'comm': 'communist_leader',
#                 'democracy': 'democratic_regime',
#                 'collect': 'collective_leadership',
#                 'regime': 'regime_type'}, axis=1, inplace=True)
# ## unioning the two ccodes above so they can both be represented
# ## this will also allow for substates to be joined to the larger states
# ## this will need to be recognized later on to prevent from saying same leadership when it's the same leader
# dd_df = deepcopy(pd.concat([dd_df_1, dd_df_2], sort=True))
# ## removing duplicates from concat
# duplicate_list = ['c_code',
#                   'military_leader',
#                   'royal_leader',
#                   'communist_leader',
#                   'democratic_regime',
#                   'regime_type',
#                   'collective_leadership',
#                   'year']
# dd_df.drop_duplicates(subset = duplicate_list, keep = 'first', inplace=True)
#
# dd_df = deepcopy(pd.merge(dd_df, dd_df, how = 'left', on = ['year']))
# for column in dd_df.columns:
#     if column[-2:]=='_x':
#         dd_df.rename({column: column[:-2] + '_a'}, axis=1, inplace=True)
#     elif column[-2:]=='_y':
#         dd_df.rename({column: column[:-2] + '_b'}, axis=1, inplace=True)
#     else:
#         pass
#
# dd_df = deepcopy(dd_df[dd_df['c_code_a']!=dd_df['c_code_b']])
#
# ################################################################################
#
# conditional_statement = (dd_df['military_leader_a']==dd_df['military_leader_b']) & (dd_df['communist_leader_a']==dd_df['communist_leader_b']) & (dd_df['royal_leader_a']==dd_df['royal_leader_b']) & (dd_df['democratic_regime_a']==dd_df['democratic_regime_b'])
# dy_df_9 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_dd(dd_df, conditional_statement, 'same_leader_type'))
#
# conditional_statement = (dd_df['military_leader_a']==1) & (dd_df['military_leader_b']==1)
# dy_df_10 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_dd(dd_df, conditional_statement, 'both_military_leaders'))
#
# conditional_statement = (dd_df['communist_leader_a']==1) & (dd_df['communist_leader_b']==1)
# dy_df_11 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_dd(dd_df, conditional_statement, 'both_communist_leaders'))
#
# conditional_statement = (dd_df['royal_leader_a']==1) & (dd_df['royal_leader_b']==1)
# dy_df_12 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_dd(dd_df, conditional_statement, 'both_royal_leaders'))
#
# conditional_statement = dd_df['regime_type_a']==dd_df['regime_type_b']
# dy_df_13 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_dd(dd_df, conditional_statement, 'same_regime_type'))
#
# conditional_statement = ((dd_df['democratic_regime_a']==1)& (dd_df['democratic_regime_b']==1)) | (((dd_df['regime_type_a']==0) | (dd_df['regime_type_a']==1) | (dd_df['regime_type_a']==2)) & ((dd_df['regime_type_b']==0) | (dd_df['regime_type_b']==1) | (dd_df['regime_type_b']==2)))
# dy_df_14 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_dd(dd_df, conditional_statement, 'both_democratic_regimes'))
#
# conditional_statement = ((dd_df['regime_type_a']==3) | (dd_df['regime_type_a']==4) | (dd_df['regime_type_a']==5)) & ((dd_df['regime_type_b']==3) | (dd_df['regime_type_b']==4) | (dd_df['regime_type_b']==5))
# dy_df_15 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_dd(dd_df, conditional_statement, 'both_dictatorships'))
#
# conditional_statement = (dd_df['collective_leadership_a']==1) & (dd_df['collective_leadership_b']==1)
# dy_df_16 = deepcopy(the_networks_of_war_python_functions.descriptive_dyad_from_dd(dd_df, conditional_statement, 'both_collective_leadership'))
#
# # regime
# # Six‐fold regime classification
# # 0. Parliamentary democracy
# # 1. Mixed (semi‐presidential) democracy
# # 2. Presidential democracy
# # 3. Civilian dictatorship
# # 4. Military dictatorship
# # 5. Royal dictatorship
#
# ################################################################################
#
# descriptive_df_2 = deepcopy(pd.merge(dy_df_1, dy_df_2, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_3, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_4, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_5, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_6, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_7, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_8, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_9, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_10, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_11, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_12, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_13, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_14, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_15, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2 = deepcopy(pd.merge(descriptive_df_2, dy_df_16, how = 'outer', on = ['c_code_a', 'c_code_b', 'year']))
# descriptive_df_2['year'] = descriptive_df_2['year'].astype(float)
#
# ################################################################################
#
# print('\ntotal rows of descriptive dyadic data: {}'.format(format(len(descriptive_df_2), ',d')))
# descriptive_df_2.to_pickle(pickle_directory + 'dyadic_descriptive_df.pkl')
