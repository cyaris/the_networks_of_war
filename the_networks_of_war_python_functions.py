import pandas as pd
import numpy as np
from copy import deepcopy
from traceback import format_exc


def dictionary_from_field(dataframe, key_input, value_input):

    field_dic = dict(zip(dataframe[key_input], dataframe[value_input]))

    return field_dic


def define_c_code_dic():

    c_code_df = pd.read_csv('/Users/the_networks_of_war/data_sources/csvs/COW country codes.csv', encoding='utf8')
    c_code_df.rename({'CCode': 'c_code',
                      'StateNme': 'country'}, axis=1, inplace=True)
    c_code_df.drop(['StateAbb'], axis=1, inplace=True)

    duplicate_list = ['c_code', 'country']
    c_code_df.drop_duplicates(subset=duplicate_list, keep='first', inplace=True)
    c_code_df = deepcopy(c_code_df.reset_index(drop=True))

    c_code_dic = deepcopy(dictionary_from_field(c_code_df, 'c_code', 'country'))

    print('Total Country Codes: {}'.format(format(len(c_code_dic.keys()), ',d')))

    return c_code_dic


def define_country_abbreviation_dic():

    c_abb_df = pd.read_csv('/Users/the_networks_of_war/data_sources/csvs/COW country codes.csv', encoding='utf8')
    c_abb_df.rename({'StateNme': 'country',
                      'StateAbb': 'country_abbrev'}, axis=1, inplace=True)
    c_abb_df.drop(['CCode'], axis=1, inplace=True)

    duplicate_list = ['country', 'country_abbrev']
    c_abb_df.drop_duplicates(subset=duplicate_list, keep='first', inplace=True)
    c_abb_df = deepcopy(c_abb_df.reset_index(drop=True))

    c_abb_df = deepcopy(dictionary_from_field(c_abb_df, 'country_abbrev', 'country'))

    print('Total Abbreviated Names: {}'.format(format(len(c_abb_df.keys()), ',d')))

    return c_abb_df


def start_and_end_dates(dataframe):

    start_date_fields = ['start_day', 'start_month', 'start_year']
    end_date_fields = ['end_day', 'end_month', 'end_year']
    ## creating a field for ongoing wars
    for date_part in end_date_fields:
        dataframe.loc[dataframe[date_part]==-7, 'ongoing_participation'] = 1

    ## defining null values (missing or non-applicable data)
    ## includes ongoing wars (end date is non-applicable)
    for date_part in start_date_fields + end_date_fields:

        dataframe.loc[dataframe[date_part].astype(float)<=0, date_part] = None

        if date_part[-4:]!='year':
            ## creating an initial flag to say that the value has been filled in arbitrarily
            ## this will be made null if total_days_at_war is not calculated
            dataframe.loc[dataframe[date_part].isnull(), date_part.split('_')[0] + '_date_estimated'] = 1
            ## filling null days with the first day of the month
            ## filling null months with the first month of the year
            ## this will lead to an estimation for start_date-end_date (total days at war)
            dataframe.loc[dataframe[date_part].isnull(), date_part] = 1

    dataframe.loc[dataframe['ongoing_participation'].isnull(), 'ongoing_participation'] = 0
    ## if the above doesn't apply, then the dates were not calculated arbitrarily (or not calculated at all)
    ## this value will be adjusted to None later if not calculated at all
    dataframe.loc[dataframe['start_date_estimated'].isnull(), 'start_date_estimated'] = 0
    dataframe.loc[dataframe['end_date_estimated'].isnull(), 'end_date_estimated'] = 0

    ## calculating null for all without valid start/end dates.
    ## those with invalid data will have null dates and increae the count for not_found.
    dates_found = 0
    dates_not_found = 0

    for i, date in enumerate(dataframe['war_num']):

        for date_part in start_date_fields + end_date_fields:
            ## unsure why this occurs (.0 after integer)
            if '.' in str(dataframe.loc[i, date_part]):
                dataframe.loc[i, date_part] = str(dataframe.loc[i, date_part]).split('.')[0]

        try:
            dataframe.loc[i, 'start_date'] = pd.to_datetime(dataframe.loc[i, 'start_year'] + "-" + dataframe.loc[i, 'start_month'] + "-" + dataframe.loc[i, 'start_day'])
            valid_start_date = 1
        except:
            dataframe.loc[i, 'start_date'] = None
            dataframe.loc[i, 'start_date_estimated'] = None
            valid_start_date = 0
        try:
            dataframe.loc[i, 'end_date'] = pd.to_datetime(dataframe.loc[i, 'end_year'] + "-" + dataframe.loc[i, 'end_month'] + "-" + dataframe.loc[i, 'end_day'])
            valid_end_date = 1
        except:
            dataframe.loc[i, 'end_date'] = None
            dataframe.loc[i, 'end_date_estimated'] = None
            valid_end_date = 0

        if valid_start_date==1 and valid_end_date==1:
            dataframe.loc[i, 'days_at_war'] = dataframe.loc[i, 'end_date'] - dataframe.loc[i, 'start_date']
            dataframe.loc[i, 'days_at_war'] = int(str(dataframe.loc[i, 'days_at_war']).split(' ')[0]) + 1
        else:
            dataframe.loc[i, 'days_at_war'] = None

        if valid_start_date==1 and valid_end_date==1:
            dates_found+=1
        else:
            dates_not_found+=1

    print("Total Rows With Both Dates Found: {}".format(format(dates_found, ',d')))
    print("Total Rows With At Least One Date Not Found: {}".format(format(dates_not_found, ',d')))
    print("Total Estimated Start Dates: {}".format(format(len(dataframe[dataframe['start_date_estimated']==1]), ',d')))
    print("Total Estimated End Dates: {}\n".format(format(len(dataframe[dataframe['end_date_estimated']==1]), ',d')))

    return dataframe


def final_date_formatting(dataframe):

    null_start_years = deepcopy(len(dataframe[dataframe['start_year'].isnull()]))
    ## accounting for ongoing participation (the only time when null year is okay)
    null_end_years = deepcopy(len(dataframe[(dataframe['end_year'].isnull()) & (dataframe['ongoing_participation']==1)]))

    for i, row in enumerate(dataframe[list(dataframe.columns)[0]]):
        if len(str(dataframe.loc[i, 'start_year'])) < 4:
            try:
                dataframe.loc[i, 'start_year'] = int(str(dataframe.loc[i, 'start_date'])[0:4])
            except:
                pass
        if len(str(dataframe.loc[i, 'end_year'])) < 4 and dataframe.loc[i, 'ongoing_participation']==False:
            try:
                dataframe.loc[i, 'end_year'] = int(str(dataframe.loc[i, 'end_date'])[0:4])
            except:
                pass

    final_null_start_years = deepcopy(len(dataframe[dataframe['start_year'].isnull()]))
    ## accounting for ongoing participation (the only time when null year is okay)
    final_null_end_years = deepcopy(len(dataframe[(dataframe['end_year'].isnull()) & (dataframe['ongoing_participation']==1)]))

    print('Start Years Reformatted: {}'.format(format(null_start_years-final_null_start_years, ',d')))
    print('End Years Reformatted: {}\n'.format(format(null_end_years-final_null_end_years, ',d')))

    return dataframe


def remaining_participant_null_values(dataframe, remaining_fields):

    ## defining null values (missing data)
    for field in remaining_fields:
        ## -8 is not applicable.
        dataframe.loc[dataframe[field]==-8, field] = None
        ## -9 is unknown.
        dataframe.loc[dataframe[field]==-9, field] = None

    return dataframe


def get_switched_columns(dataframe):

    switched_columns_list = []
    ## unioning mismatching columns so each participant will get their own row
    for column in list(dataframe.columns):
        if column[-2:]=='_a' or column[-2:]=='_b':
            switched_columns_list.append(column)

    return switched_columns_list


def union_opposite_columns(dataframe):

    union_dataframe = deepcopy(dataframe)

    switched_columns_list = deepcopy(get_switched_columns(dataframe))

    ## doing these inefficient column name changes to fill in for a much needed sql union of mismatching column names
    for column in switched_columns_list:
        union_dataframe.rename({column: column + '_new'}, axis=1, inplace=True)

    for column in switched_columns_list:
        if column[-2:]=='_a':
            union_dataframe.rename({column + '_new': column[:-1] + 'b'}, axis=1, inplace=True)
        elif column[-2:]=='_b':
            union_dataframe.rename({column + '_new': column[:-1] + 'a'}, axis=1, inplace=True)
        else:
            pass

    dataframe = deepcopy(pd.concat([dataframe, union_dataframe], sort=True, ignore_index=True))
    dataframe.drop_duplicates(subset=list(dataframe.columns), keep='first', inplace=True)
    dataframe = deepcopy(dataframe.reset_index(drop=True))

    return dataframe


def drop_participant_b_columns(dataframe):

    for column in list(dataframe.columns):
        if column[-2:]=='_b':
            dataframe.drop(column, axis=1, inplace=True)

    for column in list(dataframe.columns):
        if column[-2:]=='_a':
            dataframe.rename({column: column[:-2]}, axis=1, inplace=True)

    return dataframe


def column_fills_and_converions(dataframe, grouping_type, conversion_dic):

    column_list = list(dataframe.columns)
    x_y_z_columns = []

    for i, column in enumerate(column_list):
        if column[-2:]=='_x':
            x_y_z_columns.append(column)
        elif column[-2:]=='_y':
            x_y_z_columns.append(column)
        elif column[-2:]=='_z':
            x_y_z_columns.append(column)

    null_values = 0
    unknown_values = 0
    ## filling in nulls with zeros
    ## these are ones that most likely mean zero if null (not due to missing data)
    for column in x_y_z_columns:
        null_values = null_values + len(dataframe[(dataframe[column].isnull())])
        if grouping_type=='participant':
            ## marking null values for non-state participants as null because they are non-applicable
            dataframe.loc[(dataframe[column].isnull()) & (dataframe['c_code']<=0), column] = None
            ## marking remaining null values as 0 because they are applicable and non-existant
            dataframe.loc[(dataframe[column].isnull()) & (dataframe['c_code']>=0), column] = 0
        elif grouping_type=='dyad':
            ## marking null values for non-state participants as null because they are non-applicable
            dataframe.loc[(dataframe[column].isnull()) & ((dataframe['c_code_a']<=0) | (dataframe['c_code_b']<=0)), column] = None
            ## marking remaining null values as 0 because they are applicable and non-existant
            dataframe.loc[(dataframe[column].isnull()) & (dataframe['c_code_a']>=0) & (dataframe['c_code_b']>=0), column] = 0
        ## giving these null values
        ## -9 is unknown value in the dataset
        ## -8 is non-applicable value
        unknown_values = unknown_values + len(dataframe[(dataframe[column]==-9) | (dataframe[column]==-8)])
        dataframe.loc[dataframe[column]==-9, column] = None
        dataframe.loc[dataframe[column]==-8, column] = None
        if conversion_dic==None:
            pass
        elif column[:-2] in list(conversion_dic.keys()):
            # converting these to their proper units according to documentation
            dataframe[column] = ([s * conversion_dic[column[:-2]] for s in dataframe[column]])

    print('\nTotal Columns Adjusted: {}'.format(format(len(x_y_z_columns), ',d')))
    if conversion_dic==None:
        print('Total Columns Adjusted for Conversion: 0')
    else:
        print('Total Columns Adjusted for Conversion: {}'.format(format(len(list(conversion_dic.keys())) * 2, ',d')))

    print('Total Null Values Notated: {}'.format(format(null_values, ',d')))
    print('Total Unknown Values Notated: {}'.format(format(unknown_values, ',d')))

    return dataframe


def format_part_df_from_dyadic_data(dy_df):

    ## whoever is originally marked as side a is getting labelled as 1.
    ## whoever is originally marked as side b is getting labelled as 2.
    dy_df['side_a'] = 1
    dy_df['side_b'] = 2

    ## getting start dates and end dates
    dy_df = deepcopy(start_and_end_dates(dy_df))

    dy_df = deepcopy(union_opposite_columns(dy_df))

    switched_columns_list = deepcopy(get_switched_columns(dy_df))
    ## making a copy before duplicates a taken out.
    ## this will be used below for dyadic data (since no dyadic files are available for intra-state wars)
    dy_df_columns = deepcopy(switched_columns_list)
    ## this will be adjusted again later
    dy_df.rename({'start_year': 'year'}, axis=1, inplace=True)

    # keeping one state (or non-state) per war after duplicate removal
    duplicate_list = ['war_num', 'c_code_a', 'participant_a']
    part_dataframe = deepcopy(dy_df)
    part_dataframe.drop_duplicates(subset=duplicate_list, keep='first', inplace=True)
    part_dataframe = deepcopy(part_dataframe.reset_index(drop=True))
    part_dataframe = deepcopy(drop_participant_b_columns(part_dataframe))

    dy_df_columns.append('war_num')
    dy_df_columns.append('year')
    dy_df = deepcopy(dy_df[dy_df_columns])

    ## removing non applicable participants
    ## don't need to do this for inter-state war because all is applicable
    part_dataframe = deepcopy(part_dataframe[(part_dataframe['participant']!='-8')].reset_index(drop=True))
    dy_df = deepcopy(dy_df[(dy_df['participant_a']!='-8') & (dy_df['participant_b']!='-8')].reset_index(drop=True))

    return part_dataframe, dy_df



def add_missing_dyads(pa_df_copy, dy_df, war_input, side_input, single_side):

    opposing_side_dic = {1: 2, 2: 1}
    ## part_df is already filtered to war_input.
    ## dy_df is not because the master dataframe needs to be edited.
    ## filtering to war_input here to simplify statements below.
    dy_df_copy = deepcopy(dy_df[dy_df['war_num']==war_input].reset_index(drop=True))
    ## iterating over a group of participants
    if single_side=='all_participants':
        c_code_a = pa_df_copy[pa_df_copy['side']==side_input]['c_code'].values[0]
        participant_a = pa_df_copy[pa_df_copy['side']==side_input]['participant'].values[0]
    ## iterating over a group of participants
    elif single_side=='non-state':
        c_code_a = pa_df_copy[(pa_df_copy['side']==side_input) & (pa_df_copy['c_code']==-8)]['c_code'].values[0]
        participant_a = pa_df_copy[(pa_df_copy['side']==side_input) & (pa_df_copy['c_code']==-8)]['participant'].values[0]
    ## iterating over a single participant
    elif single_side=='state':
        c_code_a = pa_df_copy[(pa_df_copy['side']==side_input) & (pa_df_copy['c_code']!=-8)]['c_code'].values[0]
        participant_a = pa_df_copy[(pa_df_copy['side']==side_input) & (pa_df_copy['c_code']!=-8)]['participant'].values[0]
    opposing_participants = sorted(list(pa_df_copy[pa_df_copy['side']==opposing_side_dic[side_input]]['participant'].unique()))
    dyadic_parties = sorted(list(set(list(dy_df_copy['participant_a']) + list(dy_df_copy['participant_b']))))
    for i, party_b in enumerate(opposing_participants):
        ## state is the only type allowed to iterate over participants that are already linked to other participants
        if single_side!='state' and party_b in dyadic_parties and len(dyadic_parties) > 0:
            pass
        elif single_side=='state' and participant_a in dyadic_parties:
            pass
        else:
            ## preparing list for start years, end years and all years in-between
            ## each assumed dyad will be joined to all years where the participant being added (not in list) was a part of the war.
            part_b_start_year = pa_df_copy[pa_df_copy['participant']==party_b]['start_year'].values[0]
            part_b_end_year = pa_df_copy[pa_df_copy['participant']==party_b]['end_year'].values[0]
            part_b_year_list = []
            if len(str(part_b_start_year))==4 and len(str(part_b_end_year))==4:
                for year in np.arange(int(part_b_start_year), int(part_b_end_year)+1):
                    part_b_year_list.append(year)
            elif len(str(part_b_start_year))==4:
                part_b_year_list.append(int(part_b_start_year))
            elif len(str(part_b_end_year))==4:
                part_b_year_list.append(int(part_b_end_year))
            else:
                ## acounting for when the list is empty since that would break the loop
                part_b_year_list.append(None)
            for part_b_year in part_b_year_list:
                df_length = deepcopy(len(dy_df))
                dy_df.loc[df_length, 'war_num'] = war_input
                dy_df.loc[df_length, 'c_code_a'] = c_code_a
                dy_df.loc[df_length, 'participant_a'] = participant_a
                dy_df.loc[df_length, 'year'] = part_b_year
                dy_df.loc[df_length, 'c_code_b'] = pa_df_copy[pa_df_copy['participant']==party_b]['c_code'].values[0]
                dy_df.loc[df_length, 'participant_b'] = party_b

    return dy_df


def descriptive_dyad_from_source(descriptive_df, source, dataframe, conditional_statement, c_code_a, c_code_b, year, binary_field):

    if source=='conditional':
        dy_df = deepcopy(dataframe[conditional_statement][[c_code_a, c_code_b, year]].reset_index(drop=True))
    elif source==None:
        dy_df = deepcopy(dataframe[[c_code_a, c_code_b, year]])
    else:
        dy_df = pd.read_csv(source, encoding='utf8')[[c_code_a, c_code_b, year]]

    dy_df.rename({c_code_a: 'c_code_a',
                  c_code_b: 'c_code_b',
                  year: 'year'}, axis=1, inplace=True)
    # removing any duplicates that may have occured
    duplicate_columns_list = ['c_code_a', 'c_code_b', 'year']
    dy_df.drop_duplicates(subset=duplicate_columns_list, keep='first', inplace=True)
    ## creating a binary field to represent this dataset
    ## more specific fields can be added later
    dy_df[binary_field] = 1
    dy_df = deepcopy(union_opposite_columns(dy_df))
    ## intergrating the descriptive data into the master dataframe for all dyads
    descriptive_df = deepcopy(pd.merge(descriptive_df, dy_df, how='left', on=['c_code_a', 'c_code_b', 'year']))

    return descriptive_df


def print_new_fields(descriptive_df, initial_columns, descriptive_columns):

    ## using this field for both the descriptive dataframes, and the dyadic dataframes joined to descriptive data.
    ## the descriptive dataframes have duplicates from unions while the dyadic dataframes do not.

    ## only need the initial columns if descriptive columns not provided
    if descriptive_columns==None:
        descriptive_columns = deepcopy(set(list(descriptive_df.columns)))
        descriptive_columns = deepcopy(list(descriptive_columns - initial_columns))

    print_dic = {}
    for column in descriptive_columns:
        if initial_columns==None:
            print_dic[column] = len(descriptive_df[descriptive_df[column]==1])
        else:
            print_dic[column] = int(len(descriptive_df[descriptive_df[column]==1])/2)

    print_df = pd.DataFrame(list(print_dic.items()), columns=['field', 'dyads'])

    if initial_columns==None:
        print_df.loc[(['_z' in s[-2:] for s in print_df['field']]), 'timeframe'] = 'Overall'
        print_df.loc[(['_y' in s[-2:] for s in print_df['field']]), 'timeframe'] = 'Last Year'
        print_df.loc[(['_x' in s[-2:] for s in print_df['field']]), 'timeframe'] = 'First Year'
        print_df = deepcopy(print_df[['timeframe', 'field', 'dyads']])
        for i, field in enumerate(print_df['field']):
            print_df.loc[i, 'field'] = field[:-2]

        print_df.sort_values(by=['dyads', 'field', 'timeframe'], ascending=(False, True, True), inplace=True)

        print(print_df.to_string(index=False, header=False))
    else:
        print_df.sort_values(by=['dyads', 'field'], ascending=(False, True), inplace=True)
        print(print_df.to_string(index=False, header=False))

    return


def descriptive_participant_from_dyad(file_input, df_part_renaming_dic):

    ## dyadic data that needs to be adjusted to be non-dyadic (by country, by year)
    df_part = pd.read_csv(file_input, encoding='utf8')
    df_part.rename(df_part_renaming_dic, axis = 1, inplace= True)
    df_part = deepcopy(df_part[list(df_part_renaming_dic.values())].reset_index(drop=True))

    ## need to union to take summations but won't need to dedupe because there are no duplicates between a and b.
    ## this means a can be summed on its own when it's combined with b.
    df_part = deepcopy(union_opposite_columns(df_part))

    ## using c_code_a as actual c_code.
    ## c_code_b will become a count of total allies (after duplicates are removed)
    ## removing duplicates from concat
    df_part.drop_duplicates(subset=list(df_part_renaming_dic.values()), keep='first', inplace=True)
    df_part.rename({'c_code_a': 'c_code'}, axis=1, inplace=True)

    return df_part


def descriptive_participant_aggregation(initial_part_df, df_part, renaming_input, aggregations_input):

    if renaming_input==None:
        pass
    else:
        df_part.rename(renaming_input, axis=1, inplace=True)

    df_part = deepcopy(df_part[list(renaming_input.values())].reset_index(drop=True))
    ## replacing unknowns with null before summation
    for field in aggregations_input.keys():
        if aggregations_input[field]=='sum':
            df_part.loc[df_part[field]==-9, field] = None
            df_part[field] = df_part[field].astype(float)

    df_part = deepcopy(df_part.groupby(['c_code', 'year']).agg(aggregations_input).reset_index())

    ## inner join to only include participants found in participants war data
    ## this will limit runtime significantly
    df_part = deepcopy(pd.merge(initial_part_df, df_part, how='inner', on=['c_code', 'year']))

    return df_part


def get_summation_aggregation_dic(df_renaming_dic, non_aggregation_values_input):

    aggregations = {}

    ## iterating over the dictionary values that will be aggregated
    for value in list(df_renaming_dic.values()):
        if value not in non_aggregation_values_input:
            aggregations[value] = 'sum'

    return aggregations

def adjustParticipantNames(dataframe, grouping_type):

    if grouping_type=='participant':
        print('Adjusting and consolidating participant names for part_df.')
        columns = ['participant']
    elif grouping_type=='dyad':
        print('Adjusting and consolidating participant names for dyad_df.')
        columns = ['participant_a', 'participant_b']

    for column in columns:
        dataframe.loc[dataframe[column]=='United States', column] = 'United States of America'
        dataframe.loc[dataframe[column]=='Baron von Ungern-Sternberg\x92s White army', column] = 'Baron von Ungern-Sternberg\'s White army'
        dataframe.loc[dataframe[column]==' Janissaries', column] = 'Janissaries'
        dataframe.loc[dataframe[column]=='Turkey/Ottoman Empire/Egypt', column] = 'Turkey, Ottoman Empire & Egypt'
        for i, participant in enumerate(dataframe[column]):
            if  ' and ' in participant:
                dataframe.loc[i, column] = dataframe.loc[i, column].replace(' and ', ' & ')
            if  ' rebels' in participant:
                dataframe.loc[i, column] = dataframe.loc[i, column].replace(' rebels', ' Rebels')
            if  ' tribe' in participant:
                dataframe.loc[i, column] = dataframe.loc[i, column].replace(' tribe', ' Tribe')
            if  ' sultanate' in participant:
                dataframe.loc[i, column] = dataframe.loc[i, column].replace(' sultanate', ' Sultanate')
            if  ' resistance' in participant:
                dataframe.loc[i, column] = dataframe.loc[i, column].replace(' resistance', ' Resistance')
            ## fixing type of 'Resistance'
            if  ' resistence' in participant:
                dataframe.loc[i, column] = dataframe.loc[i, column].replace(' resistence', ' Resistance')

    return dataframe
