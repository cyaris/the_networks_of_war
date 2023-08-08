import pandas as pd
import numpy as np
from pandasql import sqldf
import re
from copy import deepcopy
from traceback import format_exc


def dictionary_from_field(dataframe, key_input, value_input):

    field_dic = dict(zip(dataframe[key_input], dataframe[value_input]))

    return field_dic


def define_c_code_dic():

    c_code_df = pd.read_csv('/Users/charlieyaris/Personal/data_sources/the_networks_of_war/csvs/COW country codes.csv', encoding='utf8')
    c_code_df.rename({'CCode': 'c_code',
                      'StateNme': 'country'}, axis=1, inplace=True)
    c_code_df.drop(['StateAbb'], axis=1, inplace=True)

    duplicate_list = ['c_code', 'country']
    c_code_df.drop_duplicates(subset=duplicate_list, keep='first', inplace=True)
    c_code_df = deepcopy(c_code_df.reset_index(drop=True))

    c_code_dic = deepcopy(dictionary_from_field(c_code_df, 'c_code', 'country'))

    print('Total Country Codes: {}'.format(format(len(c_code_dic.keys()), ',d')))

    return c_code_dic


def start_and_end_dates(dataframe):

    date_fields = ['start_year', 'start_month', 'start_day', 'end_year', 'end_month', 'end_day']
    monthly_max_df = deepcopy(pd.read_csv('monthly_max_days.csv'))
    years_df = pd.DataFrame(np.arange(1500, 2100), columns=['year'])

    def get_max_suffix():

        suffix_list = []
        for column in dataframe.columns:
            if column[:4] in ['star', 'end_']:
                suffix_list.append(int(column[-1]))

        max_suffix = max(suffix_list)

        return max_suffix

    max_suffix = get_max_suffix()

    ## filling null start days with the first day of the month
    ## filling null start months with the first month of the year
    query_text_template = """

    select
        cast(SUFFIX as int) as suffix,
        case when y1.year is null and coalesce(a.start_year_SUFFIX, -1) != -7 then null
            else max(coalesce(a.start_year_SUFFIX, 1), 1) end as start_year,
        case when y1.year is null and coalesce(a.start_year_SUFFIX, -1) != -7 then null
            else max(coalesce(a.start_month_SUFFIX, 1), 1) end as start_month,
        case when y1.year is null and coalesce(a.start_year_SUFFIX, -1) != -7 then null
            else max(coalesce(a.start_day_SUFFIX, 1), 1) end as start_day,
        case when y2.year is null and coalesce(a.end_year_SUFFIX, -1) != -7 then null
        when coalesce(a.end_year_SUFFIX, -1) < 0 then cast(strftime('%Y', date('now')) as integer)
            else a.end_year_SUFFIX end as end_year,
        case when y2.year is null and coalesce(a.end_year_SUFFIX, -1) != -7 then null
        when coalesce(a.end_year_SUFFIX, -1) < 0 then cast(strftime('%m', date('now')) as integer)
        when a.end_year_SUFFIX > 0 and coalesce(a.end_month_SUFFIX, -1) < 0 then 12
            else a.end_month_SUFFIX end as end_month,
        case when y2.year is null and coalesce(a.end_year_SUFFIX, -1) != -7 then null
        when coalesce(a.end_year_SUFFIX, -1) < 0 then cast(strftime('%d', date('now')) as integer)
        when a.end_month_SUFFIX > 0 and coalesce(a.end_day_SUFFIX, -1) < 0 then mm.max_days
        when coalesce(a.end_month_SUFFIX, -1) < 0 and coalesce(a.end_day_SUFFIX, -1) < 0 then 31
            else a.end_day_SUFFIX end as end_day,
        case when y1.year is null and coalesce(a.start_year_SUFFIX, -1) != -7 then 0
        when coalesce(a.start_year_SUFFIX, -1) < 0 or coalesce(a.start_month_SUFFIX, -1) < 0 or coalesce(a.start_day_SUFFIX, -1) < 0 then 1
            else 0 end as start_date_estimated,
        case when y2.year is null and coalesce(a.end_year_SUFFIX, -1) != -7 then 0
        when coalesce(a.end_year_SUFFIX, -1) < 0 or coalesce(a.end_month_SUFFIX, -1) < 0 or coalesce(a.end_day_SUFFIX, -1) < 0 then 1
            else 0 end as end_date_estimated,
        case when coalesce(a.end_year_SUFFIX, -1) = -7 then 1
        when y2.year is null then 0
            else 0 end as ongoing_conflict
    from df_row a
    left join years_df y1 on a.start_year_SUFFIX = y1.year
    left join years_df y2 on a.end_year_SUFFIX = y2.year
    left join monthly_max_df mm on case when coalesce(a.end_year_SUFFIX, -1) < 0 then cast(strftime('%m', date('now')) as integer) when a.end_year_SUFFIX > 0 and coalesce(a.end_month_SUFFIX, -1) < 0 then 12 else a.end_month_SUFFIX end = mm.month

    """

    def get_date_field(start_or_end):

        if len(df_row_temp[df_row_temp[start_or_end + '_year'].isnull()])==0:
            ## fixing for leap year issues below caused by date being filled in as final day for month.
            df_row_temp.loc[(df_row_temp[start_or_end + '_year']%4>0) & (df_row_temp[start_or_end + '_month']==2) & (df_row_temp[start_or_end + '_day']==29), start_or_end + '_day'] = 28
            ## fulfilling start and dates (in the same manner for all source tables).
            df_row_temp[start_or_end + '_date'] = pd.to_datetime(df_row_temp[start_or_end + '_year'].astype(int).astype(str) + '-' + df_row_temp[start_or_end + '_month'].astype(int).astype(str) + '-' + df_row_temp[start_or_end + '_day'].astype(int).astype(str)).dt.date
        else:
            df_row_temp[start_or_end + '_date'] = None

        df_row_temp.drop([start_or_end + '_month', start_or_end + '_day'], axis=1, inplace=True)

        return df_row_temp

    for i, row in enumerate(dataframe.index):

        df_row = deepcopy(dataframe[dataframe.index==i].reset_index(drop=True))
        df_row_all_dates = pd.DataFrame()

        for suffix in np.arange(1, max_suffix+1):

            for date_field in date_fields:
                ## applying regex from stackoverflow to remove trailing zeros after final non-zero digit.
                date_value = str(df_row.loc[0, date_field + '_' + str(suffix)]).replace('/(\.\d*?[1-9])0+$/g', '$1')
                try:
                    df_row.loc[0, date_field] = int(date_value)
                except:
                    df_row.loc[0, date_field] = float(date_value)

            query_text = query_text_template.replace('SUFFIX', str(suffix))
            df_row_temp = deepcopy(sqldf(query_text, {**locals(), **globals()}))
            df_row_temp = deepcopy(get_date_field('start'))
            df_row_temp = deepcopy(get_date_field('end'))

            for column in df_row_temp.columns:
                df_row_all_dates.loc[suffix-1, column] = df_row_temp.loc[0, column]

        df_row_all_dates['next_start_date'] = df_row_all_dates['start_date'].shift(-1)
        df_row_all_dates['days_not_at_war'] = df_row_all_dates['next_start_date'] - df_row_all_dates['end_date']
        df_row_all_dates['days_not_at_war'] = df_row_all_dates['days_not_at_war'].astype(str)
        for z, days_not_at_war in enumerate(df_row_all_dates['days_not_at_war']):
            if days_not_at_war not in ['nan', 'NaT']:
                df_row_all_dates.loc[z, 'days_not_at_war'] = days_not_at_war.split(' ')[0]
            else:
                df_row_all_dates.loc[z, 'days_not_at_war'] = '0'
        df_row_all_dates['days_not_at_war'] = df_row_all_dates['days_not_at_war'].astype(int)
        df_row_all_dates.drop('next_start_date', axis=1, inplace=True)

        query_text = """

        with

        start_date_table as (

            select
                start_date,
                start_year,
                start_date_estimated,
                row_number() over(order by start_date) as start_date_count
            from df_row_all_dates
            where
                start_date is not null),

        end_date_table as (

            select
                end_date,
                end_year,
                end_date_estimated,
                row_number() over(order by end_date desc) as end_date_count
            from df_row_all_dates
            where
                end_date is not null)

        select
            sd.start_date,
            sd.start_year,
            sd.start_date_estimated,
            ed.end_date,
            ed.end_year,
            ed.end_date_estimated,
            max(a.ongoing_conflict) as ongoing_conflict,
            sum(a.days_not_at_war) as days_not_at_war
        from df_row_all_dates a
        left join start_date_table sd on sd.start_date_count = 1
        left join end_date_table ed on ed.end_date_count = 1
        group by 1, 2, 3, 4, 5, 6

        """

        df_replace = deepcopy(sqldf(query_text, {**locals(), **globals()}))

        for column_name in df_replace.columns:
            dataframe.loc[i, column_name] = df_replace[column_name].values[0]

    # outside of main loop now.
    for suffix in np.arange(1, max_suffix+1):
        dataframe.drop(['start_year_' + str(suffix), 'start_month_' + str(suffix), 'start_day_' + str(suffix), 'end_year_' + str(suffix), 'end_month_' + str(suffix), 'end_day_' + str(suffix)], axis=1, inplace = True)

    print("Total Rows With Both Dates Found: {}".format(format(len(dataframe[(dataframe['start_date'].isnull()==False) & (dataframe['end_date'].isnull()==False)]), ',d')))
    print("Total Rows With At Least One Date Not Found: {}".format(format(len(dataframe[(dataframe['start_date'].isnull()) | (dataframe['end_date'].isnull())]), ',d')))
    print("Total Estimated Start Dates: {}".format(format(len(dataframe[dataframe['start_date_estimated']==1]), ',d')))
    print("Total Estimated End Dates: {}\n".format(format(len(dataframe[dataframe['end_date_estimated']==1]), ',d')))

    return dataframe


def get_switched_columns(dataframe):

    switched_columns_list = []
    ## unioning mismatching columns so each participant will get their own row
    for column in list(dataframe.columns):
        if column[-2:]=='_a' or column[-2:]=='_b':
            switched_columns_list.append(column)

    return switched_columns_list


def union_opposite_columns(dataframe):

    switched_columns_list = deepcopy(get_switched_columns(dataframe))

    union_dataframe = deepcopy(dataframe)
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

    dataframe_copy = deepcopy(dataframe)

    for column in list(dataframe_copy.columns):
        if column[-2:]=='_b':
            dataframe_copy.drop(column, axis=1, inplace=True)

    for column in list(dataframe_copy.columns):
        if column[-2:]=='_a':
            dataframe_copy.rename({column: column[:-2]}, axis=1, inplace=True)

    return dataframe_copy


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


def process_dyadic_data(dy_df):

    ## whoever is originally marked as side a is getting labelled as 1.
    ## whoever is originally marked as side b is getting labelled as 2.
    dy_df['side_a'] = 1
    dy_df['side_b'] = 2

    ## getting start dates and end dates
    dy_df = deepcopy(start_and_end_dates(dy_df))
    ## unioning opposite columns so this can be repurposed for participant data.
    dy_df = deepcopy(union_opposite_columns(dy_df))

    ## adding in blank names when they are not available.
    ## these will be properly filled in later in the function.
    if 'participant_a' not in list(dy_df.columns):
        dy_df['participant_a'] = ''
    if 'participant_b' not in list(dy_df.columns):
        dy_df['participant_b'] = ''
    if 'disno' not in list(dy_df.columns):
        dy_df['disno'] = None
    # keeping one state (or non-state) per war after duplicate removal
    part_dataframe = deepcopy(drop_participant_b_columns(dy_df))

    ## remove later!
    csv_directory = '/Users/charlieyaris/Personal/data_sources/the_networks_of_war/csvs/'
    c_code_df = pd.read_csv(csv_directory + 'COW country codes.csv', encoding='latin-1')
    years_df = pd.DataFrame(np.arange(1500, 2100), columns=['year'])

    query_text = """

    select
        ccode as c_code,
        statenme as state_name,
        stateabb as state_name_abbreviation
    from c_code_df
    group by 1, 2, 3

    """

    c_code_df = deepcopy(sqldf(query_text, {**locals(), **globals()}))

    query_text = """

    select
        a.war_num,
        a.war_name,
        a.disno,
        a.c_code_a,
        coalesce(cca.state_name, a.participant_a) as participant_a,
        a.c_code_b,
        coalesce(ccb.state_name, a.participant_b) as participant_b,
        sum(case when a.battle_deaths_a >= 0 then a.battle_deaths_a else null end) as battle_deaths_a,
        sum(case when a.battle_deaths_b >= 0 then a.battle_deaths_b else null end) as battle_deaths_b,
        min(a.start_date) as start_date,
        cast(strftime('%Y', min(a.start_date)) as integer) as start_year,
        max(a.end_date) as end_date,
        cast(strftime('%Y', max(a.end_date)) as integer) as end_year
    from dy_df a
    left join c_code_df cca on a.c_code_a = cca.c_code
    left join c_code_df ccb on a.c_code_b = ccb.c_code
    group by 1, 2, 3, 4, 5, 6, 7

    """

    if 'war_name' not in list(dy_df.columns):
        query_text = query_text.replace('a.war_name,', '')
        query_text = query_text.replace('group by 1, 2, 3, 4, 5, 6, 7', 'group by 1, 2, 3, 4, 5, 6')

    dy_df = deepcopy(sqldf(query_text, {**locals(), **globals()}).reset_index(drop=True))

    # removing non applicable participants
    # don't need to do this for inter-state war because all is applicable
    part_dataframe = deepcopy(part_dataframe[(part_dataframe['participant']!='-8')].reset_index(drop=True))
    dy_df = deepcopy(dy_df[(dy_df['participant_a']!='-8') & (dy_df['participant_b']!='-8')].reset_index(drop=True))

    return part_dataframe, dy_df


def add_missing_dyads(pa_df_copy, dy_df, war_input, side_input, side_type, manual_participant):

    opposing_side_dic = {1: 2, 2: 1}
    ## part_df is already filtered to war_input.
    ## dy_df is not because the master dataframe needs to be edited.
    ## filtering to war_input here to simplify statements below.
    dy_df_copy = deepcopy(dy_df[dy_df['war_num']==war_input].reset_index(drop=True))
    ## iterating over a group of participants
    if side_type=='manual':
        c_code_a = pa_df_copy[pa_df_copy['participant']==manual_participant]['c_code'].values[0]
        participant_a = manual_participant
    ## iterating over a group of participants
    elif side_type=='all_participants':
        c_code_a = pa_df_copy[pa_df_copy['side']==side_input]['c_code'].values[0]
        participant_a = pa_df_copy[pa_df_copy['side']==side_input]['participant'].values[0]
    ## iterating over a group of participants
    elif side_type=='non-state':
        c_code_a = pa_df_copy[(pa_df_copy['side']==side_input) & (pa_df_copy['c_code']==-8)]['c_code'].values[0]
        participant_a = pa_df_copy[(pa_df_copy['side']==side_input) & (pa_df_copy['c_code']==-8)]['participant'].values[0]
    ## iterating over a single participant
    elif side_type=='state':
        c_code_a = pa_df_copy[(pa_df_copy['side']==side_input) & (pa_df_copy['c_code']!=-8)]['c_code'].values[0]
        participant_a = pa_df_copy[(pa_df_copy['side']==side_input) & (pa_df_copy['c_code']!=-8)]['participant'].values[0]
    opposing_participants = sorted(list(pa_df_copy[pa_df_copy['side']==opposing_side_dic[side_input]]['participant'].unique()))
    dyadic_parties = sorted(list(set(list(dy_df_copy['participant_a']) + list(dy_df_copy['participant_b']))))
    for i, participant_b in enumerate(opposing_participants):
        ## CHECK CODE BELOW
        ## state is the only type allowed to iterate over participants that are already linked to other participants
        # if side_type!='state' and participant_b in dyadic_parties and len(dyadic_parties) > 0:
        #     pass
        # elif side_type=='state' and participant_a in dyadic_parties:
        #     pass
        # else:
        if min(pa_df_copy[pa_df_copy['participant']==participant_a]['start_date'].values) < min(pa_df_copy[pa_df_copy['participant']==participant_b]['start_date'].values):
            start_date = min(pa_df_copy[pa_df_copy['participant']==participant_b]['start_date'].values)
            start_year = min(pa_df_copy[pa_df_copy['participant']==participant_b]['start_year'].values)
        else:
            start_date = min(pa_df_copy[pa_df_copy['participant']==participant_a]['start_date'].values)
            start_year = min(pa_df_copy[pa_df_copy['participant']==participant_a]['start_year'].values)
        if max(pa_df_copy[pa_df_copy['participant']==participant_a]['end_date'].values) < max(pa_df_copy[pa_df_copy['participant']==participant_b]['end_date'].values):
            end_date = max(pa_df_copy[pa_df_copy['participant']==participant_a]['end_date'].values)
            end_year = max(pa_df_copy[pa_df_copy['participant']==participant_a]['end_year'].values)
        else:
            end_date = max(pa_df_copy[pa_df_copy['participant']==participant_b]['end_date'].values)
            end_year = max(pa_df_copy[pa_df_copy['participant']==participant_b]['end_year'].values)
        if end_date > start_date:

            df_length = deepcopy(len(dy_df))
            dy_df.loc[df_length, 'war_num'] = war_input
            dy_df.loc[df_length, 'c_code_a'] = c_code_a
            dy_df.loc[df_length, 'participant_a'] = participant_a
            dy_df.loc[df_length, 'c_code_b'] = pa_df_copy[pa_df_copy['participant']==participant_b]['c_code'].values[0]
            dy_df.loc[df_length, 'participant_b'] = participant_b
            dy_df.loc[df_length, 'start_date'] = start_date
            dy_df.loc[df_length, 'start_year'] = start_year
            dy_df.loc[df_length, 'end_date'] = end_date
            dy_df.loc[df_length, 'end_year'] = end_year

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
    ## creating a binary field to represent this dataset
    ## more specific fields can be added later
    dy_df[binary_field] = 1
    dy_df = deepcopy(union_opposite_columns(dy_df))
    # removing any duplicates that may have occured
    duplicate_columns_list = ['c_code_a', 'c_code_b', 'year']
    dy_df.drop_duplicates(subset=duplicate_columns_list, keep='first', inplace=True)
    ## intergrating the descriptive data into the master dataframe for all dyads
    descriptive_df = deepcopy(pd.merge(descriptive_df, dy_df, how='left', on=['c_code_a', 'c_code_b', 'year']))
    descriptive_df.loc[descriptive_df[binary_field].isnull(), binary_field] = 0
    descriptive_df.drop_duplicates(subset=list(descriptive_df.columns), keep='first', inplace=True)

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


def descriptive_participant_from_dyad(file_input, dataframe_input, df_part_renaming_dic):

    ## dyadic data that needs to be adjusted to be non-dyadic (by country, by year)
    if file_input==None:
        df_part = dataframe_input
    else:
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
            df_part.loc[df_part[field]==-8, field] = None
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


def adjust_participant_names(dataframe, grouping_type):

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
