import pandas as pd
import re

def cleaning_scrap_data(countries_table):
    print('Cleaning scrapped data...')

    list_countries_raw = []
    for country in countries_table:
        list_countries_raw.append(country.text)

    countries = []
    for country in list_countries_raw:
        remove_n = re.sub('\n', '', country)
        remove_all = re.sub(r'^ ', '', remove_n)

        try:
            if remove_all[0] == '(':
                remove_all = remove_all[1:3]
        except:

            continue
        countries.append(remove_all)

    pairs = 2
    rows_refactored = [countries[x:x + pairs] for x in range(0, len(countries), pairs)]

    countries_df = pd.DataFrame(rows_refactored, columns=['country', 'country_code'])
    countries_df['country_code'] = countries_df['country_code'].str.replace('UK', 'GB')
    countries_df['country_code'] = countries_df['country_code'].str.replace('EL', 'GR')

    return countries_df

def merge_table(df_data, jobs_table, countries_table):

    print('merging data...')
    db_api_merged = df_data.merge(jobs_table, how='inner', left_on='normalized_job_code', right_on='uuid')
    all_merged = countries_table.merge(db_api_merged, how='inner', left_on='country_code', right_on='country_code')
    final_df = all_merged[['uuid_x', 'country', 'rural', 'title']]

    return final_df

def fix_errors(final_df):

    final_df['title'] = final_df['title'].fillna('Unemployed')
    final_df['rural'] = final_df['rural'].replace('Country', 'rural')
    final_df['rural'] = final_df['rural'].replace('Non-Rural', 'urban')
    final_df['rural'] = final_df['rural'].replace('city', 'urban')
    final_df['rural'] = final_df['rural'].replace('countryside', 'urban')


    return final_df

def group_final_table(final_df):



    grouped = final_df.groupby(['country', 'title', 'rural']).agg({'uuid_x': 'count'})
    grouped.columns = ['Quantity']
    grouped = grouped.reset_index()
    grouped['Percentage'] = (grouped['Quantity'] / grouped['Quantity'].sum()) * 100
    grouped['Percentage'] = grouped['Percentage'].apply(lambda x: "{0:.2f}%".format(x))

    grouped.sort_values(by='Quantity', ascending=False, inplace= True)

    return grouped

def final_wrangle(df_data, jobs_table, countries_table):

    countries_df= cleaning_scrap_data(countries_table)
    merged_data= merge_table(df_data, jobs_table, countries_table)
    grouped= group_final_table(merged_data)


    return group_final_table(grouped)







