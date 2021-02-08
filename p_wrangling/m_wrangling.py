import pandas as pd
import re



def cleaning_scrap_data(countries_raw):
    print('Cleaning scrapped data...')

    #text of the codes and countries

    list_countries_raw = []
    for country in countries_raw:
        list_countries_raw.append(country.text)

    #cleaning the spaces and /n from the countries

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

    #Each country and code to list of lists for getting the df after

    pairs = 2
    countries_pairs = [countries[x:x + pairs] for x in range(0, len(countries), pairs)]
    countries_df = pd.DataFrame(countries_pairs, columns=['country', 'country_code'])
    countries_df['country_code'] = countries_df['country_code'].str.replace('UK', 'GB')
    countries_df['country_code'] = countries_df['country_code'].str.replace('EL', 'GR')
    countries_df.to_csv('./data/raw/countries_df.csv')


    print('scraped data ok')

    return countries_df

def merge_table(df_data, countries_df, jobs_table):

    print('merging data')

    country_code_merge= pd.merge(left= df_data, right=countries_df, how= 'left', left_on= 'country_code', right_on='country_code')
    table_merged= pd.merge(left= country_code_merge, right= jobs_table, how='left', left_on= 'normalized_job_code', right_on= 'normalized_job_code')

    table_merged.to_csv('./data/raw/table_merged.csv')



    print('data merged')

    return table_merged








