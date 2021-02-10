import pandas as pd
import p_wrangling.m_wrangling as maw


def fix_errors(table_merged):



    table_merged['title'] = table_merged['title'].fillna('Unemployed')
    table_merged['rural'] = table_merged['rural'].replace('Country', 'rural')
    table_merged['rural'] = table_merged['rural'].replace('Non-Rural', 'urban')
    table_merged['rural'] = table_merged['rural'].replace('city', 'urban')
    table_merged['rural'] = table_merged['rural'].replace('countryside', 'urban')

    print('errors fixed')


    return table_merged


def group_final_table(table_merged):

    grouped = table_merged.groupby(['country', 'title', 'rural']).agg({'uuid_x': 'count'})
    grouped.columns = ['Quantity']
    grouped = grouped.reset_index()
    grouped['Percentage'] = (grouped['Quantity'] / grouped['Quantity'].sum()) * 100
    grouped['Percentage'] = grouped['Percentage'].apply(lambda x: "{0:.2f}%".format(x))

    grouped.sort_values(by='Quantity', ascending=False, inplace= True)

    grouped.to_csv('./data/results/results.csv')

    return grouped





