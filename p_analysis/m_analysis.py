def fix_errors(data):

    data['title'] = data['title'].fillna('Unemployed')
    data['rural'] = data['rural'].replace('Country', 'rural')
    data['rural'] = data['rural'].replace('Non-Rural', 'urban')
    data['rural'] = data['rural'].replace('city', 'urban')
    data['rural'] = data['rural'].replace('countryside', 'urban')


    return data


def group_final_table():

    grouped = final_df.groupby(['country', 'title', 'rural']).agg({'uuid_x': 'count'})
    grouped.columns = ['Quantity']
    grouped = grouped.reset_index()
    grouped['Percentage'] = (grouped['Quantity'] / grouped['Quantity'].sum()) * 100
    grouped['Percentage'] = grouped['Percentage'].apply(lambda x: "{0:.2f}%".format(x))

    grouped.sort_values(by='Quantity', ascending=False, inplace= True)

    return grouped



