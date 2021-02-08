import sqlite3
from bs4 import BeautifulSoup
import pandas as pd
import requests


url_api= 'http://api.dataatwork.org/v1/jobs/'

web_url= 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'


def load_db_query(path):

   conn=sqlite3.connect(path)

   query= ('select country_info.uuid, country_info.country_code, country_info.rural, career_info.normalized_job_code FROM country_info JOIN career_info ON career_info.uuid = country_info.uuid')

   print(f'getting db from {path}')

   df_data= pd.DataFrame(pd.read_sql_query(query, conn))
   df_data= df_data.rename(columns={'uuid':'uuid_x'})

   df_data.to_csv('./data/raw/df_data.csv')


   print('df-raw is acquired')

   return df_data

def get_api_info(df_data):
   list_api = []
   list_job_codes = set(df_data['normalized_job_code'])
   for i in list_job_codes:
       response = requests.get(f'http://api.dataatwork.org/v1/jobs/{i}')
       result_jobs = response.json()
       list_api.append(result_jobs)

   jobs_table = pd.DataFrame(list_api)
   jobs_table= jobs_table.rename(columns={'uuid': 'normalized_job_code'})
   jobs_table.to_csv('./data/raw/jobs_table.csv')

   print('jobs title table is acquired')

   return jobs_table



def country_code():

   html = requests.get(web_url).content
   soup = BeautifulSoup(html, 'html.parser')
   countries_raw = soup.find_all('td')

   print('countries-raw acquired')

   return countries_raw


def acquire(path):
   print('getting dfs..')

   df_data= load_db_query(path)
   jobs_table= get_api_info(df_data)
   countries_raw= country_code()

   print('acquire finished')

   return df_data, jobs_table, countries_raw











