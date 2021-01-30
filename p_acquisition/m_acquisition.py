from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re

url_api= 'http://api.dataatwork.org/v1/jobs/'

web_url= 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'


def load_db_query(path):

   print('getting db...')

   conn_str= 'sqlite:///./data/raw/raw_data_project_m1.db'

   engine = create_engine(conn_str)

   query= ('select country_info.uuid, country_info.country_code, country_info.rural, career_info.normalized_job_code FROM country_info JOIN career_info ON career_info.uuid = country_info.uuid' , engine)

   read_query= pd.read_sql_query(query, engine)

   df_data= pd.DataFrame(read_query)

   return df_data

def get_api_info(df_data):
   list_api = []
   list_job_codes = set(df_data['normalized_job_code'])
   for i in list_job_codes:
       response = requests.get(f'http://api.dataatwork.org/v1/jobs/{i}')
       result_jobs = response.json()
       list_api.append(result_jobs)

   jobs_table = pd.DataFrame(list_api)

   return jobs_table



def country_code():

   html = requests.get(web_url).content
   soup = BeautifulSoup(html, 'html.parser')
   countries_table = soup.find_all('td')

   return countries_table


def acquire(path):
   print('getting dfs..')

   df_data= load_db_query(path)
   jobs_table= get_api_info(df_data)
   countries_table= country_code()

   return df_data, jobs_table, countries_table











