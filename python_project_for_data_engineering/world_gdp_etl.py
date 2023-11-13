
# Code for ETL operations on Country-GDP data
# Importing the required libraries
import pandas as pd 
import numpy as np 
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime




def extract(url):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    
    html_page = requests.get(url).text


    data = BeautifulSoup(html_page, "html.parser")

    tables = data.find_all("tbody")

    rows = tables[2].find_all("tr")
    df = pd.DataFrame(columns = ["Country/Territory",
                        "UN region",
                        "IMF Estimate", 
                        "IMF Year",
                        "World Bank Estimate",
                        "World Bank Year",
                        "United Nations Estimate",
                        "United Nations Year"
                        ])

    for row in rows:
        
        count = 0
        if count < 20:
            col = row.find_all("td")
            

            if len(col) != 0:
                
                #print(col[1].contents[0])
                if col[1].contents[0] == "—" :
                    continue

                data_dict = {"Country/Territory" :col[0].a["href"],
                            "UN region": col[1].contents[0],
                            "IMF Estimate": col[2].contents[0],
                            "IMF Year": str(col[3].contents[-1]).strip(),

                            "World Bank Estimate": col[4].contents[0],
                            "World Bank Year": str(col[5].contents[-1]).strip(),
                            "United Nations Estimate": col[-2].contents[0],
                            "United Nations Year": str(col[-1].contents[-1]).strip(),
                            }

                df1 = pd.DataFrame(data_dict, index=[0])
                df =pd.concat([df, df1], ignore_index=True)
                
                count += 1
  

    
    

    return df
# def convert_to_float_with_commas(value):
#     """Converts an int/str to float

#     """
#     return float(value.replace(',', ''))

def millions_to_billions(value):
    """convert millions to billions
    
    """

    return value/1000


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    
    gdps= ["IMF Estimate","World Bank Estimate","United Nations Estimate"]
    
    df = df.replace('—', None)
    
    df[gdps] =  df[gdps].replace(',', '', regex=True).astype(float)

    df[gdps] = df[gdps].apply(millions_to_billions)



    
    return df




def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path,sep=',', encoding='utf-8')

    return None


url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'


df = extract(url)
df = transform(df)
print(df.head())

csv_path = "./world_gdp.csv"
load_to_csv(df, csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''


    df.to_sql(table_name, sql_connection,  if_exists = "replace", index=False)

    return None



def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    df = pd.read_sql(query_statement, sql_connection)

    return df

def log_progress(message, log_file_path):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    
    time = datetime.now()
    with open(log_file_path, "a") as f:
        f.write(f"{message}\t{time}\n")

    return None


world_gdp_db = "world_gpd.db"

conn = sqlite3.connect(world_gdp_db)

table = "wold_gdp"

load_to_db(df, conn, table)

message = "STARTING QUERY"
log_file = "./log.txt"
log_progress(message, log_file)
query_statement = f"SELECT * FROM {table}"

print(query_statement)

df = run_query(query_statement, conn)
print(df)



conn.close()