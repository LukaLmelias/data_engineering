# Importing the required libraries
import pandas as pd 
import numpy as np 
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    time = datetime.now()
    with open("code_log.txt", "a") as f:
        f.write(f"\n{message}\t{time}\n")

    return None

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    html_page = requests.get(url).text

    data = BeautifulSoup(html_page, "html.parser")

    tables = data.find_all("tbody")
    
    rows = tables[0].find_all("tr")
    
    df = pd.DataFrame(columns = table_attribs)  
    
    for row in rows:        
        count = 0
        if count < 20:
            col = row.find_all("td")       

            if len(col) != 0:
                data_dict = {"Rank" : int(str(col[0].contents[0]).strip()),
                            "Name": col[1].find_all('a')[1]['title'],
                            "Market cap (US$ billion)": float(str(col[2].contents[0]).strip())
                            }                

                df1 = pd.DataFrame(data_dict, index=[0])
                df =pd.concat([df, df1], ignore_index=True)                      
    
    return df

def exchange(usd, rate):
    """converts usd by the given rate

    returns the conversion    
    """
    return round(usd * rate, 2)

def transform(df, exchange_csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_df = pd.read_csv(exchange_csv_path)

    EUR_rate = exchange_df.iloc[0,1]
    GBP_rate = exchange_df.iloc[1,1]
    INR_rate = exchange_df.iloc[2,1]
    df['MC_EUR_Billion'] = df["Market cap (US$ billion)"].apply(exchange, rate=EUR_rate) # applies the exchange function above 
    df['MC_GBP_Billion'] = df["Market cap (US$ billion)"].apply(exchange, rate=GBP_rate)
    df['MC_INR_Billion'] = df["Market cap (US$ billion)"].apply(exchange, rate=INR_rate)

    return df



def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path, sep=',', encoding='utf-8')

    return None


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



url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"


message = "started scrapping"
log_progress(message)
columns = ["Rank","Name","Market cap (US$ billion)"]
df = extract(url, table_attribs=columns)
message = "completed scrapping"
log_progress(message)




message = "converting to EUR, GBP, INR"
log_progress(message)
df = transform(df, "exchange_rate.csv")
message = "conversion complete"
log_progress(message)
print(df)

message = "loading to csv"
log_progress(message)
csv_path = "Largest_banks_data.csv"
load_to_csv(df,csv_path=csv_path)
message = "Completed loading  to csv"
log_progress(message)



message = "loading to db"
log_progress(message)

db = "Banks.db"

conn = sqlite3.connect(db)

table = "Largest_banks"

load_to_db(df, conn, table)

message = "Completed loading  to db"
log_progress(message)


message = "running queris"
log_progress(message)

query = "SELECT * FROM Largest_banks"
df = run_query(query, conn)
print(df)


average_query = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
average = run_query(average_query, conn)

print(average)


names_query = "SELECT Name from Largest_banks LIMIT 5"

names_output = run_query(names_query, conn)
print(names_output)

message = "Queries complete"
log_progress(message)