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

def extract(url):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    html_page = requests.get(url).text

    data = BeautifulSoup(html_page, "html.parser")

    tables = data.find_all("tbody")
    
    rows = tables[0].find_all("tr")

    
    df = pd.DataFrame(columns = ["Rank",
                        "Bank name",
                        "Market cap (US$ billion)"
                        ])
    
    
    for row in rows:        
        count = 0
        if count < 20:
            col = row.find_all("td")        

            if len(col) != 0:

                data_dict = {"Rank" : int(str(col[0].contents[0]).strip()),
                            "Bank name": col[1].a["title"],
                            "Market cap (US$ billion)": float(str(col[2].contents[0]).strip())
                            }                

                df1 = pd.DataFrame(data_dict, index=[0])
                df =pd.concat([df, df1], ignore_index=True)
                






                
    
    return df

def transfrom(usd, rate):
    """converts usd by the given rate

    returns the conversion    
    """

    return round(usd * rate, 2)



url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"


message = "started scrapping"
log_progress(message)
df = extract(url)

message = "completed scrapping"
log_progress(message)

exchange_rate = pd.read_csv("exchange_rate.csv")

print(exchange_rate)

message = "converting to EUR, GBP, INR"
log_progress(message)
EUR_rate = exchange_rate.iloc[0,1]
GBP_rate = exchange_rate.iloc[1,1]
INR_rate = exchange_rate.iloc[2,1]
df['Market cap (EUR billion)'] = df["Market cap (US$ billion)"].apply(transfrom, rate=EUR_rate)
df['Market cap (GBP billion)'] = df["Market cap (US$ billion)"].apply(transfrom, rate=GBP_rate)
df['Market cap (INR billion)'] = df["Market cap (US$ billion)"].apply(transfrom, rate=INR_rate)


message = "conversion complete"
log_progress(message)

print(df)
