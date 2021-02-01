import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from unicodedata import normalize
import string
import time
import datetime
import sqlalchemy
from sqlalchemy import create_engine
import sqlite3

#When implementing:
#2. Change table names  

# TO DO:
#1. Add functions for goalie stats
#2. Add backup function at beggining

def get_pstats(year):


    url = 'https://www.hockey-reference.com/leagues/NHL_'+year+'_skaters.html'
    dfsi = pd.read_html(url)
    dfi = dfsi[0]
    dfi = dfi.loc[dfi['Unnamed: 0_level_0']['Rk'] != 'Rk']
    dfi.columns = dfi.columns.droplevel()
    #scraping initial dataframe from hockey-reference.com

    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, 'html.parser')
    slinks = soup.select("th+ .left a")
    surl = [s['href'] for s in slinks]
    #getting list of playerids from links

    pid = [surl[i].split('/')[3].split('.')[0] for i in range(len(surl))]  #adding playerid column
    dfi['pid'] = pid
    dfi = dfi.loc[dfi['Tm'] != 'TOT']  #dropping total rows
    dfi['Year'] = str(int(year)-1)+ '-' + year         #adding year column

    dfi['Player'] = dfi['Player'].str.rstrip('*') #removing * symbol from hall of fame players' names
    dfi['YR_INT'] = dfi['Year'].str[-4:]          #adding col for year as integer
#     dfi['YR_INT'] = dfi['YR_INT'].astype(int)
    dfi = dfi.drop(columns = ['Rk'])
    dfi = dfi.drop(columns = ['ATOI'])            #drop average time on ice column for sql
    dfi.columns = ['Player', 'Age', 'Tm', 'Pos', 'GP', 'G', 'A', 'PTS', '+/-', 'PIM', 'PS', 'EVG', 'PPG', 'SHG', 'GWG', 
     'EVA', 'PPA', 'SHA', 'S', 'S%', 'TOI', 'BLK', 'HIT', 'FOW', 'FOL', 'FO%', 'pid', 'Year', 'YR_INT']  #changing column names -more descriptive
    
    
    
    url_string = 'backup/csv/'+str(year)+'pstats_'+datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')+'.csv'
    dfi.to_csv(url_string, index = False)
    
    print('Updated Season Stats:')
    print(dfi.head())
    print(dfi.shape)
    print(f'writing to {url_string}')
    print('\n')
    return dfi




def pstats_to_sql(dataframe):
    

    
    def deleteMultipleRecords(year):
        try:
            sqliteConnection = sqlite3.connect('hockey.db')
            cursor = sqliteConnection.cursor()
            print("Connected to SQLite")

            # Deleting single record now
            sql_delete_query = f'DELETE from Player_Stats_Reg where YR_INT = {year}'
            cursor.execute(sql_delete_query)
            sqliteConnection.commit()
            print("Record deleted successfully ")
            cursor.close()

        except sqlite3.Error as error:
            print("Failed to delete record from sqlite table", error)
        finally:
            if (sqliteConnection):
                sqliteConnection.close()
                print("the sqlite connection is closed \n")
                
    def insertMultipleRecords(recordList):
        try:
            sqliteConnection = sqlite3.connect('hockey.db')
            cursor = sqliteConnection.cursor()
            print("Connected to SQLite")

            sqlite_insert_query = """INSERT INTO Player_Stats_Reg
                              (Player, Age, Tm, Pos, GP, G, A, PTS, "+/-", PIM, PS,
       EVG, PPG, SHG, GWG, EVA, PPA, SHA, S, "S%", TOI,
       BLK, HIT, FOW, FOL, "FO%", pid, Year, YR_INT) 
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""

            cursor.executemany(sqlite_insert_query, recordList)
            sqliteConnection.commit()
            print("Total", cursor.rowcount, "Records inserted successfully into Player_Stats table")
            sqliteConnection.commit()
            cursor.close()

        except sqlite3.Error as error:
            print("Failed to insert multiple records into sqlite table", error)
        finally:
            if (sqliteConnection):
                sqliteConnection.close()
                print("The SQLite connection is closed \n")
                
                
    ii = [tuple(dataframe.iloc[i].values) for i in range(len(dataframe['pid']))]
    yr = str(dataframe['YR_INT'][0])
            
    deleteMultipleRecords(yr)
    insertMultipleRecords(ii)





dfi = get_pstats('2021')



pstats_to_sql(dfi)  

