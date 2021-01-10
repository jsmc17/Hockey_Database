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
#1. Change SQL query in get_plist to get all players
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
    dfi['Year'] = '2019-2020'          #adding year column

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


def get_plist(dataframe):
    engine = create_engine('sqlite:///hockey.db')
    
    pl_b42020 = pd.read_sql("""
    SELECT pid 
    FROM Player_List1
    WHERE YOB < 2000     

    """, engine)
    
    players_sql = set(pl_b42020['pid'])
    players_scraped = set(dataframe['pid'])

    to_get_list = list(players_scraped.difference(players_sql))
    
    if len(to_get_list) == 0:
        print('No new players to add')
        return 'No new players to add'
    else:
        pid = []
        names = []
        heights = []
        weights = []
        birth_dates = []
        birth_years = []
        cities = []
        states = []
        country_codes = []

        purls = ['https://www.hockey-reference.com/players/'+s[0]+'/'+s+'.html' for s in to_get_list]
        


        for i in range(len(purls)):
            resp2= requests.get(purls[i])
            soup2 = BeautifulSoup(resp2.text, 'html.parser')
            slinks2 = soup2.select("#meta p")


            pl_id = to_get_list[i]
            pl_nm = soup2.select('span')[8].text
            pl_inf = [slinks2[y].text for y in range(len(slinks2))]


            if 'Left' in pl_inf[0]:
                shoots = 'Left'
            elif 'Right' in pl_inf[0]:
                shoots = 'Right'
            else:
                shoots = ''

            if 'cm' in pl_inf[1]:
                height = pl_inf[1].split(',')[0]
            else:
                height = ''
            if 'lb' in pl_inf[1]:
                if len(pl_inf[1].split(',')) == 1:
                    weight = pl_inf[1][0:3]
                else:
                    weight = pl_inf[1].split(',')[1][1:4]
            else:
                weight = ''

            if 'Born' in pl_inf[1]:
                bd = pl_inf[1].split('Born: ')[1].split(',')[0]
                byr = pl_inf[1].split('Born: ')[1].split(',')[1][1:5]
            elif len(pl_inf) > 2:
                if 'Born' in pl_inf[2]:
                    bd = pl_inf[2].split('Born: ')[1].split(',')[0]
                    byr = pl_inf[2].split('Born: ')[1].split(',')[1][1:5]
            else:
                bd = ''
                byr = ''


            if ' in' in pl_inf[1]:
                locat = pl_inf[1].split(' in')[1]
                locat_norm = normalize('NFKD', locat).strip()
                if len(locat_norm.split(',')) == 1:
                    country_code = locat_norm.split('  ')[1].strip()
                    city = locat_norm.split(',')[0]
                    state = ''
                else:
                    state = locat_norm.split(',')[1].strip().split('  ')[0]
                    country_code = locat_norm.split(',')[1].strip().split('  ')[-1]
                    city = locat_norm.split(',')[0]

            if len(pl_inf) > 2:
                if ' in' in pl_inf[2]:
                    locat = pl_inf[2].split(' in')[1]
                    locat_norm = normalize('NFKD', locat).strip()
                    if len(locat_norm.split(',')) == 1:
                        country_code = locat_norm.split('  ')[1].strip()
                        city = locat_norm.split(',')[0]
                        state = ''
                    else:
                        state = locat_norm.split(',')[1].strip().split('  ')[0]
                        country_code = locat_norm.split(',')[1].strip().split('  ')[-1]
                        city = locat_norm.split(',')[0]
                else: 
                    city = ''
                    state = ''
                    country_code = ''
            else: 
                    city = ''
                    state = ''
                    country_code = ''

    #         print(pl_id, pl_nm, height, weight, bd, byr, city, state, country_code)             #printing inside loop for visualization
            pid.append(pl_id)
            names.append(pl_nm)
            heights.append(height)
            weights.append(weight)
            birth_dates.append(bd)
            birth_years.append(byr)
            cities.append(city)
            states.append(state)
            country_codes.append(country_code)
            
        df = pd.DataFrame(list(zip(pid,names, heights,weights, birth_dates,birth_years,cities,states,country_codes)),
                columns = ['pid', 'Player', 'height', 'weight', 'dob', 'yob', 'city', 'state', 'country'])

        inches = []
        lhght = list(df['height'])
        lspl = list(df['height'].str.split('-'))
        for i in range(len(df['height'])):
            if type(lhght[i]) != float:
                try:
                    ht = (int(lspl[i][0]) * 12 + int(lspl[i][1]))
                except ValueError:
                    ht = ''
            else:
                ht = ''
            inches.append(str(ht))


        dates = []
        ldob = list(df['dob'])
        lyob = list(df['yob'])
        for i in range(len(df['dob'])):
            if (type(ldob[i]) != float):
                try:
                    dt = (ldob[i] + ' ' + str(int(lyob[i]))).lstrip()
                except ValueError:
                    dt = ''
            else:
                dt = ''
            dates.append(dt)

        dts = []
        for i in range(len(df['dob'])):
            if dates[i] != '':
                dtt = datetime.datetime.strptime(dates[i], '%B %d %Y')
                dttt = datetime.datetime.strftime(dtt, '%Y-%m-%d')
            else:
                dttt = ''
            dts.append(dttt)


        df['height_in'] = inches
        df['born'] = dts
        
        url_string = 'backup/csv/'+'plist_'+datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')+'.csv'
        df.to_csv(url_string, index = False)
        
        print('New to Player List:')
        print(df.head())
        print(df.shape)
        print(f'writing to {url_string}')
        print('\n')
    
        return df

def pstats_to_sql(dataframe):
    

    
    def deleteMultipleRecords(year):
        try:
            sqliteConnection = sqlite3.connect('hockey.db')
            cursor = sqliteConnection.cursor()
            print("Connected to SQLite")

            # Deleting single record now
            sql_delete_query = f'DELETE from Player_Stats_Reg3 where YR_INT = {year}'
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

            sqlite_insert_query = """INSERT INTO Player_Stats_Reg3
                              (Player, Age, Tm, Pos, GP, G, A, PTS, "+/-", PIM, PS,
       EVG, PPG, SHG, GWG, EVA, PPA, SHA, S, "S%", TOI,
       BLK, HIT, FOW, FOL, "FO%", pid, Year, YR_INT) 
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""

            cursor.executemany(sqlite_insert_query, recordList)
            sqliteConnection.commit()
            print("Total", cursor.rowcount, "Records inserted successfully into Player_List table")
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

def plist_to_sql(dataframe):

    record_list = [tuple(dataframe.iloc[i].values) for i in range(len(dataframe['pid']))]
    
    try:
        sqliteConnection = sqlite3.connect('hockey.db')
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_insert_query = """INSERT INTO Player_List1
                          (pid, Player, height, weight, dob, yob, city, state, country, height_in, born) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""

        cursor.executemany(sqlite_insert_query, record_list)
        sqliteConnection.commit()
        print("Total", cursor.rowcount, "Records inserted successfully into Player_List table")
        sqliteConnection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert multiple records into sqlite table", error)
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("The SQLite connection is closed")





dfi = get_pstats('2020')

dfp = get_plist(dfi)

pstats_to_sql(dfi)  

if type(dfp) != str:
    plist_to_sql(dfp)