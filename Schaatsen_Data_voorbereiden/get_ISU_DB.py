# GET ALL WorldRecords from https://api.isuresults.eu/records
import requests
import pandas as pd
from pandas import json_normalize
import time


def get_isu_worldrecord_db(engine):
    df_final=pd.DataFrame()

    URL = "https://api.isuresults.eu/records/?type=WR"
    response= requests.get(url=URL)
    data = response.json()
    df = json_normalize(data,'results')
    df_final=pd.concat([df_final,df], axis=0)

    while ('next' in data) and data['next']!=None :
        response= requests.get(url=data['next'])
        data = response.json()
        df = json_normalize(data,'results')
        df_final=pd.concat([df_final,df], axis=0)
    
    df_final=df_final.drop(['laps'], axis=1)

    df_final.to_sql("Staging_ISU_worldrecord", con=engine,if_exists="replace", chunksize=1000)
    return

def get_isu_skater_db(engine):
    df_final=pd.DataFrame()

    URL = 'https://api.isuresults.eu/skaters/'
    response= requests.get(url=URL)
    data = response.json()
    df = json_normalize(data,'results')
    df_final=pd.concat([df_final,df], axis=0)

    while ('next' in data) and data['next']!=None :
            response= requests.get(url=data['next'])
            data = response.json()
            df = json_normalize(data,'results')
            df_final=pd.concat([df_final,df], axis=0)
        


    # Remove the columns that are not needed and change the names of the 2 other columns.
    df_final=df_final.drop(['dateOfDeath', 'isActive', 'iocCode', 'photo', 'personalBestUrl', 'created', 'modified'], axis=1)

    df_final.to_sql("Staging_ISU_skaters", con=engine,if_exists="replace", chunksize=1000)
    return

def get_isu_skater_personalbest_db(engine):
    df_final=pd.DataFrame()

    URL = 'https://api.isuresults.eu/records/?type=PB'
    response= requests.get(url=URL)
    data = response.json()
    df = json_normalize(data,'results')
    df_final=pd.concat([df_final,df], axis=0)

    while ('next' in data) and data['next']!=None :
            response= requests.get(url=data['next'])
            data = response.json()
            df = json_normalize(data,'results')
            df_final=pd.concat([df_final,df], axis=0)
            time.sleep(0.1)
        


    # Remove the columns that are not needed and change the names of the 2 other columns.
    df_final=df_final.drop(['country', 'created', 'modified', 'distance.identifier', 'distance.name', 'distance.lapCount', 'distance.type', 'distance.resourceUrl'], axis=1)
    df_final=df_final.drop(['laps', 'skater.dateOfDeath', 'skater.isActive', 'skater.iocCode', 'skater.photo', 'skater.personalBestUrl', 'skater.created', 'skater.modified'], axis=1)
    df_final=df_final.drop(['track'], axis=1)


    df_final.to_sql("Staging_ISU_skaters_personalbest", con=engine,if_exists="replace", chunksize=1000)
    return