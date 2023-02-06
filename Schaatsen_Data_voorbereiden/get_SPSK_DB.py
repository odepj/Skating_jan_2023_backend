#  GET ALL Skaters from https://api.isuresults.eu
import requests
import pandas as pd
import time
import datetime

from pandas import json_normalize
from helper_db_voorbereiden import make_db_connection, get_all_isu_skaters, get_all_spsk_skaters


def get_skater_spsk_id(givenname,familyname):
    SkaterLookupURL="https://speedskatingresults.com/api/json/skater_lookup.php"
    parameters = {'givenname':givenname,'familyname':familyname} 
    r = requests.get(url = SkaterLookupURL, params = parameters) 
    data = r.json() 
    results = json_normalize(data)
    df_skater_spsk = pd.json_normalize(results.skaters[0])

    return df_skater_spsk

def get_all_isu_skaters_spsk(engine):
    #engine=make_db_connection()
    df_skater=get_all_isu_skaters(engine)

    #testing only one API breaks with a lot of requests
    #df_skater= df_skater.loc[df_skater['familyname']=='de Vries']

    df_spsk=pd.DataFrame()
    for i, row in df_skater.iterrows(): 

        givenname = row['givenname']
        familyname = row['familyname']
        skater_spsk = get_skater_spsk_id(givenname, familyname)
        skater_spsk['dateofbirth']=row['dateofbirth']
        skater_spsk['ISU_skater_id']=row['ISU_skater_id']
        #print(skater_spsk)
        print(familyname)
        
        df_spsk=pd.concat([df_spsk,skater_spsk],axis=0)
        time.sleep(0.1)


    df_spsk=df_spsk.drop_duplicates()
    df_spsk.reset_index()
    df_spsk.to_sql("Staging_SPSK_skaters", con=engine,if_exists="replace", chunksize=1000)
    return 

def get_spsk_skater_distance_seasonalbest(spsk_id, distance, start):

    seasonBestUrl = "https://speedskatingresults.com/api/json/season_bests.php"
    Parameters = {'skater': spsk_id, 'start': start, 'distance':distance}
    r = requests.get(url=seasonBestUrl, params=Parameters)
    data = r.json()
    results = json_normalize(data)
    results_sb = pd.json_normalize(results.seasons[0])

    # results_SeasonalBest is a nested Json, needs to be unnested
    df_skater_dist_sb=pd.DataFrame()
    for i, row in results_sb.iterrows():
        season_year=(row['start'])

        unnest_SB=pd.json_normalize(row.records)
        spsk_time = unnest_SB['time'].iloc[0]
        date = unnest_SB['date'].iloc[0]
        location = unnest_SB['location'].iloc[0]
        season_skater_dist_sb=[[spsk_id, season_year, distance, spsk_time, date, location]]
        df_season_skater_dist_sb=pd.DataFrame(season_skater_dist_sb,columns=['spsk_id', 'season_year', 'distance', 'spsk_time','date', 'location'])
        df_skater_dist_sb=pd.concat([df_skater_dist_sb,df_season_skater_dist_sb],axis=0)
    return df_skater_dist_sb



def get_skater_all_spsk_seasonalbest(spsk_id, start):

    seasonBestUrl = "https://speedskatingresults.com/api/json/season_bests.php"
    Parameters = {'skater': spsk_id, 'start': start}
    r = requests.get(url=seasonBestUrl, params=Parameters)
    data = r.json()
    results = json_normalize(data)
    unnest_sb = pd.json_normalize(results.seasons[0])

    # results_SeasonalBest is a nested Json, needs to be unnested
    df_skater_all_sb=pd.DataFrame()
    for i, row in unnest_sb.iterrows():
        season_year=(row['start'])
        unnest_records=pd.json_normalize(unnest_sb.records[i])
        for j, row_records in unnest_records.iterrows():
            distance=row_records['distance']
            spsk_time= row_records['time']
            date = row_records['date']
            location = row_records['location']
            ls_skater_sb=[[spsk_id, season_year, distance, spsk_time, date, location]]
            df_skater_sb=pd.DataFrame(ls_skater_sb,columns=['SPSK_skater_id', 'season_year', 'distance', 'spsk_time','date', 'location'])
            df_skater_all_sb=pd.concat([df_skater_all_sb,df_skater_sb],axis=0)
    return df_skater_all_sb


def get_all_skaters_all_spsk_seasonalbest(engine):
    
    df_skater=get_all_spsk_skaters(engine)

    # testing only one API breaks with alot of requests
    # df_skater= df_skater.loc[df_skater['familyname']=='de Vries']

    df_all_skater_all_sb=pd.DataFrame()
    for i, row in df_skater.iterrows(): 
        
        spsk_id = row['SPSK_skater_id']
        loadyear=row['loadyear']
        if (loadyear==datetime.datetime.now().year) :
            start=0   
        else:
            start=datetime.datetime.now().year-1
        
        df_skater_all_sb = get_skater_all_spsk_seasonalbest(spsk_id, start)
        print(row['familyname'])
        df_all_skater_all_sb=pd.concat([df_all_skater_all_sb,df_skater_all_sb],axis=0)
        time.sleep(0.1)


    df_all_skater_all_sb= df_all_skater_all_sb.drop_duplicates()
    df_all_skater_all_sb.to_sql("Staging_SPSK_seasonalbest", con=engine,if_exists="replace", chunksize=1000)
    return 