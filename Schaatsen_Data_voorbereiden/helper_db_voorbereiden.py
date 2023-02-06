from sqlalchemy.sql import text,select
from sqlalchemy import create_engine
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()

def call_stored_procedure_insert(sql_engine,function_name):
    connection = sql_engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.callproc(function_name)
        connection.commit()
        
    finally:
        connection.close()


def make_db_connection():
    engine=create_engine(os.getenv("db_connect_url"))
    return engine


def get_all_isu_skaters(engine):
    df_isu=pd.read_sql("SELECT * FROM ISU_skaters", con=engine)
    return df_isu

def get_all_spsk_skaters(engine):
    df_isu=pd.read_sql("SELECT * FROM SPSK_skaters", con=engine)
    return df_isu