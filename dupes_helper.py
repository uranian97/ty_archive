import sqlite3
import pandas as pd


def get_filenames(con):
    names_sql="""SELECT DISTINCT file_name FROM Files"""
    cur = con.cursor()
    cur.execute(names_sql)
    names = cur.fetchall()
    return names

def get_dupes_of(file_name,con):
    dupes_sql = f"SELECT * FROM Files WHERE file_name='{file_name}'"
    df = pd.read_sql_query(dupes_sql,con)
    if len(df.index)>1: return df
    else: return None

def report(con):
    names = get_filenames(con)
    for fn in names:
        df = get_dupes_of(fn[0],con)
        if df != None:
            print(df.to_string(columns=["file_name","kind","location","year","drive"]))
            print("\n")

    
