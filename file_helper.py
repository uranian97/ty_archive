import sqlite3
import db_utils as db
import pandas as pd
#from pandasql import sqldf

#pysqldf = lambda q: sqldf(q, globals())

#gets all files withina given location
def get_folder(folder_location, con):
  folder_sql = f"""SELECT * 
              FROM {db.FILELIST} 
              WHERE Path LIKE '{folder_location}%'"""
  
  return pd.read_sql_query(folder_sql,con)

def context_folders_of(files_df, con):
  locations = files_df['Location'].drop_duplicates().values.tolist()
  dfs = []
  #for each unique file location get_folder
  for l in locations:
    df = get_folder(l, con)
    dfs.append(df)
  return pd.concat(dfs)
  


