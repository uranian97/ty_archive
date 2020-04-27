import sqlite3
import pandas as pd
import db_utils as db



#gets all files withina given location
def get_folder(folder_location):
  try:
    folder_sql = f"""SELECT * 
                FROM {db.FILELIST} 
                WHERE path LIKE '{folder_location}%'"""
    
    return pd.read_sql_query(folder_sql,db.CON)
  except: return f'Error geting folder at {folder_location}'

def context_folders_of(files_df):
  locations = files_df['location'].drop_duplicates().values.tolist()
  dfs = []
  #for each unique file location get_folder
  for l in locations:
    df = get_folder(l)
    dfs.append(df)
  return pd.concat(dfs)

def get_parent_folder(path):
  folders = path.split("/")
  folders = folders[:-1]
  parent = "/".join(folders)
  return parent

  


