import sqlite3
import pandas as pd

def get_folder(folder_location, con):
    #cur = con.cursor()
    folder_sql = f"""SELECT file_name, kind, location 
                FROM Files 
                WHERE location LIKE '{folder_location}%'"""
    df = pd.read_sql_query(folder_sql,con)
 #   cur.execute(folder_sql)
   # records = cur.fetchall()
   # return records
    return df
  #  for row in records: 
  #      print('name = ', row[0])
   # cursor.close()