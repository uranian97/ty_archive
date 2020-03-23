import sqlite3
import pandas as pd

#gets all files withina given location
def get_folder(folder_location, con):

    folder_sql = f"""SELECT file_name, kind, location 
                FROM files 
                WHERE location LIKE '{folder_location}%'"""
    return pd.read_sql_query(folder_sql,con)
 #   cur.execute(folder_sql)
   # records = cur.fetchall()
   # return records
  #  for row in records: 
  #      print('name = ', row[0])
   # cursor.close()