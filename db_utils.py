import os
import sqlite3
import pandas as pd
import sys
import os.path as op

# create a default path to connect to and create (if necessary) a database
# called 'database.sqlite3' in the same directory as this script
dbname = 'TaeyoonArchiveDb'
DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'TaeyoonArchiveDb.sqlite')

FIELDS = {  "file_name": 30,
            "date_modified":9,
            "date_created":9,
            "kind":20,
            "size":6,
            "unit":6,
            "bytes":12,
            "path":50,
            "parent_folder":30,
            "location":50,
            "comments":9,
            "description":11,
            "tags":5,
            "version":8,
            "author":6,
            "title":5,
            "year":5,
            "creator":8,
            "date_added":10}


def connect(db_path=DEFAULT_PATH):
    con = sqlite3.connect(db_path)
    return con

def load_csv(file_name, conn):
    file_path = op.abspath(f'csvs/{file_name}.csv')
    print("file path: " + file_path)

    df = pd.read_csv(file_path)
    #put in in the db 
    #TODO specify datatypes for columns
    df.to_sql('Files', conn, if_exists='append', index=False)

    print("Table loaded.")
    print('Fields:')
    print(get_schema(conn, 'Files'))

def get_tables(con):
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return cur.fetchall()

def get_schema(con, table_name):
    cur = con.cursor()
    cur.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    return cur.fetchone()[0]

