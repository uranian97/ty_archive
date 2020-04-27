import json
import os
import os.path as op
import sqlite3
import sys

import pandas as pd
import sqlalchemy.types as dtypes

import data_utils as data
import db_utils as db
import dupes_helper as dupes
import file_helper as folders
import project_helper as proj
import size_utils as size

HEAD = 'Archive_Head'

def generate_json_data():
    export_filelist_json(db.FILELIST)
    export_dupes_json()
    
#export 
def export_filelist_json(table_name):
    df = db.get_table(table_name)
    parents = get_missing_parents(table_name)
    print(parents.to_string())
    df.append(parents, ignore_index=True)
    df['parent_folder'] = pd.Series([folders.get_parent_folder(x) for x in df['path']])
    head = [{
        'file_name':'Archive_Head',
        'kind':'Head',
        'path':'/Archive_Head',
        'location':'/Archive_Head'
    }]
    head_df = pd.DataFrame(head)
    df.append(head_df, ignore_index=True)
    check_parents(df)
    data_path = op.abspath(op.join(os.path.dirname(__file__), f'Data/{table_name}_export.json'))
    df.to_json(path_or_buf=data_path, orient='records',force_ascii=False)


def export_dupes_json():
    data_path = op.abspath(op.join(os.path.dirname(__file__), f'Data/{db.DUPLICATE_REPORT}_export.json'))
    dupes = pd.read_sql_query(f"""SELECT file_name, path FROM {db.DUPLICATE_REPORT}""",db.CON)
    dupes.to_json(path_or_buf=data_path, orient='records',force_ascii=False)    

def get_missing_parents(table_name):
    parents=[]
    paths = []
    #pd.read_sql_query(f"SELECT path FROM {table_name}",db.CON).values.tolist()
    orphans=pd.read_sql_query(f"""SELECT a.*
                                FROM {table_name} AS a
                                WHERE a.parent_folder NOT IN (SELECT b.file_name FROM {table_name} AS b)
                                AND a.kind NOT LIKE 'Volume' AND a.kind NOT LIKE 'Drive'""",db.CON) #WHERE b.kind LIKE 'Folder' OR b.kind LIKE 'Volume' OR b.kind LIKE 'Drive' 
    for i, row in orphans.iterrows():
        print(row)
        path = folders.get_parent_folder(row['path'])
        if path not in paths:
            paths.append(path)
            #print(row)
            parent_folder = folders.get_parent_folder(path).split('/').pop()
            kind = "Folder"
            if path.count('/') == 2: kind = 'Drive'
            elif path.count('/') == 1: 
                kind = 'Volume'
                parent_folder = '/Archive_Head'
            parent = {
                'file_name':row['parent_folder'],
                'kind':kind,
                'path':path,
                'location':path,
                'parent_folder':parent_folder,
                'size': 'Zero bytes',
            }
            parents.append(parent)
    parents_df = pd.DataFrame.from_records(parents)
    parents_df.drop_duplicates(inplace=True)
    print(parents_df.to_string())
    return parents_df


def check_parents(df):
    paths = df['path'].values.tolist()
    parents = df['parent_folder'].values.tolist()

    for p in parents:
        if p not in paths:
            print(f"Missing {p}")

    