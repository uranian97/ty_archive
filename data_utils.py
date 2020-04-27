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
    print(parents.to_string(columns=['file_name','parent_folder','path']))
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
    data_path = op.abspath(op.join(os.path.dirname(__file__), f'Data/{table_name}_export.json'))
    df.to_json(path_or_buf=data_path, orient='records',force_ascii=False)


def export_dupes_json():
    data_path = op.abspath(op.join(os.path.dirname(__file__), f'Data/{db.DUPLICATE_REPORT}_export.json'))
    dupes = pd.read_sql_query(f"""SELECT file_name, path FROM {db.DUPLICATE_REPORT}""",db.CON)
    dupes.to_json(path_or_buf=data_path, orient='records',force_ascii=False)    

def get_missing_parents(table_name):
    parents=[]
    table = db.get_table(table_name)
    table_paths = table['path'].values.tolist()
    orphans=pd.read_sql_query(f"""SELECT a.*
                                FROM {table_name} AS a
                                WHERE a.parent_folder NOT IN (SELECT b.file_name FROM {table_name} AS b)
                                AND a.kind NOT LIKE 'Volume' AND a.kind NOT LIKE 'Drive'""",db.CON) #WHERE b.kind LIKE 'Folder' OR b.kind LIKE 'Volume' OR b.kind LIKE 'Drive' 
    for i, row in orphans.iterrows():
        path = folders.get_parent_folder(row['path'])
        if path not in table_paths:
            table_paths.append(path)
            #print(row)
            file_name = row['parent_folder']
            parent_folder = folders.get_parent_folder(path).split('/').pop()
            kind = "Folder"
            if path.count('/') == 2: kind = 'Drive'
            elif path.count('/') == 1: 
                kind = 'Volume'
                parent_folder = '/Archive_Head'
            parent_folder = {
                'file_name':file_name,
                'kind':kind,
                'path':path,
                'location':path,
                'parent_folder':parent_folder,
                'size': 'Zero bytes',
            }
            parents.append(parent_folder)
    parents_df = pd.DataFrame.from_records(parents)
    parents_df.drop_duplicates(inplace=True)
    return parents_df






    