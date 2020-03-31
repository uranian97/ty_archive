import sqlite3
import pandas as pd
import db_utils as db

#get all unique filenames
def get_filenames(con):
    names_sql = f'SELECT * FROM {db.FILELIST}'
    filelist_table = pd.read_sql_query(names_sql,con)
    names = filelist_table['File_Name'].drop_duplicates().values.tolist()
    return names

#return df of all files with file_name if there are more than 1
def get_dupes_of(file_name,con):
    dupes_sql = f'SELECT * FROM {db.FILELIST} WHERE File_Name="{file_name}"'
    df = pd.read_sql_query(dupes_sql,con)
    return df

def all_dupes(df,con):
    file_names = df['File_Name'].values.tolist()
    dfs = []
    for name in file_names:
        dupes_df = pd.read_sql_query(f'SELECT * FROM {db.DUPLICATE_REPORT} WHERE File_Name LIKE {name}',con)
        dfs.append(dupes_df)
        print(dupes_df.to_string(columns=["File_Name","Kind","Location","Year"]))
        print("\n")
    return pd.concat(dfs)


#create or update the duplicate report
def report(con, ifprint=False):
    names = get_filenames(con)
    dfs = []

    for name in names:
        #get duplicates of each file
        df = get_dupes_of(name[0],con)
        if len(df.index)>1:
            #for files with duplicates, add the to the df and print if needed
            dfs.append(df)
           # if ifprint:
            print(df.to_string(columns=["File_Name","Kind","Location","Year"]))
            print("\n")
    df = pd.concat(dfs)
    #create or update the duplicate report
    
    return df

    
