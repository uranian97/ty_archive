import sqlite3
import pandas as pd

#get all unique filenames
def get_filenames(con):
    names_sql="""SELECT DISTINCT file_name FROM files"""
    cur = con.cursor()
    cur.execute(names_sql)
    names = cur.fetchall()
    return names

#return df of all files with file_name if there are more than 1
def get_dupes_of(file_name,con):
    dupes_sql = f'SELECT * FROM files WHERE file_name="{file_name}"'
    df = pd.read_sql_query(dupes_sql,con)
    if len(df.index)>1: return df
    else: return None

#create or update the duplicate report
def report(con, ifprint):
    names = get_filenames(con)
    dfs = []

    for fn in names:
        #get duplicates of each file
        df = get_dupes_of(fn[0],con)
        if df is not None:
            #for files with duplicates, add the to the df and print if needed
            dfs.append(df)
            if ifprint:
                print(df.to_string(columns=["file_name","kind","location","year","drive"]))
                print("\n")
    df = pd.concat(dfs)
    #create or update the duplicate report
    df.to_sql('duplicate_report', con, if_exists='replace', index=False)
    return df

    
