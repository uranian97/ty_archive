import sqlite3
import pandas as pd
import db_utils as db
import file_helper as folders

#get all unique filenames
def get_filenames(con):
    names_sql = f'SELECT * FROM {db.FILELIST}'
    filelist_table = pd.read_sql_query(names_sql,con)
    dupe_names = []
    names_list = filelist_table['file_name'].values.tolist()
    for n in names_list:
        if names_list.count(n)>1 and n not in dupe_names:
            dupe_names.append(n)
    
    return dupe_names
    #names = filelist_table['file_name'].drop_duplicates().values.tolist()
    #return names

#return df of all files with file_name if there are more than 1
def get_dupes_of(file_name,con):
    dupes_sql = f'SELECT * FROM {db.FILELIST} WHERE file_name="{file_name}"'
    df = pd.read_sql_query(dupes_sql,con)
    return df

def all_dupes(df,con):
    file_names = df['file_name'].values.tolist()
    dfs = []
    for name in file_names:
        dupes_df = pd.read_sql_query(f'SELECT * FROM {db.DUPLICATE_REPORT} WHERE file_name LIKE {name}',con)
        dfs.append(dupes_df)
        
        pd.set_option('display.max_colwidth',150)
        pd.set_option('display.width',300)
        print(dupes_df.to_string(columns=db.PRINT_COLS))
        print("\n")
    return pd.concat(dfs)

def get_overlap(location1,location2,con):
    folder1 = folders.get_folder(location1,con)
    folder2 = folders.get_folder(location2,con)
    all = pd.concat([folder1, folder2])
    names = names = all['file_name'].drop_duplicates().values.tolist()
    results = []
    for file_name in names:
        dupes_sql = f"""SELECT * 
                        FROM {db.FILELIST} 
                        WHERE file_name='{file_name}' 
                        AND location LIKE '{location1}%'
                        UNION
                        SELECT * 
                        FROM {db.FILELIST} 
                        WHERE file_name='{file_name}' 
                        AND location LIKE '{location2}%'"""
        df = pd.read_sql_query(dupes_sql,con)
        if(df.index)>1:
            results.append(df)
    result = pd.concat(results)
    return result


#create or update the duplicate report
def report2(con, ifprint=False):
    names = get_filenames(con)

    dfs = []

    for name in names:
    
        #get duplicates of each file
        df = get_dupes_of(name,con)
        if len(df.index)>1:
            #for files with duplicates, add the to the df and print if needed
            dfs.append(df)
           # if ifprint:
           # pd.set_option('display.max_colwidth',150)
            #pd.set_option('display.width',300)
            #print(df.to_string(columns=db.PRINT_COLS))
            #print("\n")
    df = pd.concat(dfs)
    return df

def report(con):
    dupes_sql = f"""SELECT a.* 
                    FROM file_list a 
                    JOIN (SELECT file_name, COUNT(*) 
                        FROM file_list 
                        GROUP BY file_name 
                        HAVING count(*) > 1 ) b 
                        ON a.file_name = b.file_name 
                        ORDER BY a.file_name;"""

    return pd.read_sql_query(dupes_sql,con)
    
