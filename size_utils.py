import sqlite3
import db_utils as db
import pandas as pd

#return df of size_by_year
def all_years(conn, yr1,yr2):
    
    data = {'year':[],'size':[]}
    #for every year in range 
    for yr in range(int(yr1), int(yr2)):
        s = of_year(yr, conn)
        #if there are files from that year
        if s != 'None':
            #sum their sizes and add them to the data
            data['year'].append(yr)
            data['size'].append(readable_size(float(s)))
    
    #create or update the size_by_year table
    df = pd.DataFrame(data=data)
    df.to_sql('size_by_year', conn, if_exists='replace', index=False)
    return df

#returns the size of files from given year
def of_year(year, conn):
    year_sql=f"""SELECT sum(bytes) AS 'total_bytes' FROM files WHERE year=='{year}'"""
    cur = conn.cursor()
    cur.execute(year_sql)
    total_bytes = str(cur.fetchone()[0])
    return total_bytes

#converts bytes to readable vile sizes
def readable_size(bs):
    b=float(bs)
    val = ""
    if b >= 1024:
        if b < 1024*1024:
            kb = round(b/1024,2)
            val = str(kb)+" KB"
        elif b < 1024*1024*1024:
            mb = round(b/(1024*1024),2)
            val = str(mb)+" MB"
        else: 
            gb = round(b/(1024*1024*1024),2)
            val=str(gb)+" GB"
    else:
        val = str(round(b,2)) +" bytes"
    return val