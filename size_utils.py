import sqlite3
import db_utils as db
import pandas as pd

UNITS={'bytes':1,
    'byte':1,
    'KB':1024,
    'MB':1024*1024,
    'GB':1024*1024*1024
    }

#return df of size_by_year
def all_years(conn, yr1,yr2):
    data = {'year':[],'size':[]}
    #for every year in range 
    for yr in range(yr1, yr2):
        s = of_year(yr, conn)
        data['year'].append(yr)
        if s != 'None': data['size'].append(readable_size(float(s)))
        else: data['size'].append(readable_size(float(0)))
    df = pd.DataFrame(data=data)
    return df

#returns the size of files from given year
def of_year(year, conn):
    year_sql=f"""SELECT sum(bytes) AS 'total_bytes' FROM {db.FILELIST} WHERE year=='{year}'"""
    cur = conn.cursor()
    try:
        cur.execute(year_sql)
        total_bytes = str(cur.fetchone()[0])
        return total_bytes
    except: return None

def to_bytes(size):
    print(size)
    num = size.split()[0].strip()
    unit = size.split()[1].strip()
    if num != 'Zero':
        num = float(num)
    else: num = 0
    bs = round(num * UNITS[unit],2)
    return bs
    
#converts bytes to readable vile sizes
def readable_size(bs):
    b=float(bs)
    val = ""
    if b >= 1024:
        if b < 1024*1024:
            kbs = round(b/1024,2)
            val = str(kbs)+" KB"
        elif b < 1024*1024*1024:
            mbs = round(b/(1024*1024),2)
            val = str(mbs)+" MB"
        else: 
            gbs = round(b/(1024*1024*1024),2)
            val=str(gbs)+" GB"
    else:
        val = str(round(b,2)) +" bytes"
    return val
