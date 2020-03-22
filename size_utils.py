import sqlite3

def all_years(conn):
    sizes = {}
    for yr in range(1996,2020):
        s = of_year(yr, conn)
        if s != 'None':
            sizes.update({str(yr): readable_size(float(s))})
    return sizes

def of_year(year, conn):
    year_sql=f"""SELECT sum(bytes) AS 'total_bytes' 
                FROM Files
                WHERE year=='{year}'"""
    cur = conn.cursor()
    cur.execute(year_sql)
    total_bytes = str(cur.fetchone()[0])
    return total_bytes

def readable_size(b):
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