
import sys
import os
import os.path as op
import pandas as pd
import sqlite3

import db_utils as db
import size_utils as size
import file_helper as files
import dupes_helper as dupes

function = sys.argv[1]
con = db.connect()

#load in a new csv of files
if function=='load':
    for i in range(2, len(sys.argv)):
        file_name = sys.argv[i]
        db.load_csv(file_name, con)

#get size of a certain year
elif function=='sizeof':
    yr = int(sys.argv[2])
    yr_size = size.of_year(yr,con)
    print(yr_size)

#get size report for all years
elif function=="size_report":
    results = size.all_years(con)
    for yr, s in results.items():
        print(yr + ": "+s)

#print duplicates of folder
elif function=="folder":
    print(files.get_folder(sys.argv[2],con).to_string(columns=['file_name','kind','location']))

#print all duplicate files grouped
elif function=="duplicate_report":
    dupes.report(con)

#print duplicates of a specific file
elif function=="dupesof":
    dupes_df = dupes.get_dupes_of(sys.argv[2],con)
    if dupes_df == None:
        print(f"No Duplicates found for {file_name}")
    else: 
        print(dupes_df.to_string(columns=["file_name","kind","location","year","drive"]))