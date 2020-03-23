
import sys
import os
import os.path as op
import pandas as pd
import sqlite3

import db_utils as db
import size_utils as size
import file_helper as files
import dupes_helper as dupes



def main():
    database = ""

    if len(sys.argv)==2:
        database = sys.argv[1]
        con = db.connect(database)
        if con is not None:
            init_msg = f"""Successfully connected to {database}\ntype 'help' for more information"""
            print(init_msg)
            parse(input('>'), con)
    else: print("Please enter a database name")

def parse(cmd, con):
    exit_parse = False
    args = cmd.split()
    function = args[0].strip()
    if function=='help':
        print("""you can execute normal SQL queries with this database
    -to access the table containing your filelist is called 'files'
    -other tables can be created by you or by some of the commands below
or you can use some of the commands below to get information.
    -folder <file_path> - get the contents of the folder at the given path (incl all subfolders)
    -load <file_name> - load csv of files to the main files table
    -sizeof <year> - get the size of data used in that year
    -sizereport <first_year> <last_year> - get report of file size by year for the period between the two years
    -dupesof <file_name> - gets the duplicates of file with given name
    -duplicatereport - gets all duplicate folders grouped by name
    -quit to quit
        """)
    elif function=='load':
        for i in range(1, len(args)):
            file_name = args[i]
            db.load_csv(file_name, con)
    #get size of a certain year
    elif function=='sizeof':
        print(size.of_year(int(args[1]),con))
    #get size report for all years
    elif function=="sizereport":
        print("Table: size_by_year")
        print(size.all_years(con, args[1] , args[2]).to_string())
    #print duplicates of folder
    elif function=="folder":

        print(files.get_folder(args[1],con).to_string(columns=['file_name','kind','location']))
    #print all duplicate files grouped
    elif function=="duplicatereport":
        dupes.report(con,True)
    #print duplicates of a specific file
    elif function=="dupesof":
        dupes_df = dupes.get_dupes_of(args[2],con)
        if dupes_df == None:
            print(f"No Duplicates found for {file_name}")
        else: 
            print(dupes_df.to_string(columns=["file_name","kind","location","year","drive"]))
    elif function=='quit':
        exit_parse=True
        print('bye bye')
        con.close()
    else:
        try:
            cur = con.cursor()
            results = cur.execute(cmd.strip())
            df = pd.read_sql_query(cmd.strip(), con)
            print(df.to_string())
        except sqlite3.Error as e:
            print("Error: Invalid statement or command. Type help for more info.")
    #await next prompt if still open
    if exit_parse is not True:
        parse(input('>'),con)

if __name__ == "__main__":
    main()