
import sys
import os
import os.path as op
import pandas as pd
import sqlite3
import db_utils as db
import size_utils as size
import file_helper as folders
import dupes_helper as dupes


CMDS = {
    'help':{'args':'',
            'about':'Display usage instructions'},
    'dbinfo': {'args':'',
            'about':'display table and schema information for the current database'},
    'load':{'args':' <file_name>',
            'about':'Add file to database as a new table with same name as file'},
    'sizeof':{'args':' <year>', 
            'about':'Display the size of data used in <year>'},
    'sizereport':{'args':'',
            'about':f"Create and print a table called '{db.SIZE_BY_YEAR}' containing file size by year"},
    'getfolder':{'args':' <file_path>',
            'about':'Prints the contents of the folder at the given path (incl all subfolders).'},
    'foldercontext':{'args':' <cmd>',
            'about':'returns the result of <cmd> with the contents of all surrounding folders and subfolders'},
    'duplicatereport':{'args':'',
            'about':f"Creates and prints a table called '{db.DUPLICATE_REPORT}' containing all duplicate filed grouped by name."},
    'dupesof':{'args':' <file_name>',
            'about':'Gets all files with file_name <file_name>.'},
    'quit':{'args':'',
            'about':'Quits the program.'},
    'init':{'args':'',
            'about':'Initializes Filelist with files in Filelists folder'},
    'update':{'args':'',
            'about':'Call this after adding new sheets to the Filelists folder in order to update the Filelist and generate new meta tables.'},
    'newtable':{'args':' <table_name> <cmd>',
            'about':'create a new table of a given name from the results of the following command (either sql or custom)'},
    'getschema':{'args':' <table_name>',
            'about': 'gets the schema (column names) of the specified table'},
}

def main():
    #check if valid input
    if len(sys.argv)==2:
        #get database name and connect to it
        database = sys.argv[1]
        con = db.connect(database)
        if con is not None:
            #print init message
            print(f"""Successfully connected to {database}\nType 'help' for more information.\n Make sure to call 'init' if this is a new database""")
            while True:
                #ead and parse user input
                user_in = input('>')
                print('\n')
                cmd = user_in.split(maxsplit=1)
                function = cmd[0].strip()
                
                if function == 'quit': break
                elif function == 'help': dbhelp()
                elif function == 'dbinfo': dbinfo(con)
                elif function == 'init' or function == 'update': db.make_db(con)
                else: print_result(function, parse(user_in,con), con)

        else: print('Could not connect.')
    else: print("Error: Please enter a database name.")

def print_result(fn, res,con):
    if isinstance(res, pd.DataFrame):
        if fn in ['duplicatereport', 'getfolder', 'dupesof','foldercontext']:
            print(res.to_string(index=False, columns=db.PRINT_COLS,))
        else: print(res.to_string(index=False))
    else:    
        print(res)

def parse(cmd, con):
    function = cmd.split(maxsplit=1)[0].strip()
    cur = con.cursor()
   
    #TODO: create_csv <file_name> = <cmd>
    #TODO: send above back through parse and return result
    if function == 'sizereport': return db.get_table(db.SIZE_BY_YEAR,con)
    elif function == 'duplicatereport': return db.get_table(db.DUPLICATE_REPORT,con)
    elif len(cmd.split(maxsplit=1))>1:
        args = cmd.split(maxsplit=1)[1].strip()
        if function == 'sizeof': 
            try:
                return size.readable_size(size.of_year(int(args),con))
            #print error if invalid arg
            except ValueError as ve:
                return(f'{ve}\nPlease enter an integer year')
        #print duplicates of a specific file
        elif function=="dupesof": 
            results = dupes.get_dupes_of(args,con)
            if len(results.index)>1:
                return results
            else: return "This file has no duplicates"
        #get contents of a folder
        elif function=="getfolder": return folders.get_folder(args,con)
        #load file to database as table
        elif function=='load': return db.load(args,con)
        #print the whole containing folder and any subfolders of a query
        elif function=="foldercontext":
            subresult = parse(args,con)
            if isinstance(subresult,pd.DataFrame): 
                return folders.context_folders_of(subresult,con)
            else: return subresult
        #make new table out of query
        elif function=="newtable":
            try:
                table_name = args.split(maxsplit=1)[0]
                subresult = parse(args.split(maxsplit=1)[1],con)
                if isinstance(subresult,pd.DataFrame):
                    #check if this table exists
                    #get the count of tables with the name
                    cur.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                    #if the count is 1, then table exists
                    if cur.fetchone()[0]==1: 
                        print(f'{table_name} already exists as a table. Do you want to replace it?')
                        ans = input('y/n>')
                        if ans in ['yes','y','Y']:
                            subresult.to_sql(table_name, con, if_exists='replace')
                        else: return 'table not created'
                    
                return subresult
            except IndexError as ie:
                print(f'{ie}\n Please specify a table name before your query')
        #get schema of a table
        elif function=='getschema':
            try:
                return db.get_schema(con,args)
            except: return f'Error: Could not get schema of table {args}.'
        elif function not in CMDS.keys():
            #try it as an sql query
            try:
                cur.execute(str(cmd))
                df = pd.read_sql_query(cmd, con)
                return df#.to_string()
            except sqlite3.Error as e:
                return f'{e}\n Make sure you have entered a valid custom command or SQL query'
    else: return "Error: Please enter a valid command or SQL query. type 'help' for more options"

def dbhelp():
    print('About this program:')
    print('You can execute normal SQL queries on this database')
    print(f" -the table containing your filelist is called '{db.FILELIST}'")
    print(" -other tables can be created by you or by some of the commands below.")
    print(f" -typing 'dbinfo' will {CMDS['dbinfo']['about']}")
    print('Custom commands and their usage:')
    for k,v in CMDS.items():
        print(f"{k}{v['args']} --  {v['about']}")

def dbinfo(con):
    about_txt = f"""About this database:
    file_list Schema:
    {db.get_schema(con, db.FILELIST)}\n
    Tables:
    {db.get_tables(con).to_string()}\n
    type getschema <table_name> to see a specific tables columns."""
    print(about_txt)
            

if __name__ == "__main__":
    main()
