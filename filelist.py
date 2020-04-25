
import sqlite3
import pandas as pd
import sys
import os
import os.path as op
import db_utils as db
import size_utils as size
import file_helper as folders
import dupes_helper as dupes
import project_helper as proj



CMDS = {
    'help':{'args':'',
            'about':'Display usage instructions'},
    'dbinfo': {'args':'',
            'about':'display table and schema information for the current database'},
    'load':{'args':' <file_name>',
            'about':'Add file to database as a new table with same name as file (UNDER CONSTRUCTION)'},
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
            'about':'updates project lists and metatables'},
    'newtable':{'args':' <table_name> <cmd>',
            'about':'create a new table of a given name from the results of the following command (either sql or custom)'},
    'getschema':{'args':' <table_name>',
            'about': 'gets the schema (column names) of the specified table'},
}

def main():
    #check if valid input
    if len(sys.argv)==2:
        #get database name and connect to it
        db.connect(sys.argv[1])
        if db.CON is not None:
            #print init message
            print(f"""Successfully connected to {db.NAME}\nType 'help' for more information.\n Make sure to call 'init' if this is a new database""")
            while True:
                #ead and parse user input
                user_in = input('>')
                print('\n')
                cmd = user_in.split(maxsplit=1)
                function = cmd[0].strip()
                
                if function == 'quit': break
                elif function == 'help': dbhelp()
                elif function == 'dbinfo': db.info()
                elif function == 'init': db.make_db()
                elif function == 'update' :db.update()
                else: print_result(function, parse(user_in))

        else: print('Could not connect.')
    else: print("Error: Please enter a database name.")

def print_result(fn, res):
    if isinstance(res, pd.DataFrame):
        if fn in ['duplicatereport', 'getfolder', 'dupesof','foldercontext']:
            print(res.to_string(index=False, columns=db.PRINT_COLS,))
        else: print(res.to_string(index=False))
    else:    
        print(res)

def parse(cmd):
    function = cmd.split(maxsplit=1)[0].strip()
    cur = db.CON.cursor()
   
    #TODO: create_csv <file_name> = <cmd>
    #TODO: send above back through parse and return result
    if function == 'sizereport': return db.get_table(db.SIZE_BY_YEAR)
    elif function == 'duplicatereport': return db.get_table(db.DUPLICATE_REPORT)
    elif len(cmd.split(maxsplit=1))>1:
        args = cmd.split(maxsplit=1)[1].strip()
        if function == 'sizeof': 
            try:
                return size.readable_size(size.of_year(int(args)))
            #print error if invalid arg
            except ValueError as ve:
                return(f'{ve}\nPlease enter an integer year')
        #print duplicates of a specific file
        elif function=="dupesof": 
            results = dupes.get_dupes_of(args)
            if len(results.index)>1:
                return results
            else: return "This file has no duplicates"
        #get contents of a folder
        elif function=="getfolder": return folders.get_folder(args)
        #load file to database as table (probably dont use this one yet. im working on it)
        elif function=='load': return db.load(args)
        #print the whole containing folder and any subfolders of a query
        elif function=='toexcel': return db.export_excel(args)
        elif function=="foldercontext":
            subresult = parse(args)
            if isinstance(subresult,pd.DataFrame): 
                return folders.context_folders_of(subresult)
            else: return subresult
        #make new table out of query
        elif function=="newtable":
            try:
                table_name = args.split(maxsplit=1)[0]
                subresult = parse(args.split(maxsplit=1)[1])
                if isinstance(subresult,pd.DataFrame):
                    #check if this table exists
                    #get the count of tables with the name
                    cur.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                    #if the count is 1, then table exists
                    if cur.fetchone()[0]==1: 
                        print(f'{table_name} already exists as a table. Do you want to replace it?')
                        ans = input('y/n>')
                        if ans in ['yes','y','Y']:
                            subresult.to_sql(table_name, db.CON, if_exists='replace',index=False)
                            return 'Table replaced'
                        else: return 'table not replaced'
                    else:
                        subresult.to_sql(table_name, db.CON, if_exists='replace', index=False)
                        return f'table {table_name} successfuly created'
                else:
                    return subresult
            except IndexError as ie:
                return f'{ie}\nPlease specify a table name before your query'
                
        #get schema of a table
        elif function=='getschema':
            try:
                return db.get_schema(args)
            except: return f'Error: Could not get schema of table {args}.'
        elif function not in CMDS.keys():
            #try it as an sql query
            try:
                cur.execute(str(cmd))
                df = pd.read_sql_query(cmd, db.CON)
                return df#.to_string()
            except sqlite3.Error as e:
                return f'{e}\nMake sure you have entered a valid custom command or SQL query'
    else: return "Error: Please enter a valid command or SQL query. type 'help' for more options"

def dbhelp():
    print('About this program:')
    print('You can execute normal SQL queries on this database')
    print('For some examples of useful commands and queries, type "queryhelp"')

    print(f" -the table containing your filelist is called '{db.FILELIST}'")
    print(" -other tables can be created by you or by some of the commands below.")
    print(f" -typing 'dbinfo' will {CMDS['dbinfo']['about']}")
    print('Custom commands and their usage:')
    for k,v in CMDS.items():
        print(f"{k}{v['args']} --  {v['about']}")
    
def query_help():
    print('Here are some SQL queries and command combinations that will help you find things in this database')
    print(f"""To find files related to a certain search term or project, use the query
    SELECT * FROM {db.FILELIST} WHERE path LIKE '%[**insert search here**]%'
    the path this will yield any files with your term in the name, or within files with your term in the name""")
    #file_queries=[f"SELECT * FROM {db.FILELIST} WHERE path LIKE '%[**insert search here**]%'"]
    #proj_queries = [f"SELECT * FROM {proj.PROJECT_LIST} WHERE proj_name LIKE '%[**insert project name here**]%'",
     #               f"SELECT * FROM {proj.PROJECT_LIST} WHERE work LIKE '%[**insert work name here**]%'"]
    

if __name__ == "__main__":
    main()
