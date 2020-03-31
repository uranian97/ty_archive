
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
            'about':'Display usage instructions',
            'error':''},
    'dbinfo': {'args':'',
            'about':'display table and schema information for the current database',
            'error':''},
    'load':{'args':' <file_name>',
            'about':'Add file to database as a new table with same name as file',
            'error':'Error: Could not load file(s)'},
    'sizeof':{'args':' <year>', 
            'about':'Display the size of data used in <year>',
            'error':'Error: Could not calculate year'},
    'sizereport':{'args':'',
            'about':f"Create and print a table called '{db.SIZE_BY_YEAR}' containing file size by year",
            'error':''},
    'contentsoffolder':{'args':' <file_path>',
            'about':'Prints the contents of the folder at the given path (incl all subfolders).',
            'error':'Error: could not get contents of given folder'},
    'foldercontext':{'args':' <cmd>',
            'about':'returns the result of <cmd> with the contents of all surrounding folders and subfolders',
            'error':'Error: could not complete command'},
    'duplicatereport':{'args':'',
            'about':f"Creates and prints a table called '{db.DUPLICATE_REPORT}' containing all duplicate filed grouped by name.",
            'error':'Error: trouble generating duplicate report'},
    'dupesof':{'args':' <file_name>',
            'about':'Gets all files with file_name <file_name>.',
            'error':'Error: No duplicates found for given filename'},
    'quit':{'args':'',
            'about':'Quits the program.',
            'error':'Error: could not quit'},
    'init':{'args':'',
            'about':'Initializes Filelist with files in Filelists folder',
            'error':''},
    'update':{'args':'',
            'about':'Call this after adding new sheets to the Filelists folder in order to update the Filelist and generate new meta tables.',
            'error':'Error: could not quit'},
    'newtable':{'args':' <table_name> <cmd>',
            'about':'create a new table of a given name from the results of the following command (either sql or custom)',
            'error':'Error: could not create table. please check your table name and command for errors'},
}

def main():
    #check if valid input
    if len(sys.argv)==2:
        #get database name and connect to it
        database = sys.argv[1]
        con = db.connect(database)
        if con is not None:
            #print init message
            print(f"""Successfully connected to {database}\nType 'help' for more information.""")
            while True:
                #ead and parse user input
                user_in = input('>')
                cmd = user_in.split(maxsplit=1)
                function = cmd[0].strip()
                
                if function == 'quit':break
                elif function == 'help': dbhelp()
                elif function == 'dbinfo': dbinfo(con)
                elif function == 'init' or function == 'update': db.make_db(con)
                elif function == 'sizeof':
                    if int(cmd[1]): print(size.readable_size(size.of_year(int(cmd[1]),con)))
                    #print error if invalid arg
                    else: print(CMDS['sizeof']['error'])
                #if not the above, parse database command
                else:
                    results = parse(user_in, con)
                    pd.set_option('display.max_colwidth', 100)
                    if results is not None: #'None':
                        if function in CMDS.keys() and function not in ['sizeof','load']: 
                            #use only these columns for general filelist queries
                            print(results.to_string(index=False,columns=pd.Index(['File_Name', 'Kind', 'Location', 'Year' ],dtype='object')))
                        else:
                            print(results.to_string())
                    #otherwise print error msg
                    else: print(CMDS[function]['error'])
    else: print("Error: Please enter a database name.")

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
    Filelist Schema:
    {db.get_schema(con, db.FILELIST)}\n
    Tables:
    {db.get_tables(con).to_string()}"""
    print(about_txt)

def parse(user_in, con):
    cmd = user_in.split(maxsplit=1)
    function = cmd[0].strip()
   

    #TODO: newtable <tablename> = <cmd>
    #TODO: create_csv <file_name> = <cmd>
    #TODO: send above back through parse and return result
    
    #load file to database as table
    if function=='load': 
        return db.load(cmd[1].strip(),con)
    #get size report for all years
    elif function=="sizereport": 
        return size.all_years(con, 1980, 2020)
    #print duplicates of folder
    elif function=="contentsoffolder": return folders.get_folder(cmd[1].strip(),con)
    #print all duplicate files grouped
    elif function=="foldercontext":
        subresult = parse(cmd[1].strip(),con)
        if isinstance(subresult,pd.DataFrame): 
            return folders.context_folders_of(subresult,con)
        else: return subresult
    elif function=="newtable":
        table_name = cmd[1].split(maxsplit=1)[0]
        subresult = parse(cmd[1].split(maxsplit=1)[1],con)
        if isinstance(subresult,pd.DataFrame):
            subresult.to_sql(table_name, con, if_exists='replace')
            return subresult
        else: return subresult
    #generate and print dupes report
    elif function=="duplicatereport": return dupes.report(con)
    #print duplicates of a specific file
    elif function=="dupesof": 
        results = dupes.get_dupes_of(cmd[1].strip(),con)
        if len(results.index)>1:
            return results
        else: return None
    else:
        try:
            cur = con.cursor()
            cur.execute(user_in.strip())
            df = pd.read_sql_query(user_in.strip(), con)
            return df#.to_string()
        except sqlite3.Error as e:
            print(e)
            return None
            

if __name__ == "__main__":
    main()
