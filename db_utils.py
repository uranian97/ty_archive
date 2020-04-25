import sqlite3
import pandas as pd
import sys
import os
import os.path as op
import sqlalchemy.types as dtypes
import size_utils as size
import dupes_helper as dupes
import project_helper as proj
# create a default path to connect to and create (if necessary) a database
# called 'database.sqlite3' in the same directory as this script
#dbname = 'TaeyoonArchiveDb'
#DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'TaeyoonArchiveDb.sqlite')

NAME = None
CON = None


#table names 
FILELIST = 'file_list'
SIZE_BY_YEAR = 'size_by_year'
DUPLICATE_REPORT = 'duplicate_report'

#fild names for the file_list table

FILES_FORMAT={
            "file_name":"{:<40}".format,
            "kind":"{:<10}".format,
            "path":"{:<100}".format,
            "parent_folder":"{:50}".format,
            "location":"{:<60}".format,
            "year":"{:<4}".format,
}

#these are the columns we want to see when we print
PRINT_COLS = ['file_name','kind','size','year','location']

#connect to database of name 'db'
def connect(db):
    #set globals for whole session
    global NAME
    global CON
    try:
        NAME = f'{db}.sqlite'
        db_path = op.join(os.path.dirname(__file__), NAME)
        CON = sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(e)

def make_db():
    print('Creating Composite filelist...')
    filelists_path = op.join(os.path.dirname(__file__), 'filelists')
    if os.path.isdir(filelists_path):
        try:
            #empty filelist if it exists, this way we can ensure no duplicates
            #this is fine because we shouldnt be altering the filelist, only extracting data and creating new tables with it
            CON.execute(f'DELETE FROM {FILELIST}')
        except sqlite3.OperationalError:
            pass
        #read the files in the filelists directory
        files = os.listdir(filelists_path)
        for file_name in files:
            if file_name.endswith('.xls') or file_name.endswith('.xlsx'):
                print('Reading: '+file_name)
                df = load_excel_filelist(file_name)
                #add it to the table
                df.to_sql(FILELIST, CON, if_exists='append', index=False, chunksize=1000) 
        print('All Filelists loaded.\n')
        update()
        make_metatables()
        info()
    else:
        os.makedirs(filelists_path)
        print(f"Please add excel files to the /filelists directory to populate {FILELIST} ") 

def update():
    print('Loading project table')
    proj.make_project_lists()
    print('Projects loaded.')
    


#generate meta tables
def make_metatables():
    print('Generating Meta Reports...')
    print(f'Generating {SIZE_BY_YEAR} report...')
    yr_report = size.all_years(1980,2020)
    yr_report.to_sql(SIZE_BY_YEAR, CON, if_exists='replace', index=False)
    print(yr_report.to_string(index=False))
    print(f'Generating {DUPLICATE_REPORT} report (this may take a while)...')
    dupes_report = dupes.report()
    dupes_report.to_sql(DUPLICATE_REPORT, CON, if_exists='replace', index=False, chunksize=1000)
    print(f'\n')
    print('Table generation complete.')

#display info about this database
def info():
    about_txt = f"""About this database:
        file_list : a table of file metadata from multiple drives.
        file_list Schema:
        {get_schema(FILELIST)}\n

        All Tables:
        {get_tables().to_string(index=False)}\n

        type getschema <table_name> to see a specific tables columns.
        """
    print(about_txt)
            
def load_excel_filelist(file_name):
    #list of columns in standard file list sheets and tables. dont change this
    FL_COLS = ['file_name','date_modified','date_created','kind','size','path','parent_folder','location','comments',
        'description','tags','version','pages','authors_or_artist','title','album','track_NO','genre',
        'year','duration','audio_bitrate','encoding_app','audio_sample_rate','audio_channels','dimensions',
        'width','height','total_pixels','height_DPI','width_DPI','color_space','color_profile',
        'alpha_channel','creator','video_bitrate','total_bitrate','codecs','date_added','MD5_checksum',
        'SHA256_checksum','camera_make','cam_description','camera_model_name','owner_name','serial_number',
        'copyright','software','date_taken','lens_make','lens_model','lens_serial_number','iso','fnumber',
        'focal_length','flash','orientation','latitude','longitude','maps_url']

    file_path = op.abspath(op.join(os.path.dirname(__file__), f'filelists/{file_name}'))
    #read the excel file
    df = pd.read_excel(file_path, names=FL_COLS)
    #change the fields
    df['bytes'] = pd.Series([size.to_bytes(x) for x in df['size']])
    df['year']=pd.Series([pd.Timestamp(x).year for x in df['date_modified']])
    df['location']=pd.Series(x.strip() for x in df['location'])
    #drop the ones we dont need 
    return df

#export specified table as excel file
def export_excel(table_name):
    #convert the table Dataframe
    df = get_table(table_name)
    #make the path of the file we are creating (exported sheets will be placed in the 'lists' directory)
    path = op.abspath(op.join(os.path.dirname(__file__), f'lists/{table_name}_export.xlsx'))
    #convert dataframe to excel
    df.to_excel(path, table_name, index=False)
    print(f"Export complete, check the /lists folder for {table_name}_export.xlsx")

#currently i am not using this 
def load(file_name):
    try: 
        file_path = op.abspath(f'{file_name}')
        table_name = os.path.splitext(op.basename(file_path))
        #put in in the db in the archive table which holds all of our filelists
        #TODO specify datatypes for columns
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.xls') or file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        df.to_sql(table_name, CON, if_exists='replace', index_label='index',chunksize=1000)
        return df
    except pd.ParseError as pe:
        return pe

#returns a dataframe with the contents of the given table
def get_table(table_name):
    df = pd.read_sql_query(f'SELECT * FROM {table_name}',CON)
    return df

#returns a dataframe of all tables in this database
def get_tables():
    table_sql="SELECT name FROM sqlite_master WHERE type='table'"
    return pd.read_sql_query(table_sql,CON)

#eturns a dataframe with the schema for the given table 
def get_schema(table_name):
    table = get_table(table_name)
    return table.columns
    #schema_sql = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    #return pd.read_sql_query(schema_sql,con)

