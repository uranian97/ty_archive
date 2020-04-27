import sqlite3
import pandas as pd
import json
import sys
import os
import os.path as op
import sqlalchemy.types as dtypes
import size_utils as size
import dupes_helper as dupes
import project_helper as proj
import file_helper as folders
import data_utils as data

# create a default path to connect to and create (if necessary) a database
# called 'database.sqlite3' in the same directory as this script
#dbname = 'TaeyoonArchiveDb'
#DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'TaeyoonArchiveDb.sqlite')

#global info for this database
NAME = None
CON = None

#table names 
FILELIST = 'file_list'
SIZE_BY_YEAR = 'size_by_year'
DUPLICATE_REPORT = 'duplicate_report'

#these are the columns we want to see when we print
PRINT_COLS = ['file_name','kind','bytes','year','parent_folder']
#these are the columns we want to keep during import. add columns to this to keep them
All_COLS = ['file_name','kind','size','year','location','parent_folder','path','bytes']
 #list of columns in standard file list sheets and tables. dont change this
FL_COLS = ['file_name','date_modified','date_created','kind','size','path','parent_folder','location','comments',
        'description','tags','version','pages','authors_or_artist','title','album','track_NO','genre',
        'year','duration','audio_bitrate','encoding_app','audio_sample_rate','audio_channels','dimensions',
        'width','height','total_pixels','height_DPI','width_DPI','color_space','color_profile',
        'alpha_channel','creator','video_bitrate','total_bitrate','codecs','date_added','MD5_checksum',
        'SHA256_checksum','camera_make','cam_description','camera_model_name','owner_name','serial_number',
        'copyright','software','date_taken','lens_make','lens_model','lens_serial_number','iso','fnumber',
        'focal_length','flash','orientation','latitude','longitude','maps_url']

filelists_path = op.abspath(op.join(os.path.dirname(__file__), 'filelists'))

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

#populate the database
def make_db():
    print('Creating Composite filelist...')
    #start reading filelists
    if os.path.isdir(filelists_path):
        try:
            #empty filelist if it exists, this way we can ensure no duplicates
            #this is fine because we shouldnt be altering the filelist, only extracting data and creating new tables with it
            CON.execute(f'DELETE FROM {FILELIST}')
        except sqlite3.OperationalError:
            pass
        #keep track of the drives for each sheet
        sheets = pd.DataFrame(columns=FL_COLS)
        drives = []
        drop = [col for col in FL_COLS if All_COLS.count(col)<1]
        #read each files in the filelists directory
        for file_name in os.listdir(filelists_path):
            if file_name.endswith('.xls') or file_name.endswith('.xlsx'):
                print('Reading: '+file_name)
                df = load_excel_filelist(op.join(filelists_path, f'{file_name}'))
                #get the drive info
                drives.extend(get_drives(df['location'].values.tolist()))
                #add sheet to the table
                sheets.append(df, ignore_index=True)
                #df.to_sql(FILELIST, CON, if_exists='append', index=False, chunksize=1000) 
        #add drives to the table
        drives_df = pd.DataFrame.from_records(drives)
        drives_df.drop_duplicates(inplace=True)
        sheets.append(drives)
        #cols we dont need 
        sheets.drop(columns=drop,inplace=True)
        sheets.to_sql(FILELIST, CON, if_exists='append', index=False)
        print('All Filelists loaded.\n')
        #make all analysis tables
        make_metatables()
        #do the under construction loading tasks
        #this is here so i can test them without reloading the whole db each time
        update()
        #print information about this databae
        info()
    else:
        os.makedirs(filelists_path)
        print(f"Please add excel files to the /filelists directory to populate {FILELIST} ") 

def update():
    data.generate_json_data()
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

#loads a filelist from an excel sheet
def load_excel_filelist(file_path):
    #read the excel file
    df = pd.read_excel(file_path, names=FL_COLS,converters={'file_name':str,'location':str,'path':str,'parent_folder':str,'year':int,'kind':str})
    #change the fields
    df['bytes'] = pd.Series([size.to_bytes(x) for x in df['size']])
    df['year']=pd.Series([pd.Timestamp(x).year for x in df['date_modified']])
    df['location']=pd.Series([str(x).strip() for x in df['location']])
    df['path']=pd.Series([str(x).strip() for x in df['path']])
    df['file_name']=pd.Series([str(x).strip() for x in df['file_name']])
    df['parent_folder']=pd.Series([str(x).strip() for x in df['parent_folder']])
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

#display info about this database
def info():
    print('About this database:')
    print('file_list : a table of file metadata from multiple drives.')
    print('file_list Schema:')
    print(get_schema(FILELIST))
    print('')
    print('All Tables:')
    print(get_tables().to_string(index=False))
    print('')
    print('type getschema <table_name> to see the columns of specific table.')
            
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

#get the parent drive name and volume from a list of locations
def get_drives(locations):
    #create entries for drive and volumes
    drives = []
    first_loc = locations[0]
    location = first_loc.split('/')
    vol = location[1]
    drive = location[2]

    vol_file = {
        'file_name':vol,
        'path':"/".join(["",vol]),
        'location': "".join(["/",vol]),
        'parent_folder': "".join(["/", data.HEAD]),
        'kind':'Volume',
        'size': 'Zero bytes',
    }
    drive_file = {
        'file_name':drive,
        'path':"/".join(['',vol,drive]),
        'location':"/".join(['',vol,drive]),
        'parent_folder':vol,
        'kind':'Drive',
        'size':'Zero bytes',
    }
    drives.append(vol_file)
    drives.append(drive_file)
    return drives

