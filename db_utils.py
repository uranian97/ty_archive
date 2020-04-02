import os
import sqlite3
import pandas as pd
import sys
import os
import os.path as op
import glob
import sqlalchemy.types as dtypes
import size_utils as size
import dupes_helper as dupes

# create a default path to connect to and create (if necessary) a database
# called 'database.sqlite3' in the same directory as this script
#dbname = 'TaeyoonArchiveDb'
#DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'TaeyoonArchiveDb.sqlite')

#table names 
FILELIST = 'file_list'
SIZE_BY_YEAR = 'size_by_year'
DUPLICATE_REPORT = 'duplicate_report'

#fild names for the file_list table

FIELDS = {  "file_name":{'type':dtypes.Unicode()},
            "Date_Modified":{'type':dtypes.DateTime()},
            "cate_Created":{'type':dtypes.DateTime()},
            "kind":{'type':dtypes.String()},
            "size":{'type':dtypes.Float()},
            "Path":{'type':dtypes.Unicode()},
            "Parent_Folder":{'type':dtypes.Unicode()},
            "Location":{'type':dtypes.Unicode()},
            "Year":{'type':dtypes.Integer()}, 
            "Date_Added":{'type':dtypes.DateTime()}
            #"drive":{'type':dtypes.Unicode()},
            #"unit":{'type':dtypes.String()},
            #"bytes":{'type':dtypes.Float()},
            #"comments":{'type':dtypes.Unicode()},
            #"description":{'type':dtypes.Unicode()},
            #"tags":{'type':dtypes.Unicode()},
            #"version":{'type':dtypes.Float()},
            #"author":{'type':dtypes.Unicode()},
            #"creator":{'type':dtypes.Unicode()},
            #"title":{'type':dtypes.Unicode()}
            }

FILES_FORMAT={
            "file_name":"{:<40}".format,
            "kind":"{:<10}".format,
            "path":"{:<100}".format,
            "parent_folder":"{:50}".format,
            "location":"{:<60}".format,
            "year":"{:<4}".format,
}

FL_COLS = ['file_name','date_modified','date_created','kind','size','path','parent_folder','location','comments',
    'description','tags','version','pages','authors_or_artist','title','album','track_NO','genre',
    'year','duration','audio_bitrate','encoding_app','audio_sample_rate','audio_channels','dimensions',
    'width','height','total_pixels','height_DPI','width_DPI','color_space','color_profile',
    'alpha_channel','creator','video_bitrate','total_bitrate','codecs','date_added','MD5_checksum',
    'SHA256_checksum','camera_make','cam_description','camera_model_name','owner_name','serial_number',
    'copyright','software','date_taken','lens_make','lens_model','lens_serial_number','iso','fnumber',
    'focal_length','flash','orientation','latitude','longitude','maps_url']

#these are the columns we dont need to look at right now. 
# #if you want to see them in the results just delete them from this list
DROP_COLS = ['date_modified','date_created','comments','version','pages','authors_or_artist','title','album',
    'track_NO','genre','duration','audio_bitrate','encoding_app','audio_sample_rate','audio_channels','dimensions',
    'width','height','total_pixels','height_DPI','width_DPI','color_space','color_profile',
    'alpha_channel','creator','video_bitrate','total_bitrate','codecs','date_added','MD5_checksum',
    'SHA256_checksum','camera_make','cam_description','camera_model_name','owner_name','serial_number',
    'copyright','software','date_taken','lens_make','lens_model','lens_serial_number','iso','fnumber',
    'focal_length','flash','orientation','latitude','longitude','maps_url']

#these are the columns we want to see
PRINT_COLS = ['file_name','kind','size','year','location']

#connect to database of name 'db'
def connect(db):
    con = None
    try:
        dbname = f'{db}.sqlite'
        db_path = op.join(os.path.dirname(__file__), dbname)
        con = sqlite3.connect(db_path)
        return con
    except sqlite3.Error as e:
        print(e)
    return con

def make_db(con):
    print('Creating Composite filelist...')
    filelists_path = op.join(os.path.dirname(__file__), 'filelists')
    if os.path.isdir(filelists_path):
        try:
            #empty filelist if it exists, this way we can ensure no duplicates
            con.execute(f'DELETE FROM {FILELIST}')
        except sqlite3.OperationalError:
            pass
        #read the files in the filelists directory
        files = os.listdir(filelists_path)
        for file_name in files:
            if file_name.endswith('.xls') or file_name.endswith('.xlsx'):
                print('Reading: '+file_name)
                df = load_excel_filelist(file_name,con)
       
       #generate meta tables 
        print('All Filelists loaded.\n')
        print('Generating Meta Reports...')
        print(f'Generating {SIZE_BY_YEAR} report...')
        yr_report = size.all_years(con, 1980,2020)
        yr_report.to_sql(SIZE_BY_YEAR, con, if_exists='replace', index=False)
        print(yr_report.to_string(index=False))
        print(f'Generating {DUPLICATE_REPORT} report (this may take a while)...')
        dupes_report = dupes.report(con)
        dupes_report.to_sql(DUPLICATE_REPORT, con, if_exists='replace', index=False, chunksize=1000)
        print(f'\n')
        print('Table generation complete.')
        
    else:
        os.makedirs(filelists_path)
        print(f"Please add excel files to the /filelists directory to populate {FILELIST} ") 
        

def load_excel_filelist(file_name,con):
    file_path = op.join(os.path.dirname(__file__), f'filelists/{file_name}')
    file_abs_path = op.abspath(f'{file_path}')
    #read the excel file
    df = pd.read_excel(file_abs_path, names=FL_COLS)
    #change the fields
    df['bytes'] = pd.Series([size.to_bytes(x) for x in df['size']])
    df['year']=pd.Series([pd.Timestamp(x).year for x in df['date_modified']])
    df['location']=pd.Series(x.strip() for x in df['location'])
    #drop the ones we dont need 
    df.drop(columns=DROP_COLS)
    #add it to the table
    df.to_sql(FILELIST, con, if_exists='append', index=False, chunksize=1000)#, dtype={k:v['type'] for k, v in FIELDS.items()})
    return df


def load(file_name, con):
    try: 
        file_path = op.abspath(f'{file_name}')
        table_name = os.path.splitext(op.basename(file_path))
        #put in in the db in the archive table which holds all of our filelists
        #TODO specify datatypes for columns
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        if file_path.endswith('.xls') or file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        df.to_sql(table_name, con, if_exists='replace', index_label='index',chunksize=1000)
        return df
    except pd.ParseError as pe:
        return pe

def get_table(table_name, con):
    df = pd.read_sql_query(f'SELECT * FROM {table_name}',con)
    return df

def get_tables(con):
    table_sql="SELECT name FROM sqlite_master WHERE type='table'"
    return pd.read_sql_query(table_sql,con)

def get_schema(con, table_name):
    table = get_table(table_name,con)
    return table.columns
    #schema_sql = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    #return pd.read_sql_query(schema_sql,con)

