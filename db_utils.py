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

FILELIST = 'Filelist'
SIZE_BY_YEAR = 'Size_By_Year'
DUPLICATE_REPORT = 'Duplicate_Report'





FIELDS = {  "File_Name":{'type':dtypes.Unicode()},
            "Date_Modified":{'type':dtypes.DateTime()},
            "Date_Created":{'type':dtypes.DateTime()},
            "Kind":{'type':dtypes.String()},
            "Size":{'type':dtypes.Float()},
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

XL_FIELDS = ['File_Name',
    'Date_Modified',
    'Date_Created',
    'Kind',
    'Size',
    'Path',
    'Parent_Folder',
    'Location',
    'Comments',
    'Description',
    'Tags',
    'Version',
    'Pages',
    'Authors_or_Artist',
    'Title',
    'Album',
    'Track_NO',
    'Genre',
    'Year',
    'Duration',
    'Audio_BitRate',
    'Encoding_App',
    'Audio_Sample_Rate',
    'Audio_Channels',
    'Dimensions',
    'Width',
    'Height',
    'Total_Pixels',
    'Height_DPI',
    'Width_DPI',
    'Color_Space',
    'Color_Profile',
    'Alpha_Channel',
    'Creator',
    'Video_BitRate',	
    'Total_BitRate',
    'Codecs',
    'Date_Added',
    'MD5_Checksum',
    'SHA256_Checksum',
    'Camera_Make',
    'cam_Description',
    'Camera_Model_Name',
    'Owner_Name',
    'Serial_Number',
    'Copyright',
    'Software',
    'Date_Taken',
    'Lens_Make',
    'Lens_Model',
    'Lens_Serial_Number',
    'ISO',
    'FNumber',
    'Focal_Length',
    'Flash',
    'Orientation',
    'Latitude',
    'Longitude',
    'Maps_URL']

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
            #empty filelist if it exists
            con.execute(f'DELETE FROM {FILELIST}')
        except sqlite3.OperationalError:
            pass
        
        #csvs = [x for x in os.listdir(filelists_path) if x.endswith(".csv")]
        files = os.listdir(filelists_path)
        for file_name in files:
            file_path = op.join(os.path.dirname(__file__), f'filelists/{file_name}')
            print('Reading: '+file_path)
            df = load_excel_filelist(file_path,con)
            df.to_sql(FILELIST, con, if_exists='append', index_label='index')#, dtype={k:v['type'] for k, v in FIELDS.items()})
        #generate meta tables
        
        print('All Filelists loaded.\n')

        print('Generating Meta Reports...')
        print(f'Generating {SIZE_BY_YEAR} report...')
        yr_report = size.all_years(con, 1980,2020)
        yr_report.to_sql(SIZE_BY_YEAR,con,if_exists='replace',index=False)
        print(f'Generating {DUPLICATE_REPORT} report (this may take a while)...')
        dupes_report = dupes.report(con)
        dupes_report.to_sql(DUPLICATE_REPORT, con, if_exists='replace', index=False)
        print(f'\n')
        print('Table generation complete.')
        
    else:
        os.makedirs(filelists_path)
        print(f"Please add .csv files to the /filelists directory to populate {FILELIST} ") 
        
def load_excel_filelist(file_name,con):
    file_path = op.abspath(f'{file_name}')
    df = pd.read_excel(file_path, names=XL_FIELDS)
    df['Bytes'] = pd.Series([size.to_bytes(x) for x in df['Size']])
    df['Year']=pd.Series([pd.Timestamp(x).year for x in df['Date_Modified']])
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
        df.to_sql(table_name, con, if_exists='replace', index_label='index')
        return pd.read_sql_table(table_name, con)
    except:
        return None

def get_tables(con):
    table_sql="SELECT name FROM sqlite_master WHERE type='table'"
    return pd.read_sql_query(table_sql,con)

def get_schema(con, table_name):
    table = pd.read_sql_table(table_name,con)
    return table.columns
    #schema_sql = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    #return pd.read_sql_query(schema_sql,con)

