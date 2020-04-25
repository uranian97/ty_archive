import sqlite3
import os
import os.path as op
import pandas as pd
import db_utils as db
import file_helper as folders
import re

#project table name
WEB_LIST = 'web_list'
CHAT_LIST = 'CHAT_list'

def make_project_lists():
    print('Reading Teayoon@CHAT sheet')
    CHAT_path = op.abspath(op.join(os.path.dirname(__file__), f'projectlists/taeyoon_at_CHAT.xlsx'))
    CHAT_cols = ['exhibit','artist','type','title','medium','components','condition','digital','confirmed','dimension','year','ownership','location','context_or_series','images','video']
    #dataframe from Taeyoon @ Chat sheet
    chat = pd.read_excel(CHAT_path, names=CHAT_cols,usecols="A:P",skiprows=11,nrows=50, true_values=["Y"], false_values=["N"])
    chat.dropna(axis=0,how='all', inplace=True)

    chat['collab'] = pd.Series([get_collab(x) for x in chat['artist']])
    chat['artist'] = pd.Series([get_artist(x) for x in chat['artist']])


    

    print(chat.to_string(index=False, columns=['exhibit','artist','type','title','medium','components','digital','year']))

    print('Reading Taeyoonchoi.com sheet')
    web_path = op.abspath(op.join(os.path.dirname(__file__), f'projectlists/taeyoonchoi_com.xlsx'))
    web_cols = ['project_name', 'work', 'document', 'doc_short','hierarchy','status','year','tags','collaborator','venue','finish','url','new_url']
    sheet_names = ['Urban Prog','Soft Care', 'Poetic Comp','Your Friend','Camerauto','Presence','Speakers','ISOPT']
    
    #dict of sheet dfs and a master df for the taeyoonchoi.com sheet
    proj_lists = pd.read_excel(web_path, names=web_cols, sheet_name=sheet_names, usecols="A:M")

    site_list = pd.concat(list(proj_lists.values()))
    site_list.dropna(axis=0,how='all', inplace=True)
    print(site_list.to_string(index=False, columns=['project_name', 'work', 'document', 'doc_short','year']))


    chat.to_sql(CHAT_LIST, db.CON, if_exists='replace')
    site_list.to_sql(WEB_LIST, db.CON, if_exists='replace')

    #alter + combine these lists to for the project list and the documentation list
    #get projects and artworks
        #any entry w/empty work and doc in .com

    #get documentations

def get_collab(artist):
    artists = re.split(', | and ', artist)
    

def get_artist(artist):
    artists = re.split(', | and ', artist)
    if 'Taeyoon Choi' not in artists:
        return artist
    else:
        return 'Taeyoon Choi'

def list_projects():
    df = pd.read_sql_query(f'SELECT project_name, work FROM {WEB_LIST}',db.CON)
    project_names = df.drop_duplicates().drop_duplicates(subset="work")

    return project_names

def project_info(proj_name):
    proj_sql = f'SELECT * FROM {WEB_LIST} WHERE project_name LIKE %{proj_name}%'
    df = pd.read_sql_query(proj_sql, db.CON).drop_duplicates
    return df

def work_info(work_name):
    work_sql = f'SELECT * FROM {WEB_LIST} WHERE work LIKE %{work_name}%'
    df = pd.read_sql_query(work_sql, db.CON).drop_duplicates
    return df

#def find_related_files(document=None, project_name=None,work=None, collaborator, ):
    #get project entries related to term
 #   doc_sql = f"SELECT * FROM WEB_list"
  #  doc_args = []
   # if document is not None: doc_args.append("document LIKE '%{document}%'")
    #if project_name is not None: doc_args.append(f"project_name LIKE '%{project_name}%'")
    #if work is not None: doc_args.append(f"work LIKE '%{work}%'")
    #arg = " OR ".join(doc_args)

    #file_sql = f"SELECT {', '.join(db.PRINT_COLS)} FROM file_list"

#def related_files(project_df):
    #entries = {}
    #file_sql = f"SELECT {', '.join(db.PRINT_COLS)} FROM file_list"

    #for i, item in project_df.head().iterrows()
        #conditions=[]

        

        #entries.update(item, files)



    #get project entries related to term
    #file_sql = f"SELECT {', '.join(db.PRINT_COLS)} FROM file_list"
    #doc_args = []
    #if document is not None: doc_args.append("document LIKE '%{document}%'")
    #if project_name is not None: doc_args.append(f"project_name LIKE '%{project_name}%'")
    #if work is not None: doc_args.append(f"work LIKE '%{work}%'")
    #arg = " OR ".join(doc_args)

    #file_sql = f"SELECT {', '.join(db.PRINT_COLS)} FROM file_list"

        

#def find_related_files(project_name, document_name):
    #look at entry for projject or document
    #get all related to project
    #look at tags/ other info to ascertain what Kind of file
    #get things w name, file type, etc that may relate to document
    #prompt user to give some filetypes? 


    #project adress: projectname/work/document

#def sort_files():
    #make tables for every project and sort files into them one by one
