# filelist.py
filelist.py is a Python3 and SQLite3 script for working on a database of filelists

#dependencies: 
python3
pandas
xlrd
sqlalchemy

#filelists
These are .xls or .xlsx files output from the File List Export application. 
To load a filelist, add the excel file to the /filelists folder
When you open a database, type 'init' to load the filelists to the Filelist table and (re)generate the metatables: size_by_year and duplicate_report


run the filelist.py program:
	python filelist.py <databasename>

and type 'help' for a list of custom commands, or write your own sql query

