
functions so far and how to use them:

load a csv of files into the files table:
python ty_archive.py load <file_name> [<file_name>...]

get the size of the data for a certain year
python ty_archive.py sizeof <year>

get size of all files by year:
python ty_archive.py size_report

get contents of a folder
python ty_archive.py folder <path\to\folder>

get duplicates of a file
python ty_archive.py dupesof <file_name>

get duplicate report:
python ty_archive.py duplicate_report


preparing a spreadsheet to add to the database:
-Extract year from 'date modified' as usual
Make bytes column and calculate (
Replace “Zero” with 0 in the size column
Add ‘drive’ column after "date_added," which contains name of the drive

the columns should be as follows:
file_name
date_modified
date_created
kind
size
unit
bytes
path
parent_folder
location
comments
description
tags
version
author
title
year
creator
date_added
drive 
