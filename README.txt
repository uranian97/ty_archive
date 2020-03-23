preparing a spreadsheet for insertion into this database:

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


export the spreadsheet as a csv
run the filelist.py program 
	python filelist.py <databasename>
and use the 'load' command to insert it into the filelist. 
