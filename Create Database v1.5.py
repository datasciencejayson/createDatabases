# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 13:55:06 2018

@author: backesj
"""

# import packages
# we need pandas and sqlalchemy to create the indexed databases that we will use

import pandas as pd
from sqlalchemy import create_engine # database connection
import sqlite3
import pandas.io.sql as pd_sql
import sqlite3 as sql

# create user defined function to import csv database

def chunkImport(file, sourceLocation, dbname, tableName, chunksize=10000, inputType=1, outputLocation=''):
    if outputLocation == '':
        beneDatabase = create_engine('sqlite:///{}\\{}.db'.format(sourceLocation, dbname))
    else:
        beneDatabase = create_engine('sqlite:///{}\\{}.db'.format(outputLocation, dbname))
    i = 0
    j = 1
    if inputType == 1:
        for df in pd.read_csv('{}/{}'.format(sourceLocation, file), chunksize=chunksize, iterator=True,  dtype={'recip_ssn': str, 'RECIP_ZIP_CD': str, 'ZIP5': str}):
              df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
              df.index += j
              i+=1
              df.to_sql(tableName,  beneDatabase, if_exists='append' , index=False)
              j = df.index[-1] + 1
        return file, sourceLocation, dbname, tableName, chunksize, inputType, outputLocation  
    else: 
        for df in pd.read_csv('{}/{}'.format(sourceLocation, file), chunksize=chunksize, iterator=True):
              df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
              df.index += j
              i+=1
              df.to_sql(tableName,  beneDatabase, if_exists='append' , index=False)
              j = df.index[-1] + 1
        return file, sourceLocation, dbname, tableName, chunksize, inputType, outputLocation
        
file, sourceLocation, dbname, tableName, chunksize, inputType, outputLocation = \
chunkImport('smallBeneList.csv', 'F:\\backesj\\', 'tempBeneDatabase', 'beneTable')

