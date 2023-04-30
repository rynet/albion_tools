import gspread
import pandas as pd
import time
import mysql.connector
from dotenv import load_dotenv
import os
import json

# connect to db 
load_dotenv()
db = mysql.connector.connect(
  host = os.environ.get('db_host'),
  user = os.environ.get('db_user'),
  password = os.environ.get('db_pass'),
  database ="albionDB"
)
cur = db.cursor(dictionary=True)

# open up sheet
SHEET_ID = '1ALs5IvuNrql6AxYuEB4aLQFghBDJ3NGNsdzhkCxekqQ'
SHEET_NAME = 'Master Price Sheet'
gc = gspread.service_account('e:/aarow-and-co-2e8195ad989c.json')
spreadsheet = gc.open_by_key(SHEET_ID)
worksheet = spreadsheet.worksheet(SHEET_NAME)

df = df = pd.DataFrame(worksheet.get('A:B'))
df.columns = df.iloc[1] # setting top row as headers
df = df[2:] # ignoring type row as header

# grab list of items to fetch
i = 0
while i < df.shape[0]:
    if df.iloc[i,1] is None: 
        #print(df.iloc[i,0])
        sql = 'select * from items where itemName = "{}"'.format(df.iloc[i,0])
        cur.execute(sql)
        res = cur.fetchall()
        #df.iloc[i,1] = res['itemTechnicalName']
        #print(res["itemTechnicalName"])
        print(type(res))

    i+=1

# get technical names of items 

# write back technical names

# fetch prices

# write results to database

# write results to sheet