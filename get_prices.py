"""
Aureus Knights Albion Online Data Library 
Script retrieves prices from the Albion Online Data Project based on a list of items in a google sheet
those item prices are then stored in a database for further analysis
"""

import gspread
import pandas as pd
import time
import mysql.connector
from dotenv import load_dotenv
import os
import json
import requests
from datetime import datetime

# connect to db 
load_dotenv()
db = mysql.connector.connect(
  host = os.environ.get('db_host'),
  user = os.environ.get('db_user'),
  password = os.environ.get('db_pass'),
  database ="albionDB",
  auth_plugin='mysql_native_password'
)

# open up sheet
SHEET_ID = '1ALs5IvuNrql6AxYuEB4aLQFghBDJ3NGNsdzhkCxekqQ'
gc = gspread.service_account(os.environ.get('sheet_token'))
spreadsheet = gc.open_by_key(SHEET_ID)
worksheet = spreadsheet.worksheet('Manifest of Items')

df = df = pd.DataFrame(worksheet.get('A:B'))
df.columns = df.iloc[0] # setting top row as headers
df = df[1:] # ignoring type row as header

# grab list of items to fetch, and update their technical names from the database
i = 0
while i < df.shape[0]:
    if df.iloc[i,1] is None: 
        #print(df.iloc[i,0])
        sql = 'select * from items where itemName = "{}"'.format(df.iloc[i,0])
        cur.execute(sql)
        res = cur.fetchone()
        if res is not None:
            df.iloc[i,1] = res['itemTechnicalName']
            #print(res["itemTechnicalName"])
    i+=1

# write back technical names to sheet
worksheet.update([df.columns.values.tolist()] + df.values.tolist())

# for future: maybe write the list of scraped items to the db?

# fetch prices
cur = db.cursor(dictionary=True)

# for each item we write results to database and add to the dataframe
for i in range(len(df)):
    #print(df.iloc[i,1])
    # https://www.albion-online-data.com/api/v2/stats/prices/T4_BAG,T5_BAG.json?locations=Caerleon,Bridgewatch&qualities=2
    if df.iloc[i,1] is not None:
        url = 'https://west.albion-online-data.com/api/v2/stats/prices/{}?qualities=0,1,2,3,4.json'.format(df.iloc[i,1])
        print('Getting price for: '+url)
        response = requests.get(url)
        data = json.loads(response.text)
        # only pull the price if we haven't already pulled it this run
        for x in data:
            # add to database
            if x['sell_price_min'] != 0 and x['buy_price_max'] != 0:
                sql = "INSERT INTO prices (itemTechnicalName, city, quality, sell_price, buy_price, sell_price_min, sell_price_max, buy_price_min, buy_price_max) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = (x['item_id'], x['city'], x['quality'], x['sell_price_min'], x['buy_price_max'], x['sell_price_min'], x['sell_price_max'], x['buy_price_min'], x['buy_price_max'])
                cur.execute(sql, values)
        time.sleep(.1)
# write changes to database
db.commit()