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
  database ="albionDB"
)
cur = db.cursor(dictionary=True)
#cur = db.cursor()

# open up sheet
SHEET_ID = '1ALs5IvuNrql6AxYuEB4aLQFghBDJ3NGNsdzhkCxekqQ'
gc = gspread.service_account('e:/aarow-and-co-2e8195ad989c.json')
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
## make a new data frame for the output format
outSheet = spreadsheet.worksheet('Master Price Sheet')
outDF = pd.DataFrame(outSheet.get('A:G'))
outDF.columns = outDF.iloc[0] # setting top row as headers
outDF = outDF[1:] # ignoring type row as header
outDF = outDF[0:0] # empty the existing table

# for each item we write results to database and add to the dataframe
for i in range(len(df)):
    #print(df.iloc[i,1])
    # https://www.albion-online-data.com/api/v2/stats/prices/T4_BAG,T5_BAG.json?locations=Caerleon,Bridgewatch&qualities=2
    if df.iloc[i,1] is not None:
        url = 'https://www.albion-online-data.com/api/v2/stats/prices/{}.json'.format(df.iloc[i,1])
        print('Getting price for: '+url)
        response = requests.get(url)
        data = json.loads(response.text)
        # only pull the price if we haven't already pulled it this run
        for x in data:
            # add to database
            sql = "INSERT INTO prices (itemTechnicalName, city, quality, sell_price, buy_price, sell_price_min, sell_price_max, buy_price_min, buy_price_max, pricecreated) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (x['item_id'], x['city'], x['quality'], x['sell_price_min'], x['buy_price_max'], x['sell_price_min'], x['sell_price_max'], x['buy_price_min'], x['buy_price_max'], datetime.now())
            cur.execute(sql, values)
            # add to sheet
            outStr = str(df.iloc[i,0])+str(x['city'])+str(x['quality'])
            outDF.loc[len(outDF.index)] = [outStr, df.iloc[i,0], df.iloc[i,1], x['quality'], x['city'], x['sell_price_min'], x['buy_price_max']]
            time.sleep(0.25)

db.commit() # write all those entries to the database
# write final dataframe back to sheet
outSheet.update([outDF.columns.values.tolist()] + outDF.values.tolist())