"""
Aureus Knights Albion Online Data Library 
This script uses data available in our database that comes from the Albion Online Data Project
to figure out the current pricing for items in Albion Online
This scripts creates the table "bestAvailablePrice" which uses a range of data from the AODP and creates a single version of the truth using the following logic:
For both buy and sell price, we try and find the most recent price that was successfully scanned by the AODP that is stored in our data library 
We then update the relevant database table and write the output to our google sheet
"""

import gspread
import pandas as pd
import time
import mysql.connector
from dotenv import load_dotenv
import os
import json
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

cur = db.cursor(dictionary=True)

# regenerate current price database
## create a temp table to fill up
print("Writing new best available price database...")
sql = """create table if not exists new_bestAvailablePrice (
	itemTechnicalName varchar(256),
    itemName varchar(256),
    city varchar(128),
    quality int,
    sell_price int,
    buy_price int,
    avg_sell_price int,
    avg_buy_price int,
    sell_price_age datetime,
    buy_price_age datetime
)"""
cur.execute(sql)
db.commit()

## get item manifest

cur.execute("select * from vw_itemManifest")
result = cur.fetchall()

## update price entries for manifest items
for x in result:
    sql = "SELECT (select sell_price from albionDB.prices p where p.itemTechnicalName = %s and p.city = %s and p.quality = %s and p.sell_price <> 0 and pricecreated >= date_add(curdate(),interval -7 day)  order by pricecreated desc limit 1 ) as sell_price, (select buy_price from albionDB.prices p where p.itemTechnicalName = %s and p.city = %s and p.quality = %s and p.buy_price <> 0 and pricecreated >= date_add(curdate(),interval -7 day) order by pricecreated desc limit 1 ) as buy_price , (select avg(sell_price) from albionDB.prices p where p.itemTechnicalName = %s and p.city = %s and p.quality = %s and p.sell_price <> 0 and pricecreated >= date_add(curdate(),interval -7 day)) as avg_sell_price, (select avg(buy_price) from albionDB.prices p where p.itemTechnicalName = %s and p.city = %s and p.quality = %s and p.buy_price <> 0 and pricecreated >= date_add(curdate(),interval -7 day)) as avg_buy_price, (select pricecreated from albionDB.prices p where p.itemTechnicalName = %s and p.city = %s and p.quality = %s and p.sell_price <> 0 and pricecreated >= date_add(curdate(),interval -7 day) order by pricecreated desc limit 1) as sell_price_age, (select pricecreated from albionDB.prices p where p.itemTechnicalName = %s and p.city = %s and p.quality = %s and p.buy_price <> 0 and pricecreated >= date_add(curdate(),interval -7 day) order by pricecreated desc limit 1) as buy_price_age"
    val = (x['itemTechnicalName'], x['city'], x['quality'], x['itemTechnicalName'], x['city'], x['quality'], x['itemTechnicalName'], x['city'], x['quality'], x['itemTechnicalName'], x['city'], x['quality'], x['itemTechnicalName'], x['city'], x['quality'], x['itemTechnicalName'], x['city'], x['quality'])
    cur.execute(sql, val)
    res2 = cur.fetchall()
    sql = "INSERT INTO new_bestAvailablePrice values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (x['itemTechnicalName'], x['itemName'], x['city'], x['quality'], res2[0]['sell_price'], res2[0]['buy_price'], res2[0]['avg_sell_price'], res2[0]['avg_buy_price'], res2[0]['sell_price_age'], res2[0]['buy_price_age'])
    cur.execute(sql, val)
db.commit()

## drop the old table
print("Replacing old best available price table...")
cur.execute("drop table bestAvailablePrice")
db.commit()
cur.execute("rename table new_bestAvailablePrice to bestAvailablePrice")
db.commit()

# write current price database to spreadsheet 
## make a new data frame for the output format
print("Creating new best price worksheet")
outSheet = spreadsheet.worksheet('Master Price Sheet')
outDF = pd.DataFrame(outSheet.get('A:K'))
outDF.columns = outDF.iloc[0] # setting top row as headers
outDF = outDF[1:] # ignoring type row as header
outDF = outDF[0:0] # empty the existing table
outSheet.clear() # clear worksheet

## write the current best prices to the outsheet

cur.execute("select * from bestAvailablePrice")
result = cur.fetchall()

for x in result:
    outStr = str(x['itemName'])+str(x['city'])+str(x['quality'])
    outDF.loc[len(outDF.index)] = [outStr, x['itemName'], x['itemTechnicalName'], x['quality'], x['city'], str(x['sell_price']), str(x['buy_price']), str(x['avg_sell_price']), str(x['avg_buy_price']), str(x['sell_price_age']), str(x['buy_price_age'])]

# write final dataframe back to sheet
print("Saving worksheet...")
outSheet.update([outDF.columns.values.tolist()] + outDF.values.tolist())