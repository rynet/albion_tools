import json
import mysql.connector
from dotenv import load_dotenv
import os
import time

# open file
f = open('e:\items.json', encoding='UTF-8')
data = json.load(f)

load_dotenv()

db = mysql.connector.connect(
  host = os.environ.get('db_host'),
  user = os.environ.get('db_user'),
  password = os.environ.get('db_pass'),
  database ="albionDB"
)
cur = db.cursor()

# clear db first
cur.execute("delete from items")
db.commit()

# reload data
for x in data:
    if x["LocalizedDescriptions"] is not None:
        #tmpDesc = None
        tmpDesc = x["LocalizedDescriptions"]["EN-US"]
    else:
        #tmpDesc = x["LocalizedDescriptions"]["EN-US"]
        tmpDesc = None

    if x["LocalizedNames"] is not None:
        tmpName = x["LocalizedNames"]["EN-US"]
    else: 
        tmpName = None

    sql = "INSERT INTO items (itemid, itemName, itemDescription, itemTechnicalName) values (%s, %s, %s, %s)"
    values = (x["Index"], tmpName , tmpDesc ,x["UniqueName"])
    cur.execute(sql, values)

    #print(x["LocalizedNames"]["EN-US"])
    #print(x["LocalizedDescriptions"]["EN-US"]) # nullable 
    #print(x["Index"])
    print(x["UniqueName"])
    time.sleep(0.1)

db.commit()