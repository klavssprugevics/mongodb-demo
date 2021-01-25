# Klavs Sprugevics - ks20064
# Python un MongoDB

import json
from pymongo import MongoClient
from bson.dbref import DBRef
import random
from datetime import datetime, timedelta

# key
#client = MongoClient("")

db = client["MD3"]
active_col = db["Users"]
active_col.remove()

with open('.//data//users.json') as file:
    file_data = json.load(file)
    active_col.insert_many(file_data)
    file.close()

active_col = db["Categories"]
active_col.remove()

with open('.//data//categories.json') as file:
    file_data = json.load(file)
    active_col.insert_many(file_data)
    file.close()

active_col = db["Attributes"]
active_col.remove()

with open('.//data//attributes.json') as file:
    file_data = json.load(file)
    active_col.insert_many(file_data)
    file.close()


active_col = db["Items"]
active_col.remove()

with open('.//data//items.json') as file:
    file_data = json.load(file)
    active_col.insert_many(file_data)
    file.close()


all_items = db["Items"].find()

# Katram item pieskir random attributes un cenu
for item in all_items:

    attributes = []

    # color
    attributes.append(random.randint(1, 6))
    # creation year
    attributes.append(random.randint(7, 17))

    # body material
    if item["categories"] == 1 or item["categories"] == 3:
        attributes.append(random.randint(18, 20))

    # key count
    if item["categories"] == 3:
        attributes.append(random.randint(21, 23))

    # metronome max bpm
    if item["categories"] == 4:
        attributes.append(random.randint(24, 26))

    # drum count
    if item["categories"] == 2:
        attributes.append(random.randint(27, 29))

    db["Items"].update({"_id":item["_id"]}, {"$set":{"attributes":attributes}})
    db["Items"].update({"_id":item["_id"]}, {"$set":{"price":round(random.uniform(5,100), 2)}})

    # nejausi nosaka, vai precei bus atlaide
    if bool(random.getrandbits(1)):
        db["Items"].update({"_id":item["_id"]}, {"$set":{"discount":round(random.uniform(0.05, 0.5), 2)}})


# Izveido iepirkuma grozu un taja ievieto preces
active_col = db["Carts"]
active_col.remove()

with open('.//data//carts.json') as file:
    file_data = json.load(file)
    active_col.insert_many(file_data)
    file.close()

all_carts = db["Carts"].find()

# Atrod item ids
item_list = []
items = db.Items.find()
for item in items:
    item_list.append(item["_id"])

for cart in all_carts:

    # Nejausi izvelas precu skaitu groza
    item_count = random.randint(1, 5)
    db["Carts"].update({"_id":cart["_id"]}, {"$set":{"items":random.sample(item_list, item_count)}})

    purchased = bool(random.getrandbits(1))
    db["Carts"].update({"_id":cart["_id"]}, {"$set":{"purchased":purchased}})

    if purchased:
        date = datetime.now() - timedelta(days=random.randint(1, 999))
        db["Carts"].update({"_id":cart["_id"]}, {"$set":{"date_of_purchase":date}})

