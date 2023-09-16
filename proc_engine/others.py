import pymongo
import sqlite3
import os
from . import __base__


path_to_db = "C:/Users/Animesh/Desktop/Projects/gtoprocessing/scripts/pkrProcessing/ranges.db"


def transform_func(item):
    return {
        "cards": item[0],
        "pairing": item[1],
        "suiting": item[2],
        "category": item[3]
    }


if os.path.isfile(path_to_db):
    db = sqlite3.connect(path_to_db)
    cursor = db.cursor()
    cursor.execute("""select * from ranges;""")
    res = cursor.fetchall()
    db.close()
    res = list(map(transform_func, res))
    mongoconn = pymongo.MongoClient(
        "mongodb://animesh:mongo123@gtoplus.gtoinspector.poker:8179")
    rangesdb = mongoconn["ranges"]["card_ranges"]
    rangesdb.insert_many(res)
    print("Committed!")
else:
    print("Does not exist")
