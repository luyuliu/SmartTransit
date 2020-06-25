
from pymongo import MongoClient
import pymongo
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_bus = client.buses
col_merge = db_bus.merge


import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools


rl_merge = col_merge.find({})

for each_record in rl_merge:
    layer = (each_record["layer"])
    id = each_record["_id"]
    wt_pr = each_record["wt_pr_" + layer]
    mr_pr = each_record["mr_pr_" + layer]
    wt_rr = each_record["wt_rr_" + layer]
    mr_rr = each_record["mr_rr_" + layer]

    col_merge.update_one({"_id": id}, {"$set":{
        "wt_pr": wt_pr,
        "mr_pr": mr_pr,
        "wt_rr": wt_rr,
        "mr_rr": mr_rr
    }})
