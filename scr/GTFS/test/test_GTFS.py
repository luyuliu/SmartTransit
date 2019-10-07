import pymongo
from datetime import timedelta, date
import time

import os
import sys
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools


def sortArray(A):
    return A["time"]


# database setup
client = pymongo.MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs

db_time_stamps_set=set()
db_time_stamps=[]
raw_stamps=db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw=int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()

schedule_count = 0
total_schedule_count = len(db_time_stamps)

db_new_trips = client.cota_gtfs_new_trips

last_trip_list = []

count = 0
for each_time_stamp in db_time_stamps:
    db_seq=db_GTFS[str(each_time_stamp)+"_trip_seq"]
    db_stops=db_GTFS[str(each_time_stamp)+"_stops"]
    db_stop_times=db_GTFS[str(each_time_stamp)+"_stop_times"]
    db_trips=db_new_trips[str(each_time_stamp)+"_new_trips"]

    query_trip=list(db_trips.find({"route_id": "002"},no_cursor_timeout=True).sort("new_trip_id"))
    trip_list = []
    for i in query_trip:
        trip_list.append(i["new_trip_id"])


    print("-----------------------","FindDone: ",each_time_stamp,schedule_count,total_schedule_count,"-----------------------")
    
    if count == 0 :
        last_trip_list = trip_list
        count += 1
        continue
    
    setA = set(trip_list)
    setB = set(last_trip_list)
    intersec = setA.intersection(setB)
    print(len(intersec), len(setA), len(setB))

    db_trips.create_index([("trip_id", pymongo.ASCENDING)])


    last_trip_list = trip_list

    count += 1