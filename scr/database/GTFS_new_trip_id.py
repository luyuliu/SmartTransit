import pymongo
from datetime import timedelta, date
import time

import os
import sys
import time, multiprocessing
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
trip_dic = {}

def GTFS_new_trips(each_time_stamp):
    db_seq=db_GTFS[str(each_time_stamp)+"_trip_seq"]
    db_stops=db_GTFS[str(each_time_stamp)+"_stops"]
    db_stop_times=db_GTFS[str(each_time_stamp)+"_stop_times"]
    db_trips=db_GTFS[str(each_time_stamp)+"_trips"]

    col_new_trip = db_new_trips[str(each_time_stamp) + "_new_trips"]

    rl_trips = list(db_trips.find({}))


    for each_trip in rl_trips:
        trip_id = each_trip["trip_id"]
        rl_stop_times = list(db_stop_times.find({"trip_id": trip_id}))
        start_time =transfer_tools.convert_to_timestamp(rl_stop_times[0]["arrival_time"], None) 
        
        start_stop_id = rl_stop_times[0]["stop_id"]

        route_id = int(each_trip["route_id"]) * (1 - 2* int(each_trip["direction_id"]))

        service_id = int(each_trip["service_id"])

        new_trip_id = str(start_time) + "_" + str(start_stop_id) + "_" + str(route_id) + "_" + str(service_id)
        each_trip["new_trip_id"] = new_trip_id
        col_new_trip.insert_one(each_trip)
    print(each_time_stamp)

if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=25)
    output = []
    output = pool.map(GTFS_new_trips, db_time_stamps)
    pool.close()
    pool.join()