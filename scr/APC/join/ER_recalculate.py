
from pymongo import MongoClient
from pymongo import ASCENDING
from datetime import timedelta, date
import datetime
import multiprocessing

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools


client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_apc_real_time
db_trip_update = client.trip_update
db_er_val = client.cota_apc_er_val
db_diff = client.cota_apc_join


def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    
    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    col_diff = db_diff[today_date]
    col_er_val = db_er_val["MIN_6_" + today_date]
    rl_diff = list(col_diff.find({}))
    count = 0

    for each_record in rl_diff:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]

        rl_er_val = col_er_val.find_one({"stop_id": stop_id, "trip_id": trip_id})
        if rl_er_val == None:
            time_er_alt = "None"
            time_er_arr = "None"
        else:
            time_er_alt = rl_er_val["time_er_alt"]
            time_er_arr = rl_er_val["time_er_arr"]
        
        _id = each_record["_id"]
        col_diff.update_one({"_id": _id}, {"$set":{"time_er_arr": time_er_arr, "time_er_alt": time_er_alt}})
    print(today_date + " - Done.")


if __name__ == '__main__':
    # analyze_transfer(date(2018, 2, 1))
    start_date = date(2018, 5, 7)
    end_date = date(2019, 4, 30)

    # for single_date in transfer_tools.daterange(start_date, end_date):
    #     analyze_transfer(single_date)

    # cores = int(multiprocessing.cpu_count()/4*3)
    pool = multiprocessing.Pool(processes= 30)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
