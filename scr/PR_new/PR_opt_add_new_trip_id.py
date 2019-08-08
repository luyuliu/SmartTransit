# Optimization of PR.
# Calculate the average IB for a whole year. Get parameters for PR optimal.
# The result is store in the cota_pr_optimization_result database. Each collection's name is date + "pr_opt" 

from pymongo import MongoClient
import pymongo
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_pr_optimization_result
db_opt_fin = client.cota_pr_finalization

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

walking_time_limit = 10
criteria = 5

is_paralleled = False

memory = 120
start_date = date(2018, 1, 29)
end_date = date(2019, 1, 31)

db_new_trips = client.cota_gtfs_new_trips

def calculate_parameters(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    col_opt_result = db_opt_result[today_date + "_opt_risk_averse"]
    rl_opt_result = list(col_opt_result.find({}))

    this_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    col_new_trips=db_new_trips[str(this_time_stamp)+"_new_trips"]

    for each_record in rl_opt_result:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]

        rl_new_trips = col_new_trips.find_one({"trip_id" : trip_id})
        if rl_new_trips == None:
            new_trip_id = None
        else:
            new_trip_id = rl_new_trips["new_trip_id"]

        _id = each_record["_id"]
        col_opt_result.update_one({"_id": _id}, {"$set":{"new_trip_id": new_trip_id}})

            
    
    col_opt_result.create_index([("new_trip_id", pymongo.ASCENDING), ("stop_id", pymongo.ASCENDING)])
    print(today_date +" - Finish.")



if __name__ == '__main__':
    start_date = date(2018, 1, 29)
    end_date = date(2019, 1, 31)
    pool = multiprocessing.Pool(processes= 30)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(calculate_parameters, date_range)
    pool.close()
    pool.join()

    