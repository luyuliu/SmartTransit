from pymongo import MongoClient
from pymongo import ASCENDING
from datetime import timedelta, date
import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import transfer_tools

client = MongoClient('mongodb://localhost:27017/')

for each_time_stamp in transfer_tools.db_time_stamps:
    db_stops=transfer_tools.db_GTFS[str(each_time_stamp)+"_stops"]
    db_stop_times=transfer_tools.db_GTFS[str(each_time_stamp)+"_stop_times"]
    db_trips=transfer_tools.db_GTFS[str(each_time_stamp)+"_trips"]
    db_seq = transfer_tools.db_GTFS[str(each_time_stamp) + "_trip_seq"]

    db_stop_times.create_index([("trip_id", ASCENDING),("stop_id", ASCENDING)])
    db_seq.create_index([("trip_id", ASCENDING),("stop_id", ASCENDING)])
    db_seq.create_index([("service_id", ASCENDING),("seq", ASCENDING),("route_id", ASCENDING),("stop_id", ASCENDING)])
    db_stops.create_index([("stop_id", ASCENDING)])
    db_trips.create_index([("service_id", ASCENDING), ("trip_id", ASCENDING)])
    db_trips.create_index([("service_id", ASCENDING), ("route_id", ASCENDING)])

start_date = date(2018, 2, 1)
end_date = date(2018, 1, 31)

db_opt_result = client.cota_pr_optimization_result
col_opt_result = db_opt_result.pr_opt_ibs
col_opt_result_risk_averse = db_opt_result.pr_opt_ibs_risk_averse
col_opt_result.create_index([("trip_id", ASCENDING),("stop_id", ASCENDING)])
col_opt_result_risk_averse.create_index([("trip_id", ASCENDING),("stop_id", ASCENDING)])

db_er = client.cota_er
col_er = db_er.er
col_er.create_index([("trip_id", ASCENDING),("stop_id", ASCENDING)])

db_trip_update = client.trip_update
col_trip_update = db_trip_update.full_trip_update
col_trip_update.create_index([("start_date", ASCENDING)])

db_real_time = client.cota_real_time
db_diff = client.cota_diff

date_range = transfer_tools.daterange(start_date, end_date)
for each_date in date_range:
    today_date = each_date.strftime("%Y%m%d")  # date
    col_real_time = db_real_time["R" + today_date]
    
    col_real_time.create_index([("stop_id", ASCENDING),("route_id", ASCENDING)])
    col_diff = db_diff[today_date]
    col_diff.create_index([("route_id", ASCENDING)])