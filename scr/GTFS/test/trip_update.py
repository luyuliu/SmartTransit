
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')
sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))
import transfer_tools

from itertools import chain

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization
db_opt_result = client.cota_pr_optimization_test
db_ar = client.cota_ar
db_er = client.cota_er

db_diff = client.cota_diff
db_diff_reduce = client.cota_diff_reduce
db_delay_reclamation = client.cota_delay_reclamation

walking_time_limit = 10  # min
criteria = 5  # seconds
designated_route_id = 2

def sortKey(A):
    return A["stop_sequence"]

def reduce_diff(single_date, trip_id, stop_id):
    dics = {}
    col_delay_reclamation = db_delay_reclamation["-2_delay_reclamation"]

    dic_stops = {}
    today_date = single_date.strftime("%Y%m%d")  # date
    col_diff = db_diff[today_date]
    today_weekday = single_date.weekday()

    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    # rl_opt_result = list(
    #     db_real_time['R' + today_date].find({"route_id": -2}))
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
    # db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
    db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
    # db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
    # col_real_time = db_real_time["R" + today_date]
    col_trip_update = db_trip_update[today_date]

    this_error_count = 0
    this_total_count = 0
    this_delay = 0
    
    rl_opt_result = list(
        col_trip_update.find({"trip_id": trip_id}))
    
    for each_record in rl_opt_result:
        each_seq = each_record["seq"]
        for each_item in each_seq:
            if each_item["stop"] == stop_id:
                print(each_item["arr"], each_record["ts"])
                break



if __name__ == "__main__":
    trip_id = "619237"
    stop_id = "MOU5THW1"
    end_date = date(2018, 2, 7)
    reduce_diff(end_date, trip_id , stop_id)

    # Todo: find skip reason
