
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools


db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization
db_opt_result = client.cota_pr_optimization_result
db_ar = client.cota_ar
db_er = client.cota_er


def calculate_diff(single_date):
    records_dic = {}  # Avoid IO. But could be bad for small memory.

    today_date = single_date.strftime("%Y%m%d")  # date
    col_real_time = db_real_time["R" + today_date]
    result_real_time = list(col_real_time.find({}))

    today_date = single_date.strftime("%Y%m%d")  # date

    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3

    if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
        summer_time = 0
    else:
        summer_time = 1
    today_seconds = int((single_date - date(1970, 1, 1)
                         ).total_seconds()) + 18000 + 3600*summer_time
    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
    db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
    db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
    db_seq = db_GTFS[str(that_time_stamp) + "_trip_seq"]
    db_today_real_time = db_real_time["R" + today_date]
    db_today_trip_update = db_trip_update[today_date]

    # ----------------------------------------------------------------------------------

    RTATPS = "RR"
    nonRTATPS = "AR"

    if RTATPS == "RR":
        col_RTA = db_smart_transit[today_date + "_0"]
    elif RTATPS == "PR_opt":
        col_RTA = db_opt_result[today_date]
    else:
        print("Setting error.")
        return False

    if nonRTATPS == "AR":
        col_nonRTA = db_ar[today_date]

    rl_RTA = list(col_RTA.find({}))

    rl_nonRTA = list(col_nonRTA.find({"$or": [{"route_id": 2}, {"route_id": -2}]}))

    print(len(rl_nonRTA), len(rl_RTA))

    for each_record in rl_RTA:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]


if __name__ == "__main__":
    calculate_diff(date(2018, 2, 1))
