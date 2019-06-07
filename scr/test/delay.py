
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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

walking_time_limit = 10  # min
criteria = 5  # seconds
designated_route_id = 2


def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)
    db_diff_reduce["delay"].drop()

    dic_stops = {}
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]

        that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
        rl_opt_result = list(
            db_real_time['R' + today_date].find({"route_id": 1}))
        # db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
        # db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
        # db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
        # db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
        # col_real_time = db_real_time["R" + today_date]
        # col_trip_update = db_trip_update[today_date]

        for each_record in rl_opt_result:
            stop_id = each_record["stop_id"]
            time_actual = each_record["time"]
            time_normal = each_record["scheduled_time"]
            try:
                dic_stops[stop_id]
            except:
                dic_stops[stop_id] = {}
                dic_stops[stop_id]["stop_id"] = stop_id
                dic_stops[stop_id]["stop_sequence"] = each_record["stop_sequence"]
                dic_stops[stop_id]["delay"] = 0
                dic_stops[stop_id]["count"] = 0
                dic_stops[stop_id]["lat"] = each_record["lat"]
                dic_stops[stop_id]["lon"] = each_record["lon"]

            if type(time_actual) is not int or type(time_normal) is not int:
                print(time_actual, time_normal)
                continue
            dic_stops[stop_id]["delay"] += time_actual - time_normal
            dic_stops[stop_id]["count"] += 1
        print(today_date)

    for index, each_record in dic_stops.items():
        db_diff_reduce["delay"].insert_one(each_record)


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2018, 2, 2)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
