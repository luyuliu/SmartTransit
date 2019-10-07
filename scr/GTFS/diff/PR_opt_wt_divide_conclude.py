
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

db_diff = client.cota_diff
db_diff_reduce = client.cota_diff_reduce

walking_time_limit = 10  # min
criteria = 5  # seconds
designated_route_id = 2


def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)

    col_diff = db_diff_reduce["-2_stops_max"]



    rl_diff = list(col_diff.find({}))
    low_wt_diff = 0
    high_wt_diff = 0
    low_count = 0
    high_count = 0

    for single_stop_time in rl_diff:
        lat = single_stop_time["lat"]
        wt_diff = 0
        for i in range(10):
            wt_diff += single_stop_time["wt_pr_opt_" +
                                        str(i)] - single_stop_time["wt_nr"]
        wt_diff = wt_diff/10
        if float(lat) > 39.991224:
            low_wt_diff += wt_diff * single_stop_time["total"]
            low_count += single_stop_time["total"]
        else:
            high_wt_diff += wt_diff * single_stop_time["total"]
            high_count += single_stop_time["total"]
    print(low_wt_diff/low_count, high_wt_diff/high_count)


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 30)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
