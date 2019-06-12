
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
    wt_diff = [0] * 10 
    count =0

    for single_stop_time in rl_diff:
        lat = single_stop_time["lat"]
        for i in range(10):
            wt_diff [i] += single_stop_time["mc_pr_opt_" + str(i)] 

        # wt_diff_temp = single_stop_time["wt_ar"]* single_stop_time["total"]
        count += single_stop_time["total"]
    print(wt_diff, count)


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 30)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
