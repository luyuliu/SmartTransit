
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import transfer_tools

from itertools import chain


db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_apc_pr_opt
db_ar = client.cota_apc_ar
db_er = client.cota_apc_er

db_diff = client.cota_apc_join
db_diff_reduce = client.cota_diff_reduce

walking_time_limit = 10  # min
criteria = 0  # seconds
designated_route_id = 2


def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)
    
    wt_ar = 0
    wt_ar_count = 0
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]

        rl_opt_result = list(
            col_diff.find({}))
            
        # wt_ar = 0
        # wt_ar_count = 0

        for each_record in rl_opt_result:
            time_ar_alt = each_record["time_actual"]
            time_ar_arr = each_record["time_ar_arr"]
            # print(time_ar_arr)

            if type(time_ar_alt) is int and type(time_ar_arr) is not str and time_ar_alt != 0 and time_ar_arr != 0:
                wt_ar += time_ar_alt - time_ar_arr
                wt_ar_count += 1
        
        if wt_ar_count != 0:
            print(today_date, wt_ar/wt_ar_count)
        else:
            print(0)

    print(wt_ar/wt_ar_count, wt_ar, wt_ar_count)


if __name__ == "__main__":
    start_date = date(2018, 5, 7)
    end_date = date(2019, 4, 30)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
