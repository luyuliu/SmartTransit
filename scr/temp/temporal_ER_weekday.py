
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

    wt_er = [0] *7
    wt_er_count = [0] *7
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]
        today_weekday = single_date.weekday()

        rl_opt_result = list(
            col_diff.find({}))
            

        for each_record in rl_opt_result:
            time_er_alt = each_record["time_er_alt"]
            time_er_arr = each_record["time_er_arr"]

            if type(time_er_alt) is int and type(time_er_arr) is int and time_er_alt != 0 and time_er_arr != 0:
                wt_er[today_weekday] += time_er_alt - time_er_arr
                wt_er_count[today_weekday] += 1
        
        print(today_date, wt_er[0], wt_er[1],wt_er[2],wt_er[3],wt_er[4],wt_er[5],wt_er[6])
        
    for today_weekday in range(7):
        if wt_er_count[0] != 0:
            wt_er[today_weekday] = (wt_er[today_weekday]/wt_er_count[today_weekday])

    print(today_date, wt_er[0], wt_er[1],wt_er[2],wt_er[3],wt_er[4],wt_er[5],wt_er[6])

    


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
