
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
    start_date = date(2018, 5, 7)
    end_date = date(2019, 5, 6)
    date_range = transfer_tools.daterange(start_date, end_date)
    wt_gr = [0] * 7
    wt_gr_count =[0] * 7

    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]
        today_weekday = single_date.weekday()
        rl_opt_result = list(
            col_diff.find({}))
            
        

        for each_record in rl_opt_result:
            for i in range(10):
                try:
                    time_gr_alt = each_record["time_alt_" + str(i)]
                    time_gr_arr = each_record["time_smart_" + str(i)]
                except:
                    continue

                if type(time_gr_alt) is int and type(time_gr_arr) is int and time_gr_alt != 0 and time_gr_arr != 0:
                    wt_gr[today_weekday] += time_gr_alt - time_gr_arr
                    wt_gr_count[today_weekday] += 1
        print(today_date)

    for today_weekday in range(7):
        if wt_gr_count[0] != 0:
            wt_gr[today_weekday] = (wt_gr[today_weekday]/wt_gr_count[today_weekday])

    print(today_date, wt_gr[0], wt_gr[1],wt_gr[2],wt_gr[3],wt_gr[4],wt_gr[5],wt_gr[6])

    


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
