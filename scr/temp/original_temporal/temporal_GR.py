
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
    
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff["MX" + "_" + today_date]

        rl_opt_result = list(
            col_diff.find({}))
            
        wt_gr = [0] * 10
        wt_gr_count =[0] * 10

        for each_record in rl_opt_result:
            for i in range(10):
                try:
                    time_gr_alt = each_record["time_rr_alt_" + str(i)]
                    time_gr_arr = each_record["time_rr_arr_" + str(i)]
                except:
                    continue

                if type(time_gr_alt) is int and type(time_gr_arr) is int and time_gr_alt != 0 and time_gr_arr != 0:
                    wt_gr[i] += time_gr_alt - time_gr_arr
                    wt_gr_count[i] += 1
        
        for i in range(10):
            if wt_gr_count[0] != 0:
                wt_gr[i] = (wt_gr[i]/wt_gr_count[i])

        print(today_date, wt_gr[0], wt_gr[1],wt_gr[2],wt_gr[3],wt_gr[4],wt_gr[5],wt_gr[6],wt_gr[7],wt_gr[8],wt_gr[9],)

    


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
