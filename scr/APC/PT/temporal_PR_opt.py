
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
    
    wt_gr = [0] * 10
    wt_gr_count =[0] * 10
    wt_av_gr = [0] * 10
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]

        rl_opt_result = list(
            col_diff.find({}))
            
        large_count = 0
        for each_record in rl_opt_result:
            for i in range(10):
                try:
                    time_gr_alt = each_record["time_alt_" + str(i)]
                    time_gr_arr = each_record["time_smart_" + str(i)]
                except:
                    continue

                if type(time_gr_alt) is int and type(time_gr_arr) is int and time_gr_alt != 0 and time_gr_arr != 0 and time_gr_alt != 9999999999 and time_gr_alt != 9999999999:
                    wt_gr[i] += time_gr_alt - time_gr_arr
                    wt_gr_count[i] += 1

                    if time_gr_alt - time_gr_arr > 60*60*1:
                        large_count += 1
                        # print((time_gr_alt - time_gr_arr)/60)
                        # print(each_record["route_id"])
        
        for i in range(10):
            if wt_gr_count[0] != 0:
                wt_av_gr[i] = (wt_gr[i]/wt_gr_count[i])

        print(today_date,large_count, wt_av_gr[0], wt_av_gr[1],wt_av_gr[2],wt_av_gr[3],wt_av_gr[4],wt_av_gr[5],wt_av_gr[6],wt_av_gr[7],wt_av_gr[8],wt_av_gr[9],)
        # print(today_date,large_count)
    


if __name__ == "__main__":
    start_date = date(2018, 5, 7)
    end_date = date(2019, 4, 30)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
