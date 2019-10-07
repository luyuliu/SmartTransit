
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

walking_time_limit = 10  # min
criteria = 5  # seconds
designated_route_id = 2


def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)

    wt_ar = 0
    wt_ar_count = 0
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff["MX" + "_" + today_date]

        rl_opt_result = list(
            col_diff.find({}))
            

        for each_record in rl_opt_result:
            # AR
            # time_ar_alt = each_record["time_actual"] 
            # time_ar_arr = each_record["time_ar_arr"]
            
            # ER
            # time_ar_alt = each_record["time_er_alt"]
            # time_ar_arr = each_record["time_er_arr"]

            # nR
            time_ar_alt = each_record["time_nr_alt"]
            time_ar_arr = each_record["time_actual"]

            # GR

            # PR
            # for i in range(10):
            #     try:
            #         # time_gr_alt = each_record["time_alt_" + str(i)]
            #         # time_gr_arr = each_record["time_smart_" + str(i)]

                    
            #         # GR
            #         time_gr_alt = each_record["time_rr_alt_" + str(i)]
            #         time_gr_arr = each_record["time_actual"]
            #     except:
            #         continue

            #     if type(time_gr_alt) is int and type(time_gr_arr) is not str and time_gr_alt != 0 and time_gr_arr != 0:
            #         if time_gr_alt - time_gr_arr>0:
            #             wt_ar += 1
            #         wt_ar_count += 1

            if type(time_ar_alt) is int and type(time_ar_arr) is not str and time_ar_alt != 0 and time_ar_arr != 0:
                if time_ar_alt - time_ar_arr>0:
                    wt_ar += 1
                wt_ar_count += 1
        print(today_date, wt_ar / wt_ar_count*100 )


    average = wt_ar / wt_ar_count 
    print("__________-___________")
    print(average)

    wt_ar_var = 0
    date_range = transfer_tools.daterange(start_date, end_date)
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff["MX_" + today_date]

        rl_opt_result = list(
            col_diff.find({}))
            
        for each_record in rl_opt_result:

            # PR
            for i in range(10):
                try:
                    time_gr_alt = each_record["time_alt_" + str(i)]
                    time_gr_arr = each_record["time_smart_" + str(i)]
                    
                    # GR
                    # time_gr_alt = each_record["time_rr_alt_" + str(i)]
                    # time_gr_arr = each_record["time_rr_arr_" + str(i)]
                except:
                    continue

                if type(time_gr_alt) is int and type(time_gr_arr) is int and time_gr_alt != 0 and time_gr_arr != 0:
                    single_mc = 0
                    if time_gr_alt - time_gr_arr<0:
                        single_mc = 1
                    wt_ar_var += (single_mc - average)**2


            # AR
            # time_ar_alt = each_record["time_actual"] 
            # time_ar_arr = each_record["time_ar_arr"]

            # NR
            # time_ar_alt = each_record["time_actual"]
            # time_ar_arr = each_record["time_normal"]
            # print(time_ar_arr)

            # if type(time_ar_alt) is int and type(time_ar_arr) is not str and time_ar_alt != 0 and time_ar_arr != 0:
            #     single_mc = 0
            #     if time_ar_alt - time_ar_arr<0:
            #         single_mc = 1
            #     wt_ar_var += (single_mc - average)**2
        print(today_date)

    if wt_ar_count != 0:
        print("Final: ", (wt_ar_var/wt_ar_count)**(1/2))
    else:
        print(0)

    


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
