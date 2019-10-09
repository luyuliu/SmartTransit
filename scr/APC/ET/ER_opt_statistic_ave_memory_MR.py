
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
import time

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_er_val = client.cota_apc_er_val


def analyze_transfer(memory):
    start_date = date(2018, 5, 8)
    end_date = date(2019, 4, 30)
    wt_er = 0 
    wt_er_count = 0
    for single_date in transfer_tools.daterange(start_date, end_date):
        today_date = single_date.strftime("%Y%m%d")  # date
        col_er_val = db_er_val["AVE_" + str(memory) + "_" + today_date]
        rl_opt_result = list(col_er_val.find({}))
        for each_record in rl_opt_result:
            time_er_alt = each_record["time_er_alt"]
            time_er_arr = each_record["time_er_arr"]
            if type(time_er_alt) is int and type(time_er_arr) is not str and time_er_alt != 0 and time_er_arr != 0:
                if time_er_alt < time_er_arr:
                    wt_er += 1
                wt_er_count += 1
        if wt_er_count != 0:
            print("Day - ", today_date, wt_er, (wt_er/wt_er_count), wt_er_count)
        else:
            print(0)
    average = (wt_er/wt_er_count)
    
    wt_er_val = 0
    for single_date in transfer_tools.daterange(start_date, end_date):
        today_date = single_date.strftime("%Y%m%d")  # date
        col_er_val = db_er_val["AVE_" + str(memory) + "_" + today_date]
        rl_opt_result = list(col_er_val.find({}))
        for each_record in rl_opt_result:
            time_er_alt = each_record["time_er_alt"]
            time_er_arr = each_record["time_er_arr"]
            
            if type(time_er_alt) is int and type(time_er_arr) is not str and time_er_alt != 0 and time_er_arr != 0:
                single_miss = 1 if time_er_alt < time_er_arr else 0
                wt_er_val += ((single_miss) - average) ** 2
        
    if wt_er_count != 0:
        print("Final - ", (wt_er_val/wt_er_count)**(1/2), wt_er_count)
        return (wt_er_val/wt_er_count)**(1/2)
    else:
        print(0)
        return 0
    



if __name__ == '__main__':
    # pool = multiprocessing.Pool(processes=9)
    # output = []
    # output = pool.map(analyze_transfer, range (1,10))
    # pool.close()
    # pool.join()

    # print("The answer is: ", output)
    analyze_transfer(6)