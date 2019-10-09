
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
import time

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import transfer_tools

client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_pr_val = client.cota_apc_pr_opt


def analyze_transfer(memory):
    start_date = date(2018, 5, 7)
    end_date = date(2019, 5, 6)
    wt_pr = 0 
    wt_pr_count = 0
    for single_date in transfer_tools.daterange(start_date, end_date):
        today_date = single_date.strftime("%Y%m%d")  # date
        col_pr_val = db_pr_val["REV_" + (single_date).strftime("%Y%m%d")]
        rl_opt_result = list(col_pr_val.find({}))
        for each_record in rl_opt_result:
            for i in range(10):
                try:
                    time_pr_alt = each_record["time_alt_" + str(i)]
                    time_pr_act = each_record["time_actual"]
                except:
                    continue

                if type(time_pr_alt) is int and type(time_pr_act) is int and time_pr_alt != 0 and time_pr_act != 0:
                    if time_pr_alt > time_pr_act:
                        wt_pr += 1
                    wt_pr_count += 1
        print(today_date)
        if wt_pr_count != 0:
            print("Day - ", today_date, (wt_pr/wt_pr_count), wt_pr_count)
        else:
            print(0)
        
    average = (wt_pr/wt_pr_count)
    
    wt_pr_val = 0
    for single_date in transfer_tools.daterange(start_date, end_date):
        today_date = single_date.strftime("%Y%m%d")  # date
        col_pr_val = db_pr_val["REV_" + today_date]
        rl_opt_result = list(col_pr_val.find({}))
        for each_record in rl_opt_result:
            for i in range(10):
                try:
                    time_pr_alt = each_record["time_alt_" + str(i)]
                    time_pr_act = each_record["time_actual"]
                except:
                    continue
            
                if type(time_pr_alt) is int and type(time_pr_act) is not str and time_pr_alt != 0 and time_pr_act != 0:
                    single_miss = 1 if time_pr_alt < time_pr_act else 0
                    wt_pr_val += ((single_miss) - average) ** 2
        
    if wt_pr_count != 0:
        print("Final - ", (wt_pr_val/wt_pr_count)**(1/2), wt_pr_count, average)
        return [(wt_pr_val/wt_pr_count)**(1/2) ,average, memory, wt_pr_count]
    else:
        print(0)
        return [0, memory]
    



if __name__ == '__main__':
    # pool = multiprocessing.Pool(processes=9)
    # output = []
    # output = pool.map(analyze_transfer, range (1,10))
    # pool.close()
    # pool.join()

    # print("The answer is: ", output)
    analyze_transfer(1)