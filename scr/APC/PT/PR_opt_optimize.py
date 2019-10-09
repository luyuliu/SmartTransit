# Optimization of PR.
# Calculate PR optimal's parameters (IB) for every day. For each single (stop_time), find the one with shortest waiting time and reassign the value to the PR optimal
# The result is store in the cota_pr_optimization_result database. Each collection's name is date + "_opt" 

from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_apc_pr_cal
db_opt = client.cota_apc_pr_fin

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

walking_time_limit = 10
criteria = 5

is_paralleled = False


def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    
    insurance_buffers = range(0, 301, 10)
    # insurance_buffers = range(300, -1, -10)

    records_dic=[] # Avoid IO. But could be bad for small memory.
    print(today_date +" - Initialization.")
    col_pr_opt_result = db_smart_transit["OPT_MIN_" + today_date] # only use the date before the training day; And use risk averse strategy
    pre_count = col_pr_opt_result.estimated_document_count()
    count = db_smart_transit[today_date+"_0"].estimated_document_count()

    if count == pre_count:
        print(today_date + " - Skip.")
        return False

    for each_buffer in insurance_buffers:
        db_today_smart_transit = db_smart_transit[today_date+"_"+str(each_buffer)]
        each_buffer_trip_collection = list(db_today_smart_transit.find({}))
        if each_buffer == insurance_buffers[0]:
            records_dic = each_buffer_trip_collection
            for each_record in records_dic:
                for walking_time in range(walking_time_limit):
                    each_record["optima_buffer_"+str(walking_time)] = 0
        else:
            for index in range(len(each_buffer_trip_collection)):
                for walking_time in range(walking_time_limit):
                    if type(each_buffer_trip_collection[index]["time_alt_"+str(walking_time)]) is not int or type(each_buffer_trip_collection[index]["time_smart_"+str(walking_time)]) is not int or each_buffer_trip_collection[index]["time_alt_"+str(walking_time)] == 0:
                        continue
                    if type(records_dic[index]["time_alt_"+str(walking_time)]) is not int or type(records_dic[index]["time_smart_"+str(walking_time)]) is not int or records_dic[index]["time_alt_"+str(walking_time)] == 0:
                        records_dic[index]["time_alt_"+str(walking_time)] = each_buffer_trip_collection[index]["time_alt_"+str(walking_time)]
                        records_dic[index]["time_smart_"+str(walking_time)] = each_buffer_trip_collection[index]["time_smart_"+str(walking_time)]
                        records_dic[index]["optima_buffer_"+str(walking_time)] = each_buffer
                        continue
                    if each_buffer_trip_collection[index]["time_alt_"+str(walking_time)] - each_buffer_trip_collection[index]["time_smart_"+str(walking_time)] < records_dic[index]["time_alt_"+str(walking_time)] - records_dic[index]["time_smart_"+str(walking_time)]:
                        records_dic[index]["time_alt_"+str(walking_time)] = each_buffer_trip_collection[index]["time_alt_"+str(walking_time)]
                        records_dic[index]["time_smart_"+str(walking_time)] = each_buffer_trip_collection[index]["time_smart_"+str(walking_time)]
                        records_dic[index]["optima_buffer_"+str(walking_time)] = each_buffer
    
    if len(records_dic)!= 0:
        col_pr_opt_result.insert_many(records_dic)
        print(today_date +" - Database insert.")
    else:
        print(today_date +" - Skip.")
    


if __name__ == '__main__':
    # single_date = date(2018, 2, 1)
    # analyze_transfer(single_date)

    # start_date = date(2018, 5, 7)
    # end_date = date(2019, 1, 31)

    # start_date = date(2019, 1, 31)
    # end_date = date(2019, 5, 6)

    start_date = date(2018, 5, 7)
    end_date = date(2019, 5, 6)

    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=35)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
    
    # for single_date in transfer_tools.daterange(start_date, end_date):
    #     analyze_transfer(single_date)