# Optimization of PR.
# Calculate the average IB for a whole year. Get parameters for PR optimal.
# The result is store in the cota_pr_optimization_result database. Each collection's name is date + "pr_opt" 

from pymongo import MongoClient
import pymongo
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_opt_result = client.cota_re_pr_cal
db_opt_fin = client.cota_re_pr_opt
the_route_id = 7

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

walking_time_limit = 10
criteria = 5

is_paralleled = False

memory = 120

def calculate_parameters(end_date_p):
    start_date = date(2018, 7, 15)
    records_dic={} # Avoid IO. But could be bad for small memory.
    col_opt_fin = db_opt_fin["FIN_" + end_date_p.strftime("%Y%m%d") + "_" + str(the_route_id)]
    if (end_date_p - date (2018, 9, 3)).total_seconds()>=0:
        start_date = date(2018, 9, 2)
        if (end_date_p - date (2019, 1, 8)).total_seconds()>=0:
            start_date = date(2019, 1, 7)

    for single_date in transfer_tools.daterange(start_date, end_date_p):
        today_date = single_date.strftime("%Y%m%d")  # date
        print(today_date +" - Calculation.")
        col_opt_result = db_opt_result["OPT_MIN_" + today_date + "_" + str(the_route_id)]
        rl_opt_result = list(col_opt_result.find({}))

        for each_record in rl_opt_result:
            trip_id = each_record["new_trip_id"]
            stop_id = each_record["stop_id"]
            route_id = each_record["route_id"]
            try:
                records_dic[trip_id]
            except:
                records_dic[trip_id]={}
            else:
                pass

            try:
                records_dic[trip_id][stop_id]
            except:
                records_dic[trip_id][stop_id]={}
                records_dic[trip_id][stop_id]["route_id"] = each_record['route_id']
                records_dic[trip_id][stop_id]["stop_id"] = each_record['stop_id']
                records_dic[trip_id][stop_id]["new_trip_id"] = each_record['new_trip_id']
                records_dic[trip_id][stop_id]["lon"] = each_record['lon']
                records_dic[trip_id][stop_id]["lat"] = each_record['lat']
                records_dic[trip_id][stop_id]["trip_id"] = each_record['trip_id']

                for walking_time in range(walking_time_limit):
                    records_dic[trip_id][stop_id]["sum_buff_"+str(walking_time)] = each_record["optima_buffer_"+str(walking_time)]
                    records_dic[trip_id][stop_id]["max_buff_"+str(walking_time)] = each_record["optima_buffer_"+str(walking_time)]
                records_dic[trip_id][stop_id]["date_count"] = 1
            else:
                for walking_time in range(walking_time_limit):
                    records_dic[trip_id][stop_id]["sum_buff_"+str(walking_time)] += each_record["optima_buffer_"+str(walking_time)]
                    records_dic[trip_id][stop_id]["max_buff_"+str(walking_time)] = max(each_record["optima_buffer_"+str(walking_time)], records_dic[trip_id][stop_id]["max_buff_"+str(walking_time)])
                records_dic[trip_id][stop_id]["date_count"] += 1
            
        print(today_date +" - Database insert.")
    for each_trip, stop_collection in records_dic.items():
        for each_stop, each_record in stop_collection.items():
            col_opt_fin.insert_one(each_record)
    
    col_opt_fin.create_index([("new_trip_id", pymongo.ASCENDING), ("stop_id", pymongo.ASCENDING)])



if __name__ == '__main__':
    start_date = date(2018, 7, 15)
    end_date = date(2018, 7, 22)
    pool = multiprocessing.Pool(processes= 30)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(calculate_parameters, date_range)
    pool.close()
    pool.join()

    