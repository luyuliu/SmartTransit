# Optimization of PR.
# Calculate the average IB for a whole year. Get parameters for PR optimal.
# The result is store in the cota_pr_optimization_result database. Each collection's name is date + "pr_opt" 

from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_pr_optimization_test

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

walking_time_limit = 10
criteria = 5

is_paralleled = False


def calculate_parameters(start_date, end_date):
    
    records_dic={} # Avoid IO. But could be bad for small memory.

    for single_date in transfer_tools.daterange(start_date, end_date):
        today_date = single_date.strftime("%Y%m%d")  # date
        print(today_date +" - Calculation.")
        col_opt_result = db_opt_result[today_date + "_test"]
        rl_opt_result = list(col_opt_result.find({}))

        for each_record in rl_opt_result:
            trip_id = each_record["trip_id"]
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
                records_dic[trip_id][stop_id]["trip_id"] = each_record['trip_id']
                records_dic[trip_id][stop_id]["lon"] = each_record['lon']
                records_dic[trip_id][stop_id]["lat"] = each_record['lat']

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
            db_opt_result["pr_opt_ibs_risk_averse"].insert_one(each_record)
    


if __name__ == '__main__':
    start_date = date(2018, 1, 29)
    end_date = date(2019, 1, 31)
    calculate_parameters(start_date, end_date)

    