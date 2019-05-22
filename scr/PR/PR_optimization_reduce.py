# Optimization of PR.
# Reduce trips to routes for each day.
# The result is store in the cota_pr_optimization_result database. Each collection's name is date + "_route_reduced" 

from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools


walking_time_limit = 10
criteria = 5

is_paralleled = False

db_smart_transit = client.cota_pr_optimization_result

def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    db_pr_optimization_opt = db_smart_transit[today_date+"_opt"]
    
    records_dic = {}

    db_opt_result = list(db_pr_optimization_opt.find({}))

    for each_record in db_opt_result:
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]
        try:
            records_dic[route_id]
        except:
            records_dic[route_id]={}
        else:
            pass

        try:
            records_dic[route_id][stop_id]
        except:
            records_dic[route_id][stop_id]={}
            records_dic[route_id][stop_id]["route_id"] = each_record['route_id']
            records_dic[route_id][stop_id]["stop_id"] = each_record['stop_id']
            records_dic[route_id][stop_id]["lat"] = each_record['lat']
            records_dic[route_id][stop_id]["lon"] = each_record['lon']

            for walking_time in range(walking_time_limit):
                records_dic[route_id][stop_id]["sum_buff_"+str(walking_time)] = each_record["optima_buffer_"+str(walking_time)]
                records_dic[route_id][stop_id]["max_buff_"+str(walking_time)] = each_record["optima_buffer_"+str(walking_time)]
                if each_record["time_actual"] != each_record["time_alt_"+str(walking_time)]:
                    records_dic[route_id][stop_id]["miss_risk_"+str(walking_time)] = 1
                else:
                    records_dic[route_id][stop_id]["miss_risk_"+str(walking_time)] = 0
            records_dic[route_id][stop_id]["trip_count"] = 1
        else:
            for walking_time in range(walking_time_limit):
                records_dic[route_id][stop_id]["sum_buff_"+str(walking_time)] += each_record["optima_buffer_"+str(walking_time)]
                records_dic[route_id][stop_id]["max_buff_"+str(walking_time)] = max(each_record["optima_buffer_"+str(walking_time)], records_dic[route_id][stop_id]["max_buff_"+str(walking_time)])
                records_dic[route_id][stop_id]["miss_risk_"+str(walking_time)]
                if each_record["time_actual"] != each_record["time_alt_"+str(walking_time)]:
                    records_dic[route_id][stop_id]["miss_risk_"+str(walking_time)] += 1
            records_dic[route_id][stop_id]["trip_count"] += 1

    for each_route, stop_collection in records_dic.items():
        for each_stop, each_record in stop_collection.items():
            db_smart_transit[today_date+"_route_reduced"].insert_one(each_record)

    


if __name__ == '__main__':
    single_date = date(2018, 2, 1)
    analyze_transfer(single_date)
    