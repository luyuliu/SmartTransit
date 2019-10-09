
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing

import os
import sys
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_apc_real_time
db_trip_update = client.trip_update

db_er_opt = client.cota_apc_er
db_er_val = client.cota_apc_er_val

def validate_er(single_date, memory):
    start_date_p = single_date - timedelta(days=memory)
    if (start_date_p - date(2018, 5, 7)).total_seconds() < 0:
        return False
    col_er = db_er_opt["AVE_" +
                       str(memory) + "_" + start_date_p.strftime("%Y%m%d")]

    today_date = single_date.strftime("%Y%m%d")  # date
    today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))
    col_real_time = db_real_time[today_date]
    rl_real_time = list(col_real_time.find({"$or": [{"route_id": 2}, {"route_id": -2}]}))
    col_er_val = db_er_val["AVE_" + str(memory) + "_" + today_date]
    total_count = len(rl_real_time)
    count = 0
    print(today_date + " - " + str(memory) + " - Start.")

    pre_count = col_er_val.estimated_document_count()
    
    col_er_val.drop()
    print(start_date_p.strftime("%Y%m%d") + " - Drop.")
    # if pre_count==total_count:
    #     print(today_date + " - " + str(memory) + " - Skip.")
    #     return False
    # else:
    #     if pre_count!= 0:
    #         col_er_val.drop()
    #         print(today_date + " - " + str(memory) + " - Drop.")
    #     else:
    #         print(today_date + " - " + str(memory) + " - Start.")

    recordss = []
    for each_record in rl_real_time:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]
        query_rl = col_er.find_one({"trip_id": trip_id, "stop_id": stop_id})
        if query_rl == None:
            continue
        each_record["time_er_arr"] = int(query_rl["time"] + today_seconds)
        alt_cal_rl = transfer_tools.find_alt_time_apc(
            each_record["time_er_arr"], route_id, stop_id, today_date)
        each_record["time_er_alt"] = alt_cal_rl[0]
        each_record["time_er_trip_id"] = alt_cal_rl[1]
        each_record["time_er_trip_sequence"] = alt_cal_rl[2]
        each_record.pop("_id", None)
        recordss.append(each_record)

        if len(recordss) == 10000:
            col_er_val.insert_many(recordss)
            recordss = []
            print(today_date + " - " + str(memory) + " - Insert: " + str(int(count/total_count*10000)/100))

        count += 1
    
    if len(recordss)!= 0:
        col_er_val.insert_many(recordss)
        recordss = []

    print(today_date + " - " + str(memory) + " - Done.")


if __name__ == "__main__":
    start_date = date(2018, 5, 8)
    end_date = date(2019, 4, 30)
    # print((start_date - end_date).total_seconds() > 0)

    for memory in range(1, 10):
        date_range = list(transfer_tools.daterange(start_date, end_date))
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=35)
        output = []
        output = pool.starmap(validate_er, zip(
            date_range, [memory]*len(date_range)))
        pool.close()
        pool.join()
