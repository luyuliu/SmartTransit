
from pymongo import MongoClient, ASCENDING
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
db_real_time = client.cota_apc_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_re_er_cal

def analyze_transfer(start_date_p, buffer):
    memory = 6
    print(start_date_p.strftime("%Y%m%d") + " - Start.")
    end_date_p = start_date_p + timedelta(days=memory)
    records_dic = {}  # Avoid IO. But could be bad for small memory.
    col_er = db_opt_result[start_date_p.strftime("%Y%m%d") + "_" + str(buffer)]
    col_er.drop()
    print(start_date_p.strftime("%Y%m%d") + " - Drop.")
    for single_date in transfer_tools.daterange(start_date_p, end_date_p):
        today_date = single_date.strftime("%Y%m%d")  # date
        today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))

        col_real_time = db_real_time[today_date]
        result_real_time = list(col_real_time.find({"$or": [{"route_id": 2}, {"route_id": -2}]}))
        for each_record in result_real_time:
            trip_id = each_record["trip_id"]
            stop_id = each_record["stop_id"]

            try:
                records_dic[trip_id]
            except:
                records_dic[trip_id] = {}
            else:
                pass

            try:
                records_dic[trip_id][stop_id]
            except:
                records_dic[trip_id][stop_id] = {}
                records_dic[trip_id][stop_id]["trip_id"] = trip_id
                records_dic[trip_id][stop_id]["stop_id"] = stop_id
                records_dic[trip_id][stop_id]["time"] = each_record["actual_departure_time"] - \
                    today_seconds - buffer
                records_dic[trip_id][stop_id]["count"] = 1
            else:
                records_dic[trip_id][stop_id]["time"] = min(
                    records_dic[trip_id][stop_id]["time"], each_record["actual_departure_time"] - today_seconds - buffer)
                records_dic[trip_id][stop_id]["count"] += 1

    for trip_id, trip_records in records_dic.items():
        for stop_id, stop_time_records in trip_records.items():
            col_er.insert_one(stop_time_records)

    col_er.create_index([("trip_id", ASCENDING), ("stop_id", ASCENDING)])
    print(start_date_p.strftime("%Y%m%d") + " - Done.")


if __name__ == '__main__':
    start_date = date(2018, 5, 7)
    end_date = date(2019, 5, 6)

    buffer_list = range(15, 121, 15)

    for memory in buffer_list:
        date_range = list(transfer_tools.daterange(start_date, end_date - timedelta(days = memory)))
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=30)
        output = []
        output = pool.starmap(analyze_transfer, zip(date_range,[memory]*len(date_range)))
        pool.close()
        pool.join()


