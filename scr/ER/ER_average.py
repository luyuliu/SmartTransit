from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools


client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_er

def analyze_transfer(start_date, end_date):
    records_dic={} # Avoid IO. But could be bad for small memory.
    for single_date in transfer_tools.daterange(start_date, end_date):
        if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
            summer_time = 0
        else:
            summer_time = 1
        today_seconds = int((single_date - date(1970, 1, 1)
                         ).total_seconds()) + 18000 + 3600*summer_time
        today_date = single_date.strftime("%Y%m%d")  # date
        col_real_time = db_real_time["R"+today_date]
        result_real_time = list(col_real_time.find({}))
        col_er = db_opt_result["er"]
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
                records_dic[trip_id][stop_id]["time"] = each_record["time"] - today_seconds
                records_dic[trip_id][stop_id]["count"] = 1
            else:
                records_dic[trip_id][stop_id]["time"] += each_record["time"] - today_seconds
                records_dic[trip_id][stop_id]["count"] += 1
            
        print(today_date +" - Computation.")

    for trip_id, trip_records in records_dic.items():
        for stop_id, stop_time_records in trip_records.items():
            stop_time_records["time"] = int(stop_time_records["time"]/stop_time_records["count"])
            col_er.insert_one(stop_time_records)

    print(today_date +" - Database insert.")   


if __name__ == '__main__':
    analyze_transfer(date(2018, 2, 1), date(2019, 1, 31))