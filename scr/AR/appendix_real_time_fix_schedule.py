from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing

import os
import sys, time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update


def appendix_real_time(single_date):   
    today_date = single_date.strftime("%Y%m%d")  # date
    today_seconds = int(time.mktime(time.strptime(today_date, "%Y%m%d")))
    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3

    print(today_seconds)
    # print(today_date, ": Start.")

    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]

    col_real_time = db_real_time["R" + today_date]
    rl_real_time = list(col_real_time.find({}))
    total_count = col_real_time.estimated_document_count()
    count = 0
    for each_record in rl_real_time:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        rid = each_record["_id"]
        
        trip_seq_query =(db_seq.find_one(
            {"trip_id": trip_id, "stop_id": stop_id}))
        if (trip_seq_query) == None:
            scheduled_time = "GTFS_error"
        else:
            scheduled_time = int(trip_seq_query["time"]) + today_seconds

        # print(scheduled_time, today_seconds, scheduled_time - today_seconds)
        original_query = {"_id": rid}
        update_object = {"$set": {
            "scheduled_time": scheduled_time
        }}

        col_real_time.update_one(original_query, update_object)
        count += 1
        if count % 100000 == 1:
            print(today_date, ": ", count/total_count*100)
    print(today_date, ": Done.")


if __name__ == "__main__":
    start_date = date(2018, 1, 30)
    end_date = date(2019, 1, 31)

    # appendix_real_time(start_date)
    col_list_real_time = transfer_tools.daterange(start_date, end_date)

    cores = int(multiprocessing.cpu_count()/4*3)
    pool = multiprocessing.Pool(processes=cores)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(appendix_real_time, date_range)
    pool.close()
    pool.join()
