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
db_result = client.cota_ar

def analyze_transfer(single_date):
    records_dic={} # Avoid IO. But could be bad for small memory.

    today_date = single_date.strftime("%Y%m%d")  # date
    col_real_time = db_real_time["R"+today_date]
    result_real_time = list(col_real_time.find({}))
    col_er = db_result[today_date]
    today_date = single_date.strftime("%Y%m%d")  # date

    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3
        
    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
    db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
    db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
    db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_today_real_time = db_real_time["R" + today_date]
    db_today_trip_update = db_trip_update[today_date]

    for each_record in result_real_time:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        sequence_id = each_record["seq"]

        each_trip_seq = list(db_seq.find({"trip_id": trip_id, "stop_id": stop_id}))

        if len(each_trip_seq) == 0:
            continue
        else:
            sequence_id = each_trip_seq[0]["seq"]

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
            records_dic[trip_id][stop_id]["time"] = each_record["time"] - 1500000000
            records_dic[trip_id][stop_id]["count"] = 1
        else:
            records_dic[trip_id][stop_id]["time"] += each_record["time"] - 1500000000
            records_dic[trip_id][stop_id]["count"] += 1
    print(today_date +" - Computation.")

    for trip_id, trip_records in records_dic.items():
        for stop_id, stop_time_records in trip_records.items():
            stop_time_records["time"] = stop_time_records["time"]/stop_time_records["count"] + 1500000000
            col_er.insert_one(stop_time_records)

    print(today_date +" - Database insert.")   


if __name__ == '__main__':
    analyze_transfer(date(2018, 2, 1))
    