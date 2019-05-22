from pymongo import MongoClient
from pymongo import ASCENDING
from datetime import timedelta, date
import datetime
import multiprocessing

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_result = client.cota_ar


def analyze_transfer(single_date):
    records_dic = {}  # Avoid IO. But could be bad for small memory.

    today_date = single_date.strftime("%Y%m%d")  # date
    col_real_time = db_real_time["R" + today_date]
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
    db_seq = db_GTFS[str(that_time_stamp) + "_trip_seq"]
    db_today_real_time = db_real_time["R" + today_date]
    db_today_trip_update = db_trip_update[today_date]

    #db_today_real_time.create_index([("stop_id", ASCENDING),("trip_sequence", ASCENDING)])
    count = 0
    total_count = len(result_real_time)
    for each_record in result_real_time:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        time = each_record["time"]
        sequence_id = each_record["seq"]
        alt_query = list(db_today_real_time.find(
            {"stop_id": stop_id, "trip_sequence": sequence_id - 1}))
        if len(alt_query) == 0:
            alt_query = "pre_critical_trip"
            rand_time = "pre_critical_trip"
        else:
            alt_query = alt_query[0]
            alt_time = alt_query["time"]
            rand_time = (time + alt_time)/2

        each_record['rand_time'] = rand_time
        each_record.pop('_id', None)
        db_result[today_date].insert_one(each_record)
        count += 1
        if count % 100 == 1:
            print(today_date, ": ", count/total_count*100)

    print(today_date + " - Done.")


if __name__ == '__main__':
    analyze_transfer(date(2018, 1, 29))
