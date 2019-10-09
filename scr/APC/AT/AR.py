
from pymongo import MongoClient
from pymongo import ASCENDING
from datetime import timedelta, date
import datetime
import time as atime
import multiprocessing

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools


client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_apc_real_time
db_trip_update = client.trip_update
db_result = client.cota_apc_ar


def analyze_transfer(single_date):
    records_dic = {}  # Avoid IO. But could be bad for small memory.

    today_date = single_date.strftime("%Y%m%d")  # date
    col_real_time = db_real_time[today_date]
    result_real_time = list(col_real_time.find({"$or": [{"route_id": 2}, {"route_id": -2}]}))
    col_er = db_result[today_date]
    today_date = single_date.strftime("%Y%m%d")  # date

    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3

    today_date = single_date.strftime("%Y%m%d")  # date
    today_seconds = atime.mktime(atime.strptime(today_date, "%Y%m%d"))
    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    db_seq = db_GTFS[str(that_time_stamp) + "_trip_seq"]

    count = 0
    schedule_count = 0
    precrit_count = 0
    error_count = 0

    total_count = len(result_real_time)
    pre_count = db_result[today_date].estimated_document_count()
    if pre_count==total_count:
        print(today_date + " - Skip.")
        return False
    else:
        if pre_count!= 0:
            db_result[today_date].drop()
            print(today_date + " - Drop.")
        else:
            print(today_date + " - Start.")

    for each_record in result_real_time:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]
        time = each_record["actual_departure_time"]
        # Not stop_sequence, not real-time seq, the trip sequence array's trip_sequence
        sequence_id = each_record["trip_sequence"]
        if type(sequence_id) is not int:
            rand_time = "GTFS_error"
            error_count += 1
        else:
            alt_query = list(col_real_time.find(
                {"stop_id": stop_id, "trip_sequence": sequence_id - 1, "route_id": route_id}))
            if len(alt_query) == 0:
                alt_schedule = list(db_seq.find(
                    {"service_id": service_id, "stop_id": stop_id, "seq": sequence_id - 1, "route_id": route_id}))

                if len(alt_schedule) == 0:
                    if sequence_id != 0:
                        # print(route_id, stop_id, trip_id, sequence_id)
                        rand_time = "GTFS_error"
                        error_count += 1
                    else:
                        alt_query = "pre_critical_trip"
                        rand_time = "pre_critical_trip"
                        precrit_count += 1
                else:
                    alt_schedule = alt_schedule[0]
                    alt_time = alt_schedule["actual_departure_time"] + today_seconds
                    rand_time = (time + alt_time)/2
                    schedule_count += 1

            else:
                alt_query = alt_query[0]
                alt_time = alt_query["actual_departure_time"]
                rand_time = (time + alt_time)/2

        each_record['rand_time'] = rand_time
        each_record.pop('_id', None)
        db_result[today_date].insert_one(each_record)
        count += 1
        if count % 70000 == 1:
            print(today_date, ": ", count/total_count*100, "|", count -
                  precrit_count-schedule_count,  "|", schedule_count, "|", precrit_count, "|", error_count)

    print(today_date + " - Done.")


if __name__ == '__main__':
    # analyze_transfer(date(2018, 2, 2))
    start_date = date(2018, 5, 7)
    end_date = date(2019, 4, 30)

    # appendix_real_time(start_date)
    col_list_real_time = transfer_tools.daterange(start_date, end_date)

    cores = int(multiprocessing.cpu_count()/4*3)
    pool = multiprocessing.Pool(processes=cores)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
