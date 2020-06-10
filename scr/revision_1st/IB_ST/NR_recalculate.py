
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
db_diff = client.cota_apc_pr_opt


def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    
    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    col_PR= db_diff["REV_" + today_date]
    rl_PR = list(col_PR.find({}))
    print(today_date + " - start:" ,len(rl_PR))
    count = 0

    insert_lines = []
    for each_record in rl_PR:
        line = {}
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]
        time_actual = each_record["time_actual"]
        time_nr_arr = int(each_record["time_normal"])
        [time_nr_alt, real_trip, real_trip_seq] = transfer_tools.find_alt_time_apc(time_nr_arr, route_id, stop_id, today_date)

        line["trip_id"] = each_record["trip_id"]
        line["stop_id"] = each_record["stop_id"]
        line["route_id"] = each_record["route_id"]
        line["time_actual"] = each_record["time_actual"]
        line["time_nr_arr"] = each_record["time_normal"]
        line["time_nr_alt"] = time_nr_alt
        line["real_trip_id"] = real_trip
        line["real_trip_seq"] = real_trip_seq
        line["lat"] = each_record["lat"]
        line["lon"] = each_record["lon"]
        
        insert_lines.append(line)

        if len(insert_lines) > 4000:
            client.cota_re_buffer_nr[today_date].insert_many(insert_lines)
            insert_lines = []

    if len(insert_lines) != 0:
        client.cota_re_buffer_nr[today_date].insert_many(insert_lines)
        insert_lines = []

    print(today_date + " - Done.",len(rl_PR))


if __name__ == '__main__':
    # analyze_transfer(date(2018, 2, 1))
    start_date = date(2018, 5, 8)
    end_date = date(2019, 4, 30)

    # for single_date in transfer_tools.daterange(start_date, end_date):
    #     analyze_transfer(single_date)

    cores = int(multiprocessing.cpu_count()/4*3)
    pool = multiprocessing.Pool(processes= 30)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
