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
db_er = client.cota_er
col_er = db_er.er

criteria = 5


def validate_er(single_date):
    records_dic={} # Avoid IO. But could be bad for small memory.
    today_date = single_date.strftime("%Y%m%d")  # date
    if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
        summer_time = 0
    else:
        summer_time = 1
    today_seconds = int((single_date - date(1970, 1, 1)
                        ).total_seconds()) + 18000 + 3600*summer_time
    print(today_date + " - Start.")
    col_real_time = db_real_time["R" + today_date]
    rl_real_time = list(col_real_time.find({}))
    col_er_val = db_er[today_date]
    total_count = len(rl_real_time)
    count = 0

    for each_record in rl_real_time:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]
        real_time = each_record["time"]
        query_rl = col_er.find_one({"trip_id": trip_id, "stop_id": stop_id})
        if query_rl == None:
            continue
        each_record["time_er_arr"] = int(query_rl["time"]) + today_seconds
        alt_cal_rl = transfer_tools.find_alt_time(each_record["time_er_arr"], route_id, stop_id, today_date, criteria)
        each_record["time_er_alt"] = alt_cal_rl[0]
        each_record["time_er_trip_id"] = alt_cal_rl[1]
        each_record["time_er_trip_sequence"] = alt_cal_rl[2]
        each_record.pop("_id", None)
        col_er_val.insert_one(each_record)

        count+=1
    
    print(today_date + " - Done.")

if __name__ == "__main__":
    # analyze_transfer(date(2018, 2, 2))
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)

    # validate_er(start_date)
    cores = int(multiprocessing.cpu_count()/4*3)
    pool = multiprocessing.Pool(processes=cores)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(validate_er, date_range)
    pool.close()
    pool.join()
