
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization
db_opt_result = client.cota_pr_optimization_result
db_ar = client.cota_ar
db_er = client.cota_er

db_diff = client.cota_diff

walking_time_limit = 10 # min
criteria = 5 # seconds

def calculate_diff(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    print(today_date, " - Start.")
    col_real_time = db_real_time["R" + today_date]

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
    col_real_time = db_real_time["R" + today_date]
    col_trip_update = db_trip_update[today_date]

    # ----------------------------------------------------------------------------------
    col_rr = db_smart_transit[today_date + "_0"] # RR
    rl_rr = list(col_rr.find({}))

    col_pr_opt = db_opt_result[today_date + "_reval_max"] # PR optimal
    rl_pr_opt = list(col_pr_opt.find({}))

    col_ar = db_ar[today_date] # AR
    rl_ar = list(col_ar.find(
        {"$or": [{"route_id": 2}, {"route_id": -2}]}))

    col_er = db_er[today_date] # ER
    rl_er = list(col_er.find(
        {"$or": [{"route_id": 2}, {"route_id": -2}]}))

    rl_rr = sorted(rl_rr, key=lambda i: (i['trip_id'], i['stop_id']))
    rl_pr_opt = sorted(rl_pr_opt, key=lambda i: (i['trip_id'], i['stop_id']))
    rl_ar = sorted(rl_ar, key=lambda i: (i['trip_id'], i['stop_id']))
    rl_er = sorted(rl_er, key=lambda i: (i['trip_id'], i['stop_id']))

    print(len(rl_rr), len(rl_pr_opt), len(rl_ar), len(rl_er))

    list_record = []

    for index in range(len(rl_pr_opt)):
        trip_id = rl_pr_opt[index]["trip_id"]
        stop_id = rl_pr_opt[index]["stop_id"]
        route_id = rl_pr_opt[index]["route_id"]
        rl_pr_opt[index].pop("_id", None)

        for non_index in range(len(rl_er)):
            if rl_er[non_index]["trip_id"] == trip_id and rl_er[non_index]["stop_id"] == stop_id:
                rl_pr_opt[index]["time_er_arr"] = rl_er[non_index]["time_er_arr"]
                rl_pr_opt[index]["time_er_alt"] = rl_er[non_index]["time_er_alt"]
                del rl_er[non_index]
                break
        
        for non_index in range(len(rl_ar)):
            if rl_ar[non_index]["trip_id"] == trip_id and rl_ar[non_index]["stop_id"] == stop_id:
                rl_pr_opt[index]["time_ar_arr"] = rl_ar[non_index]["rand_time"]
                del rl_ar[non_index]
                break
        
        for non_index in range(len(rl_rr)):
            if rl_rr[non_index]["trip_id"] == trip_id and rl_rr[non_index]["stop_id"] == stop_id:
                for walking_time in range(walking_time_limit):
                    rl_pr_opt[index]["time_rr_arr_" + str(walking_time)] = rl_rr[non_index]["time_smart_" + str(walking_time)]
                    rl_pr_opt[index]["time_rr_alt_" + str(walking_time)] = rl_rr[non_index]["time_alt_" + str(walking_time)]
                del rl_rr[non_index]
                break 
        
    
    print(today_date, " - Reduction finished.")
    
    print(len(rl_rr), len(rl_pr_opt), len(rl_ar), len(rl_er))
    col_diff = db_diff["MX_"+today_date]
    if len(rl_pr_opt)==0:
        return False
    col_diff.insert_many(rl_pr_opt)


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 30)

    # calculate_diff(start_date)
    # col_list_real_time = transfer_tools.daterange(start_date, end_date)

    cores = int(multiprocessing.cpu_count()/4*3)
    pool = multiprocessing.Pool(processes=cores)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(calculate_diff, date_range)
    pool.close()
    pool.join()

    # for i in col_list_real_time:
    #     calculate_diff(i)
