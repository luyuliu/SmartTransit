
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import transfer_tools

from itertools import chain

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization
db_opt_result = client.cota_pr_optimization_test
db_ar = client.cota_ar
db_er = client.cota_er

db_diff = client.cota_diff
db_diff_reduce = client.cota_diff_reduce

walking_time_limit = 10 # min
criteria = 5 # seconds
designated_route_id = 2

def reduce_diff(start_date, end_date):
    date_range =transfer_tools.daterange(start_date, end_date)

    dic_stops = {}
    insurance_buffers = range(0, 3601, 10)
    
    for insurance_buffer in insurance_buffers:
        count = 0
        sumTime = 0
        early_desyn_count = 0
        late_desyn_count = 0

        sum_time_desyn = 0
        date_range =transfer_tools.daterange(start_date, end_date)
        for single_date in date_range:
            today_date = single_date.strftime("%Y%m%d")  # date
            col_diff = db_diff[today_date]

            that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
            col_opt_result = db_opt_result[today_date + "_" + str(insurance_buffer) + "_test"]
            # print(today_date + "_" + str(insurance_buffer))
            rl_opt_result = list(col_opt_result.find({"stop_id": "HANMAIN", "route_id": -2}))
            total_count = len(rl_opt_result)*10

            for each_record in rl_opt_result:
                time_actual = each_record["time_actual"]
                time_normal = each_record["time_normal"]
                for i in range(10):
                    time_alt = each_record["time_alt_" + str(i)]
                    time_smart = each_record["time_smart_" + str(i)]
                    # if each_record["trip_id"] == "616909" and i == 9: # This means the insurance buffer dones't work on certain trip
                        # print(insurance_buffer, time_smart - time_actual, time_alt-time_actual, each_record["trip_id"])
                    # print(time_alt)
                    if type(time_alt) is int:
                        count +=1
                        sumTime+=time_alt - time_smart
                        if time_alt < time_actual:
                            early_desyn_count+=1
                        if time_alt > time_actual:
                            late_desyn_count+=1
                            sum_time_desyn += time_alt - time_smart
                            # print(time_smart - time_actual, time_actual-1517500000, time_smart-1517500000, each_record["trip_id"])
                        # if time_actual<time_normal:
                            # print(time_actual-time_normal)
            
        print(insurance_buffer, ": ", sumTime/count, sumTime, sum_time_desyn)



if __name__ == "__main__":
    start_date = date(2018, 2,1)
    end_date = date(2018,2, 2)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason