
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')
sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))
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
db_delay_reclamation = client.cota_delay_reclamation

walking_time_limit = 10  # min
criteria = 5  # seconds
designated_route_id = 2

def sortKey(A):
    return A["stop_sequence"]

def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)
    dics = {}


    this_true_count = 0
    this_total_count = 0

    for single_date in date_range:
        dic_stops = {}
        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]
        today_weekday = single_date.weekday()
        col_delay_reclamation = db_delay_reclamation[today_date +"_60"]

        that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
        # rl_opt_result = list(
        #     db_real_time['R' + today_date].find({"$or":[{"route_id": 2}, {"route_id": -2}]}))
        rl_opt_result = list(
            col_delay_reclamation.find({}))
        db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
        # db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
        db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
        # db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
        # col_real_time = db_real_time["R" + today_date]
        # col_trip_update = db_trip_update[today_date]

        this_delay = 0
        for each_record in rl_opt_result:
            stop_id = each_record["stop_id"]
            trip_id = each_record["trip_id"]
            time_actual = each_record["time_actual"]
            time_normal = each_record["time_normal"]
            
            for time_walking in range(10):
                time_smart = each_record["time_smart_" + str(time_walking)]
                try:
                    time_actual_then = each_record["time_actual_then_" + str(time_walking)]
                    time_normal_then = each_record["time_normal_then_" + str(time_walking)]
                except:
                    continue

                if type(time_actual_then) is int and type(time_normal_then) is int and time_actual_then != 0 and time_normal_then != 0:
                    delay_reclamation = (time_actual - time_normal) - (time_actual_then - time_normal_then) # If reclaimed, now delay should be smaller than then. Then <0
                    time_delta = time_actual - time_smart # If missed, arrival time is late/larger than bus time. Then <0

                    if delay_reclamation <0 :# Reclaim
                        if time_delta < 0:# Miss a bus
                            boolean = True
                            this_true_count +=1
                        else:
                            boolean = False
                        this_total_count += 1
        print(this_true_count , this_total_count , this_true_count/this_total_count*100)
                        


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
