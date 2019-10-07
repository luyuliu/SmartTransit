
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


    col_delay_reclamation = db_delay_reclamation["-2_delay_reclamation"]

    for single_date in date_range:
        dic_stops = {}
        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]
        today_weekday = single_date.weekday()

        that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
        # rl_opt_result = list(
        #     db_real_time['R' + today_date].find({"$or":[{"route_id": 2}, {"route_id": -2}]}))
        rl_opt_result = list(
            db_real_time['R' + today_date].find({"route_id": -2}))
        db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
        # db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
        db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
        # db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
        # col_real_time = db_real_time["R" + today_date]
        # col_trip_update = db_trip_update[today_date]

        this_error_count = 0
        this_total_count = 0
        this_delay = 0
        for each_record in rl_opt_result:
            stop_id = each_record["stop_id"]
            trip_id = each_record["trip_id"]
            time_actual = each_record["time"]
            time_normal = each_record["scheduled_time"]
            try:
                dic_stops[trip_id]
            except:
                dic_stops[trip_id] = []
            
            line = {}
            line["stop_id"] = stop_id
            line["stop_sequence"] = each_record["stop_sequence"]
            line["delay"] = 0
            line["count"] = 0
            line["lat"] = each_record["lat"]
            line["lon"] = each_record["lon"]
            

            if type(time_actual) is not int or type(time_normal) is not int:
                # print(time_actual, time_normal)
                continue
            line["delay"] += time_actual - time_normal
            line["count"] += 1

            dic_stops[trip_id].append(line) 

            this_delay += time_actual - time_normal
        
        # for trip_id, content in dic_stops.items():
        #     try:
        #         dics[trip_id]
        #     except:
        #         dics[trip_id] = {}
        #         stop_times_query = list(db_stop_times.find({"trip_id" : trip_id}))
        #         for each_stop in stop_times_query:
        #             stop_id_e = each_stop["stop_id"]
        #             dics[trip_id][stop_id_e]["stop_sequence"] = each_stop["stop_sequence"]
        #             dics[trip_id][stop_id_e]["delay_reclamation"] = 0
        #             dics[trip_id][stop_id_e]["count"] = 0
                
        #     content.sort(key = sortKey)
        #     count_id = 0
        #     last_content = None
        #     for stop_content in content:
        #         stop_id = stop_content["stop_id"]
        #         if last_content == None:
        #             last_content = stop_content
        #             continue
        #         dics[trip_id][stop_id]["delay_reclamation"] += stop_content["delay"] - last_content["delay"]
        #         dics[trip_id][stop_id]["delay_reclamation_count"] += 1
        #         count_id+=1

        for trip_id, content in dic_stops.items():                
            content.sort(key = sortKey)
            count_id = 0
            last_content = None
            for stop_content in content:
                stop_id = stop_content["stop_id"]
                try:
                    dics[stop_id]
                except:
                    dics[stop_id] = {}
                    dics[stop_id]["total_count"] = 0
                    dics[stop_id]["delay_reclamation_count"] = 0 
                    dics[stop_id]["stop_id"] = stop_id
                    query_stop = db_stops.find_one({"stop_id": stop_id})
                    dics[stop_id]["stop_lat"] =  query_stop["stop_lat"]
                    dics[stop_id]["stop_lon"] =  query_stop["stop_lon"]

                if last_content == None:
                    last_content = stop_content
                    continue
                if stop_content["delay"] - last_content["delay"]<0:
                    this_error_count+=1
                    dics[stop_id]["delay_reclamation_count"] += 1
                this_total_count+=1
                dics[stop_id]["total_count"] += 1
                count_id+=1

               
        print(single_date, this_error_count, this_total_count)

    for stop_id, stop_content in dics.items():
        col_delay_reclamation.insert_one(stop_content)



if __name__ == "__main__":
    start_date = date(2018, 1, 31)
    end_date = date(2019, 2, 1)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
