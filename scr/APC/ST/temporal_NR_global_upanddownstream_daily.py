
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
db_real_time = client.cota_apc_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_apc_pr_opt
db_ar = client.cota_apc_ar
db_er = client.cota_apc_er

db_diff = client.cota_apc_join
db_diff_reduce = client.cota_diff_reduce

walking_time_limit = 10  # min
criteria = 0  # seconds
designated_route_id = 2
the_stop_id = "HIGEUCN"

def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)

    stop_dic = {}
    for single_date in date_range:
        upstream = 0
        upstream_count = 0
        downstream = 0
        downstream_count = 0
        all = 0
        all_count = 0
        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]
        col_real_time = db_real_time[today_date]

        rl_opt_result = list(
            col_diff.find({"route_id": -2}))
            

        for each_record in rl_opt_result:
            stop_id = each_record["stop_id"]
            trip_id = each_record["trip_id"]
            try:
                stop_dic[trip_id]
            except:
                stop_dic[trip_id] = {}

            try:
                stop_dic[trip_id][stop_id]
            except:
                stop_dic[trip_id][stop_id] = {}
                rl_real_time = col_real_time.find_one({"stop_id": stop_id, "trip_id": trip_id})
                stop_dic[trip_id][stop_id]["stop_sequence"] = int(rl_real_time["stop_sequence"])
            
            try:
                stop_dic[trip_id][the_stop_id]
            except:
                stop_dic[trip_id][the_stop_id] = {}
                rl_real_time = col_real_time.find_one({"stop_id": the_stop_id, "trip_id": trip_id})
                if rl_real_time == None:
                    stop_dic[trip_id][the_stop_id]["stop_sequence"] = int(999)
                else:
                    stop_dic[trip_id][the_stop_id]["stop_sequence"] = int(rl_real_time["stop_sequence"])
        

            time_nr_alt = each_record["time_nr_alt"]
            try:
                time_nr_arr = int(each_record["time_nr_arr"])
            except:
                continue

            for i in range(10):
                try:
                    time_gr_alt = each_record["time_alt_" + str(i)]
                    time_gr_arr = each_record["time_smart_" + str(i)]

                except:
                    continue
            

                if type(time_gr_alt) is int and type(time_gr_arr) is int and time_gr_alt != 0 and time_gr_arr != 0 and type(time_nr_alt) is int and type(time_nr_arr) is int and time_nr_alt != 0 and time_nr_arr != 0:
                    if stop_dic[trip_id][stop_id]["stop_sequence"] is str or stop_dic[trip_id][the_stop_id]["stop_sequence"] is str:
                        print(stop_dic[trip_id][the_stop_id]["stop_sequence"], stop_dic[trip_id][stop_id]["stop_sequence"])
                        continue
                    else:
                        all += (time_gr_alt - time_gr_arr) - (time_nr_alt - time_nr_arr)
                        all_count += 1
                        # try:
                        if stop_dic[trip_id][stop_id]["stop_sequence"] < stop_dic[trip_id][the_stop_id]["stop_sequence"]:
                            upstream += (time_gr_alt - time_gr_arr) - (time_nr_alt - time_nr_arr)
                            upstream_count += 1
                        else:
                            downstream += (time_gr_alt - time_gr_arr) - (time_nr_alt - time_nr_arr)
                            downstream_count += 1
                        # except:
                        #     print(stop_dic[trip_id][stop_id]["stop_sequence"], stop_dic[trip_id][the_stop_id]["stop_sequence"])

        # print(len(rl_opt_result), upstream_count, downstream_count)
        if upstream_count != 0:
            print(today_date, upstream/upstream_count, upstream_count, downstream/downstream_count, downstream_count, all/all_count, all_count)
        else:
            print(0)

    


if __name__ == "__main__":
    start_date = date(2018, 5, 7)
    end_date = date(2019, 4, 30)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
