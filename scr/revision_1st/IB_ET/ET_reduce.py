
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import transfer_tools

db_GTFS = client.cota_gtfs
db_real_time = client.cota_apc_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_re_er_val
db_ar = client.cota_apc_ar
db_er = client.cota_apc_er
db_nr = client.cota_apc_nr
db_diff_reduce = client.cota_re_reduce

the_route_id = -2
error_criteria = 5

all_route = [1, 2, 5, 7, 8, 10]

def reduce_diff(start_date, end_date):
    date_range =transfer_tools.daterange(start_date, end_date)

    dic_stops = {}
    for single_date in date_range:
        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_opt_result["REV_" + today_date]
        
        that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)

        rl_diff = list(col_diff.find({}))

        count=0
        error_count = 0
        total_count= len(rl_diff)

        visit_tag = False
        first_stop_id = 0
        first_trip_id = 0
        for single_stop_time in rl_diff:

            error_tag = False
            count = count + 1
            stop_id = single_stop_time["stop_id"]
            trip_id = single_stop_time["trip_id"]
            if stop_id == first_stop_id and trip_id == first_trip_id:
                break
            if visit_tag == False:
                visit_tag = True
                first_stop_id = stop_id
                first_trip_id = trip_id
            time_normal = single_stop_time["time_normal"]
            time_actual = single_stop_time["time_actual"]
            # query stop_times
            try:
                dic_stops[stop_id]
            except:
                line = {}
                line['stop_id'] = single_stop_time['stop_id']
                line['lat'] = single_stop_time['lat']
                line['lon'] = single_stop_time['lon']

                line["miss_c"] = 0

                # Waiting time. WT = actual bus departure time - arrival time.
                line["wt_er"] = 0 
                
                # Delay time. Delay = actual bus departure time - scheduled time
                line["dt_er"] = 0 
                
                # Miss count.
                line["mc_er"] = 0

                # Buffer
                line["buffer_er_opt"] = 0

                line["total"] = 0  # Total count.
                dic_stops[stop_id] = line
            
            
            # RR and PR optimal
            er_opt_pass_token = False
            rr_pass_token = False

            # RR

            try:
                time_alt = single_stop_time["time_alt"]
                time_arr = single_stop_time["time_smart"]
            except:
                continue
            if time_alt == 0 or time_arr == 0:
                rr_pass_token = True
            if time_alt == -1 or time_alt == 9999999999:
                #print("Doomed: ", stop_id, trip_id)
                rr_pass_token = True
            try:
                wt_er = time_alt - time_arr
                dt_er = time_alt - time_actual
            except:
                rr_pass_token = True
            else:
                if wt_er<-error_criteria or wt_er>86400 or rr_pass_token == True:
                    error_tag = True
                else:
                    dic_stops[stop_id]["wt_er"] += wt_er
                    dic_stops[stop_id]["dt_er"] += dt_er
                    if time_alt > time_actual:
                        dic_stops[stop_id]["mc_er"]+=1

            dic_stops[stop_id]["total"] += 1
            if error_tag:
                error_count += 1
        # print(today_date, " - Done.")

    # print(today_date, " - Insert start.")
    db_result_route = db_diff_reduce["RE_ER_buffer_" + str(the_route_id)]
    all_wt = 0
    all_count = 0
    miss_count = 0
    for key, value in dic_stops.items():
        all_count += value['total']
        all_wt += value['wt_er']
        miss_count += value['mc_er']
        if value["total"] == 0:
            continue
        value['wt_er'] = value['wt_er']/value['total']
        value['dt_er'] = value['dt_er']/value['total']
            
        db_result_route.insert_one(value)
    # print(today_date, " - Insert done.")
    print(all_wt, all_count, miss_count)


if __name__ == "__main__":
    start_date = date(2018, 5, 7)
    end_date = date(2019, 5, 6)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason