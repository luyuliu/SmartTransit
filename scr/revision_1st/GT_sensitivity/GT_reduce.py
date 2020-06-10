
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
db_smart_transit = client.cota_re_pr_cal
db_opt_result = client.cota_re_gt_cal
db_ar = client.cota_apc_ar
db_er = client.cota_apc_er
db_nr = client.cota_apc_nr
db_diff_reduce = client.cota_re_reduce

walking_time_limit = 10 # min
the_route_id = 2
error_criteria = 5

all_route = [1, 2, 5, 7, 8, 10]

def reduce_diff(start_date, end_date, criteria):
    date_range =transfer_tools.daterange(start_date, end_date)

    dic_stops = {}
    for single_date in date_range:
        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_opt_result[today_date + "_" + str(criteria)]
        
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

                for time_walking in range(walking_time_limit):
                    line["miss_c_"+str(time_walking)] = 0

                    # Waiting time. WT = actual bus departure time - arrival time.
                    line["wt_rr_"+str(time_walking)] = 0 
                    
                    # Delay time. Delay = actual bus departure time - scheduled time
                    line["dt_rr_"+str(time_walking)] = 0 
                    
                    # Miss count.
                    line["mc_rr_"+str(time_walking)] = 0

                    # Buffer
                    line["buffer_pr_opt_"+str(time_walking)] = 0

                line["total"] = 0  # Total count.
                dic_stops[stop_id] = line
            
            
            # RR and PR optimal
            for time_walking in range(walking_time_limit):
                pr_opt_pass_token = False
                rr_pass_token = False

                # RR

                try:
                    if criteria == 0:
                        time_alt = single_stop_time["time_alt_"+str(time_walking)]
                    else:
                        time_alt = single_stop_time["time_ralt_"+str(time_walking)]
                    time_arr = single_stop_time["time_smart_"+str(time_walking)]
                except:
                    continue
                if time_alt == 0 or time_arr == 0:
                    rr_pass_token = True
                if time_alt == -1 or time_alt == 9999999999:
                    #print("Doomed: ", stop_id, trip_id)
                    rr_pass_token = True
                try:
                    wt_rr = time_alt - time_arr
                    dt_rr = time_alt - time_actual
                except:
                    rr_pass_token = True
                else:
                    if wt_rr<-error_criteria or wt_rr>86400 or rr_pass_token == True:
                        error_tag = True
                    else:
                        dic_stops[stop_id]["wt_rr_"+str(time_walking)] += wt_rr
                        dic_stops[stop_id]["dt_rr_"+str(time_walking)] += dt_rr
                        if time_alt > time_actual:
                            dic_stops[stop_id]["mc_rr_"+str(time_walking)]+=1

            dic_stops[stop_id]["total"] += 1
            if error_tag:
                error_count += 1
        # print(today_date, " - Done.")

    # print(today_date, " - Insert start.")
    db_result_route = db_diff_reduce["RE_GT_sensitive_" + str(the_route_id)+ "_" + str(criteria)]
    all_wt = 0
    all_count = 0
    miss_count = 0
    for key, value in dic_stops.items():
        all_count += value['total']
        for time_walking in range(walking_time_limit):
            all_wt += value['wt_rr_'+str(time_walking)]
            miss_count += value['mc_rr_'+str(time_walking)]
            value['wt_rr_'+str(time_walking)] = value['wt_rr_'+str(time_walking)]/value['total']
            value['dt_rr_'+str(time_walking)] = value['dt_rr_'+str(time_walking)]/value['total']
            
        db_result_route.insert_one(value)
    # print(today_date, " - Insert done.")
    print(all_wt, all_count, miss_count, criteria)


if __name__ == "__main__":
    start_date = date(2018, 5, 7)
    end_date = date(2019, 5, 6)
    criterias = range(1, 16)
    for criteria in criterias:
        reduce_diff(start_date, end_date, criteria)

    # Todo: find skip reason