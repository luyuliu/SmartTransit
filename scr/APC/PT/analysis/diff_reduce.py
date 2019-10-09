
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
db_smart_transit = client.cota_apc_pr_cal
db_opt_result = client.cota_apc_pr_opt
db_ar = client.cota_apc_ar
db_er = client.cota_apc_er
db_nr = client.cota_apc_nr
db_diff_reduce = client.cota_diff_reduce

db_diff = client.cota_apc_join

walking_time_limit = 10 # min
criteria = 5 # seconds
designated_route_id = -2

def reduce_diff(start_date, end_date):
    date_range =transfer_tools.daterange(start_date, end_date)

    dic_stops = {}
    for single_date in date_range:
        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]

        that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)

        rl_diff = list(col_diff.find({"route_id": designated_route_id}))

        count=0
        error_count = 0
        total_count= len(rl_diff)

        for single_stop_time in rl_diff:
            error_tag = False
            count = count + 1
            trip_id = single_stop_time["trip_id"]  # emurate rs_all_trips
            stop_id = single_stop_time["stop_id"]
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
                    line["wt_pr_opt_"+str(time_walking)] = 0
                    
                    # Delay time. Delay = actual bus departure time - scheduled time
                    line["dt_rr_"+str(time_walking)] = 0 
                    line["dt_pr_opt_"+str(time_walking)] = 0
                    
                    # Miss count.
                    line["mc_rr_"+str(time_walking)] = 0
                    line["mc_pr_opt_"+str(time_walking)] = 0

                    # Buffer
                    line["buffer_pr_opt_"+str(time_walking)] = 0
                
                line["wt_er"] = 0
                line["wt_ar"] = 0
                line["wt_nr"] = 0

                line["mc_er"] = 0 # Miss count.
                line["mc_nr"] = 0

                line["dt_er"] = 0

                line["total"] = 0  # Total count.
                dic_stops[stop_id] = line
            
            # ER
            try:
                wt_er = single_stop_time["time_er_alt"] - single_stop_time["time_er_arr"]
                dt_er = single_stop_time["time_er_alt"] - time_normal
                if single_stop_time["time_er_alt"]>time_actual:
                    dic_stops[stop_id]["mc_er"] += 1
            except:
                pass
            else:
                if wt_er<-criteria or wt_er>86400:
                    error_tag = True
                else:
                    dic_stops[stop_id]["wt_er"] += wt_er
                    dic_stops[stop_id]["dt_er"] += dt_er

            # AR
            try:
                wt_ar =  time_actual - single_stop_time["time_ar_arr"] # dt_ar is always equal to wt_ar
            except:
                pass
            else:
                if wt_ar<-criteria or wt_ar>86400:
                    error_tag = True
                else:
                    dic_stops[stop_id]["wt_ar"] += wt_ar

            # NR
            try:
                wt_nr = single_stop_time["time_nr_alt"] - single_stop_time["time_nr_arr"]
                if single_stop_time["time_nr_alt"]>time_actual:
                    dic_stops[stop_id]["mc_nr"] += 1
            except:
                pass
            else:
                if wt_nr<-criteria or wt_nr>86400:
                    error_tag = True
                else:
                    dic_stops[stop_id]["wt_nr"] += wt_nr


            # RR and PR optimal
            for time_walking in range(walking_time_limit):
                pr_opt_pass_token = False
                rr_pass_token = False

                # PR optimal
                time_alt = single_stop_time["time_alt_"+str(time_walking)]
                time_arr = single_stop_time["time_smart_"+str(time_walking)]
                if time_alt == 0 or time_arr == 0:
                    pr_opt_pass_token = True
                if time_alt == -1 or time_alt == 9999999999:
                    #print("Doomed: ", stop_id, trip_id)
                    pr_opt_pass_token = True
                try:
                    wt_pr_opt = time_alt - time_arr
                    dt_pr_opt = time_alt - time_actual
                except:
                    pr_opt_pass_token = True
                else:
                    if wt_pr_opt<-criteria or wt_pr_opt>86400 or pr_opt_pass_token == True:
                        error_tag = True
                    # if wt_pr_opt > 300:
                    #     print(wt_pr_opt, trip_id, stop_id, time_walking)
                    else:
                        dic_stops[stop_id]["wt_pr_opt_"+str(time_walking)] += wt_pr_opt
                        dic_stops[stop_id]["dt_pr_opt_"+str(time_walking)] += dt_pr_opt
                        if time_alt > time_actual:
                            dic_stops[stop_id]["mc_pr_opt_"+str(time_walking)]+=1
                        dic_stops[stop_id]["buffer_pr_opt_"+str(time_walking)] += single_stop_time["buffer_"+str(time_walking)]
                
                # RR
                try:
                    time_alt = single_stop_time["time_rr_alt_"+str(time_walking)]
                    time_arr = single_stop_time["time_rr_arr_"+str(time_walking)]
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
                    if wt_rr<-criteria or wt_rr>86400 or rr_pass_token == True:
                        error_tag = True
                    else:
                        dic_stops[stop_id]["wt_rr_"+str(time_walking)] += wt_rr
                        dic_stops[stop_id]["dt_rr_"+str(time_walking)] += dt_rr
                        if time_alt > time_actual:
                            dic_stops[stop_id]["mc_rr_"+str(time_walking)]+=1

            dic_stops[stop_id]["total"] += 1
            if error_tag:
                error_count += 1
        print(today_date, " - Done.")

    print(today_date, " - Insert start.")
    db_result_route = db_diff_reduce["APC_" + str(designated_route_id)]
    for key, value in dic_stops.items():
        value["wt_nr"] = value['wt_nr']/value['total']
        value["wt_er"] = value['wt_er']/value['total']
        value["dt_er"] = value['dt_er']/value['total']
        value["wt_ar"] = value['wt_ar']/value['total']
        for time_walking in range(walking_time_limit):
            value['wt_rr_'+str(time_walking)] = value['wt_rr_'+str(time_walking)]/value['total']
            value['dt_rr_'+str(time_walking)] = value['dt_rr_'+str(time_walking)]/value['total']
            
            value['wt_pr_opt_'+str(time_walking)] = value['wt_pr_opt_'+str(time_walking)]/value['total']
            value['dt_pr_opt_'+str(time_walking)] = value['dt_pr_opt_'+str(time_walking)]/value['total']
            value['buffer_pr_opt_'+str(time_walking)] = value['buffer_pr_opt_'+str(time_walking)]/value['total']
        db_result_route.insert_one(value)
    print(today_date, " - Insert done.")


if __name__ == "__main__":
    start_date = date(2018, 5, 8)
    end_date = date(2019, 5, 7)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason