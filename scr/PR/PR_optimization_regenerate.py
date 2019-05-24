# Optimization of PR.
# Calculate the average IB for a whole year. Get parameters for PR optimal.
# The result is store in the cota_pr_optimization_result database. Each collection's name is date + "pr_opt" 

from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_pr_optimization_result

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

# main loop
# enumerate every day in the range
walking_time_limit = 10
buffer = 120
criteria = 5
designated_route = 2

def analyze_transfer(single_date):
    dic_stops = {}
    if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
        summer_time = 0
    else:
        summer_time = 1
    today_date = single_date.strftime("%Y%m%d")  # date
    db_today_smart_transit = transfer_tools.db_smart_transit[today_date]

    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)

    rs_all_stop_time = list(db_today_smart_transit.find({}))

    count=0
    total_count= len(rs_all_stop_time)


    for single_stop_time in rs_all_stop_time:
        if count % 100 == 1:
            print(count/total_count)
        count = count + 1
        trip_id = single_stop_time["trip_id"]  # emurate rs_all_trips
        stop_id = single_stop_time["stop_id"]
        # query stop_times
        try:
            dic_stops[stop_id]
        except:
            line = {}
            line['lat'] = single_stop_time['lat']
            line['lon'] = single_stop_time['lon']

            for time_walking in range(walking_time_limit):
                line["miss_c_"+str(time_walking)] = 0
                line['wt_dif_'+str(time_walking)] = 0 # wait_difference, the difference between waiting time of two scenarios
                line['dt_dif_'+str(time_walking)] = 0 # depart_difference, the difference between departure time of two scenarios

            line["totl_c"] = 0  # TOTAL COUNT
            dic_stops[stop_id] = line
        
        time_actual = (single_stop_time["time_actual"]) # Time for actual transit arrival time, which is the last time you should be 
        time_normal = (single_stop_time["time_normal"]) # Time for normal transit users, aka scheduled time follower
        for time_walking in range(walking_time_limit):
            time_alt = (single_stop_time["time_alt_"+str(time_walking)]) # Time for smart transit user's actual onboard time
            time_smart = (single_stop_time["time_smart_"+str(time_walking)]) # Time for smart transit users' arrival time at the receiving stop
            
            try:
                wait_diff = (float(time_alt) - float(time_smart)) - (float(time_actual) - float(time_normal))
                depart_diff = float(time_alt) - float(time_actual)
            except:
                wait_diff = "no_result"
                depart_diff = "no_result"
                continue
            
            if time_alt == -1 or time_alt == 9999999999:
                #print("Doomed: ", stop_id, trip_id)
                continue
            if time_smart > time_actual:
                dic_stops[stop_id]["miss_c_"+str(time_walking)]+=1
            dic_stops[stop_id]['wt_dif_'+str(time_walking)] += wait_diff
            dic_stops[stop_id]['dt_dif_'+str(time_walking)] += depart_diff
        dic_stops[stop_id]["totl_c"] += 1
    
    db_result_route = db_opt_result[str(designated_route)]               
    for key, value in dic_stops.items():
        for time_walking in range(walking_time_limit):
            value['wt_dif_'+str(time_walking)] = value['wt_dif_'+str(time_walking)]/value['totl_c']
            value['dt_dif_'+str(time_walking)] = value['dt_dif_'+str(time_walking)]/value['totl_c']
        db_result_route.insert_one(value)
        


if __name__ == '__main__':
    start_date1 = date(2018, 2, 1)
    end_date1 = date(2018, 1, 31)
    
    date_range = transfer_tools.daterange(start_date, end_date)
        raw_route_id = int(each_route["route_id"])
        
        analyze_transfer(start_date1, end_date1, raw_route_id)
        analyze_transfer(start_date1, end_date1, -raw_route_id)

        


