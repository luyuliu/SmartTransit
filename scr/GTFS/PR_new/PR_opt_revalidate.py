# Revalidation of PR optimal
# The result is store in the cota_pr_optimization_result database. Each collection's name is date + "pr_opt" 

from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import time
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_pr_validation
db_opt_finalization = client.cota_pr_finalization

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

# main loop
# enumerate every day in the range
walking_time_limit = 10
criteria = 5
designated_route = 2

def validate_stop_time(single_date):
    dic_stops = {}
    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3
        
    col_opt_result = db_opt_finalization["SEP_" + today_date]
    col_smart_transit = db_opt_result["SEP_" + today_date]
    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)

    col_real_time = db_real_time["R"+today_date]
    col_trip_update = db_trip_update[today_date]

    trips_collection = []

    pre_count = col_smart_transit.estimated_document_count()

    col_smart_transit.drop() # ------------------------------------------------------

    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
    db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
    db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
    db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
    col_real_time = db_real_time["R" + today_date]
    col_trip_update = db_trip_update[today_date]

    db_new_trips = client.cota_gtfs_new_trips[str(that_time_stamp) + "_new_trips"]

    rs_all_trips = list(db_new_trips.find({"service_id": str(
        service_id), "route_id": "002"}))  # Control the size of sampling
    count = 0
    total_count = len(rs_all_trips)
    realtime_error = 0

    print(today_date, total_count, ": start.")
    for single_trip in rs_all_trips:
        trip_id = single_trip["trip_id"]  # emurate rs_all_trips
        direction_id = int(single_trip["direction_id"])
        new_trip_id = single_trip["new_trip_id"]
        # route number: if direction_id=0 then tag=1; if direction_id=1 then tag=-1;
        route_id = int(single_trip["route_id"]) * (-direction_id*2+1)
        rs_all_stops = list(db_stop_times.find({"trip_id": trip_id}))
        rs_all_trip_update = list(col_trip_update.find(
            {"trip_id": trip_id}, no_cursor_timeout=True))  # All GTFS real-time feed
        
        expected_stop_times_list = {} # First key: stop, second key: T_cu. Value: T_ex
        for single_feed in rs_all_trip_update: 
            time_current = single_feed["ts"]
            for each_stop in single_feed["seq"]:
                try:
                    expected_stop_times_list[each_stop['stop']]
                except:
                    expected_stop_times_list[each_stop['stop']] = {}
                
                try:
                    expected_stop_times_list[each_stop['stop']][time_current]
                except:
                    expected_stop_times_list[each_stop['stop']][time_current] = each_stop["arr"]
                else:
                    expected_stop_times_list[each_stop['stop']][time_current] = each_stop["arr"]

        for single_stop_time in rs_all_stops: # Or you could directly query cota_real_time. But that could miss some stop since real-time data could miss some stop_time.
                                              # Another merit that we didn't directly query real-time is: we can save some time querying 
            stop_id = single_stop_time["stop_id"]  # query stop_times

            # ----------------Find the optimal IB (insurance buffer)---------------------
            opt_ib = (col_opt_result.find_one({"new_trip_id": new_trip_id, "stop_id": stop_id}))
            if (opt_ib) == None :
                opt_ib = "no_record"
            # ----------------Find the optimal IB (insurance buffer)---------------------

            line = {}
            a_stop = (db_stops.find_one({"stop_id": stop_id}))
            if a_stop == None:
                continue
            line['lat'] = a_stop['stop_lat']
            line['lon'] = a_stop['stop_lon']
            line['trip_id'] = trip_id
            line['stop_id'] = stop_id
            line['route_id'] = route_id
            # normal users' actual boarding time (bus's actual boarding time)
            line["time_actual"] = 0
            # normal users' expected boarding time (bus's scheduled boarding time)
            line["time_normal"] = 0
            for time_walking in range(walking_time_limit):
                # smart users' actual boarding time
                line["time_alt_" + str(time_walking)] = 0
                # smart users' expected boarding time
                line["time_smart_" + str(time_walking)] = 0

            ### Time Normal: Time for normal transit users, aka scheduled time follower ###
            line["time_normal"] = transfer_tools.convert_to_timestamp(
                single_stop_time["arrival_time"], single_date)  # schedule

            ### Time Actual: Time for actual transit arrival time, which is the last time you should be ###
        
            # This list is the alt trips' actual departure time. The sequence of this is the temporal sequence of trip sequence array in the SCHEDULE.
            # However, the order could be not strictly in temporal order.
            
            alt_times_list = []
            real_time = -1
            alt_trips_list = list(col_real_time.find({"stop_id": stop_id, "route_id": route_id}))
            for each_trip in alt_trips_list:
                i_real_time = each_trip["time"]
                alt_times_list.append(i_real_time)
                if each_trip["trip_id"] == trip_id:
                    real_time = i_real_time
            if real_time == -1: 
                line["time_actual"] = "no_realtime_trip"
                # print("no_realtime_trip: ", stop_id, trip_id, route_id, that_time_stamp)
                realtime_error += 1
                continue
            line["time_actual"] = real_time

            for time_walking in range(walking_time_limit):
                if opt_ib == "no_record":
                    buffer = 120
                else:
                    buffer = opt_ib["max_buff_" + str(time_walking)]
                ### Time Smart: Time for smart transit users' arrival time at the receiving stop ###
                # past_predicted_time + walking_time
                line["time_smart_" + str(time_walking)] = 0
                line["buffer_" + str(time_walking)] = int(buffer)
                time_current = 0
                time_feed = -1

                # The simulation process of finding pathes in RTA. To find the suffcient current time to leave.
                for time_current, time_feed in expected_stop_times_list[stop_id].items():
                    # If the current ETA plus the Insurance buffer is equal to or greater than the buses' ETA, then go.
                    if time_current + time_walking*60 + buffer >= time_feed:
                        line["time_smart_" +
                             str(time_walking)] = time_current + time_walking*60
                        break

                if time_current + time_walking*60 + buffer >= time_feed:
                    line["time_smart_" +
                         str(time_walking)] = time_current + time_walking*60
                if line["time_smart_" + str(time_walking)] == 0:
                    line["time_smart_" +
                         str(time_walking)] == "cannot_find_smart"
                    # print("cannot_find_smart")
                    continue
                ### Time Smart: Time for smart transit users' arrival time at the receiving stop ###

                ### Time Alt: Time for smart transit user's actual onboard time ###

                line["time_alt_" + str(time_walking)] = 9999999999

                for i_real_time in alt_times_list:
                    # relaxed
                    if line["time_alt_" + str(time_walking)] > i_real_time and i_real_time >= line["time_smart_" + str(time_walking)] - criteria:
                        line["time_alt_" + str(time_walking)] = i_real_time

                if line["time_alt_" + str(time_walking)] == 9999999999:
                    # there's no an alternative trip.
                    line["time_alt_" + str(time_walking)] = "critical_trip"

            trips_collection.append(line)
            
            if len(trips_collection)!= 0:
                col_smart_transit.insert_many(trips_collection)
                trips_collection = []

        # print(today_date, count, total_count, realtime_error, len(rs_all_stops), route_id)
        if round(count/total_count*100, 0) % 20 == 5:
            print(today_date, route_id, ": ", int(count /
                  total_count*100), " % finished.")
        count += 1

    if len(trips_collection)!= 0:
        col_smart_transit.insert_many(trips_collection)
        trips_collection = []
    print(today_date, ": finished.")
    


if __name__ == '__main__':
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=25)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(validate_stop_time, date_range)
    pool.close()
    pool.join()
        


