from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update


def appendix_real_time(single_date):
    stop_dic = {}
    trip_dic = {}

    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3

    if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
        summer_time = 0
    else:
        summer_time = 1
    today_seconds = int((single_date - date(1970, 1, 1)).total_seconds()) + 18000 + 3600*summer_time
    print(today_seconds)
    print(today_date, ": Start.")

    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
    db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
    db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
    db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
    col_trip_update = db_trip_update[today_date]

    col_real_time = db_real_time["R" + today_date]
    rl_real_time = list(col_real_time.find({}))
    total_count = col_real_time.estimated_document_count()
    count = 0
    for each_record in rl_real_time:
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]
        rid = each_record["_id"]
        try:
            stop_dic[stop_id]
        except:
            stop_dic[stop_id] = {}
            stop_query = list(db_stops.find({"stop_id": stop_id}))
            if len(stop_query) == 0:
                stop_dic[stop_id]["lat"] = "stop_error"
                stop_dic[stop_id]["lon"] = "stop_error"
                stop_dic[stop_id]["stop_name"] = "stop_error"
                stop_dic[stop_id]["stop_code"] = "stop_error"
            else:
                stop_query = stop_query[0]
                stop_dic[stop_id]["lat"] = stop_query["stop_lat"]
                stop_dic[stop_id]["lon"] = stop_query["stop_lon"]
                stop_dic[stop_id]["stop_name"] = stop_query["stop_name"]
                stop_dic[stop_id]["stop_code"] = stop_query["stop_code"]

        try:
            trip_dic[trip_id]
        except:
            trip_dic[trip_id] = {}
            trip_query = list(db_trips.find({"trip_id": trip_id}))
            if len(trip_query) == 0:
                trip_dic[trip_id]['block_id'] = "trip_error"
                trip_dic[trip_id]['shape_id'] = "trip_error"
                trip_dic[trip_id]['trip_headsign'] = "trip_error"
                trip_dic[trip_id]['direction_id'] = "trip_error"
                trip_dic[trip_id]['route_id'] = "trip_error"
            else:
                trip_query = trip_query[0]
                trip_dic[trip_id]['block_id'] = trip_query["block_id"]
                trip_dic[trip_id]['shape_id'] = trip_query["shape_id"]
                trip_dic[trip_id]['trip_headsign'] = trip_query["trip_headsign"]
                trip_dic[trip_id]['direction_id'] = int(
                    trip_query["direction_id"])
                trip_dic[trip_id]['route_id'] = int(
                    trip_query["route_id"])*(1-2*int(trip_query["direction_id"]))

        stop_times_query = list(db_stop_times.find(
            {"trip_id": trip_id, "stop_id": stop_id}))
        if len(stop_times_query) == 0:
            stop_sequence = "stop_time_error"
        else:
            stop_times_query = stop_times_query[0]
            stop_sequence = stop_times_query["stop_sequence"]

        trip_seq_query = list(db_seq.find(
            {"trip_id": trip_id, "stop_id": stop_id}))
        if len(trip_seq_query) == 0:
            pass
        else:
            trip_seq_query = trip_seq_query[0]
            trip_sequence = trip_seq_query["seq"]
            scheduled_time = int(trip_seq_query["time"]) + today_seconds

        original_query = {"_id": rid}
        update_object = {"$set": {
            "lat": stop_dic[stop_id]["lat"],
            "lon": stop_dic[stop_id]["lon"],
            "stop_name": stop_dic[stop_id]["stop_name"],
            "shape_id":  trip_dic[trip_id]['shape_id'],
            "trip_headsign":  trip_dic[trip_id]['trip_headsign'],
            "route_id": trip_dic[trip_id]['route_id'],
            "stop_sequence": stop_sequence,
            "trip_sequence": trip_sequence,
            "scheduled_time": scheduled_time
        }}

        col_real_time.update_one(original_query, update_object)
        count += 1
        if count % 1000 == 1:
            print(today_date, ": ", count/total_count*100)
    print(today_date, ": Start.")


if __name__ == "__main__":
    start_date = date(2018, 1, 29)
    end_date = date(2018, 1, 30)

    appendix_real_time(start_date)
    # col_list_real_time = transfer_tools.daterange(start_date, end_date)

    # cores = int(multiprocessing.cpu_count()/3*2)
    # pool = multiprocessing.Pool(processes=cores)
    # date_range = transfer_tools.daterange(start_date, end_date)
    # output = []
    # output = pool.map(appendix_real_time, date_range)
    # pool.close()
    # pool.join()
