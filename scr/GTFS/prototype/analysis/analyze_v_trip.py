import shapefile
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_smart_transit


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


db_time_stamps_set = set()
db_time_stamps = []
raw_stamps = db_GTFS.list_collection_names()
for each_raw in raw_stamps:
    each_raw = int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()


def find_gtfs_time_stamp(single_date):
    today_seconds = int(
        (single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup = db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds > 86400:
            return backup
        backup = each_time_stamp
    return db_time_stamps[len(db_time_stamps) - 1]


def convert_to_timestamp(time_string, single_date, summer_time):
    time = time_string.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    total_second = hours * 3600 + minutes * 60 + seconds

    today_seconds = int(
        (single_date - date(1970, 1, 1)).total_seconds()) + 18000 - summer_time*3600

    return total_second+today_seconds

def sortQuery(A):
    return A["seq"]


# main loop
# enumerate every day in the range
designated_routes = [1]
walking_time_limit = 10
buffer = 120
criteria = 5


def analyze_transfer(single_date):
    dic_stops = {}
    if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
        summer_time = 0
    else:
        summer_time = 1
    today_date = single_date.strftime("%Y%m%d")  # date
    
    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3
    db_today_smart_transit = db_smart_transit[today_date]

    that_time_stamp = find_gtfs_time_stamp(single_date)
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
    db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
    db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
    db_seq=db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_today_real_time = db_real_time["R" + today_date]
    db_today_trip_update = db_trip_update[today_date]

    for route_id in designated_routes:
        rs_all_trips = list(db_trips.find(
            {"route_id": "{:03d}".format(route_id)}))

        count = 0
        total_count = len(rs_all_trips)
        for single_trip in rs_all_trips:
            print(today_date, count,"/",total_count)
            count += 1
            trip_id = single_trip["trip_id"]  # emurate rs_all_trips

            rs_all_stops = list(db_stop_times.find({"trip_id": trip_id}))

            for single_stop_time in rs_all_stops:
                stop_id = single_stop_time["stop_id"]  # query stop_times
                try:
                    dic_stops[stop_id]
                except:
                    line = {}
                    a_stop = list(db_stops.find({"stop_id": stop_id}))
                    if a_stop == []:
                        continue
                    else:
                        a_stop = a_stop[0]
                    line['lat'] = a_stop['stop_lat']
                    line['lon'] = a_stop['stop_lon']
                    line['trip_id'] = trip_id
                    line['stop_id'] = stop_id
                    line['route_id'] = route_id
                    line['buffer'] = buffer
                    line['diff_time'] = 0
                    for time_walking in range(walking_time_limit):
                        line["miss_count_" + str(time_walking)] = 0
                        line["diff_time_" + str(time_walking)] = 0
                    
                    line["total_count"] = 0  # TOTAL COUNT
                    dic_stops[stop_id] = line

                for time_walking in range(walking_time_limit):
                    time_smart = 999  # past_predicted_time + walking_time

                    rs_all_trip_update = db_today_trip_update.find(
                        {"trip_id": trip_id}, no_cursor_timeout=True)

                    time_current = 0
                    time_current_backup = 0
                    time_feed = -1
                    for single_feed in rs_all_trip_update:
                        time_feed = 0
                        time_current_backup = time_current
                        time_current = single_feed["ts"]
                        for each_stop in single_feed["seq"]:
                            if each_stop["stop"] == stop_id:
                                time_current_backup = time_current
                                time_feed = each_stop["arr"]
                                break
                        if time_feed == 0:
                            break
                        if time_feed != 0 and time_current + time_walking*60 > time_feed:
                            time_smart = time_current_backup + time_walking*60
                            break
                    '''
                    alt_trip_id = "0"
                    alt_sequence_id = ""'''
                    time_alt = 9999999999
# Time for smart transit users' arrival time at the receiving stop
                    if time_current + time_walking*60 + buffer > time_feed and time_current + time_walking*60 < time_feed:
                        time_smart = time_current + time_walking*60
                    if time_smart == 999:
                        continue
                    
# Time for normal transit users, aka scheduled time follower
                    time_normal = convert_to_timestamp(
                        single_stop_time["arrival_time"], single_date, summer_time)  # schedule

# Time for actual transit arrival time, which is the last time you should be 
                    real_time = list(db_today_real_time.find(
                        {"stop_id": stop_id, "trip_id": trip_id}))

                    if (len(real_time) == 0):
                        continue
                    else:
                        time_actual = real_time[0]["time"]


# Time for smart transit user's actual onboard time
                    ##### Done: Initialization #####

                    ##### Start: Calculate the real trip. #####
                    false_trips_list = list(db_seq.find({"service_id": str(
                        service_id), "stop_id": stop_id, "route_id": route_id}))
                    # print(false_trips_list)

                    if len(false_trips_list)==0:
                        continue

                    false_trips_list.sort(key=sortQuery)
                    # print(false_trips_list)
                    '''seq_query = list(db_seq.find({"service_id": str(
                        service_id), "stop_id": stop_id, "trip_id": trip_id}))
                    if len(seq_query) == 0:
                        continue
                    
                    flag_sequence_id = seq_query[0]["seq"]'''

                    for each_trip in false_trips_list:
                        i_trip_id = each_trip["trip_id"]
                        # seq_id = each_trip["seq"]
                        # Find the b_alt_time for this trip_id and compare it to the b_alt_time (overall). Until we find the smallest one.
                        query_realtime=list(db_today_real_time.find({"stop_id":stop_id,"trip_id":str(i_trip_id)}))
                        if query_realtime==[]:
                            continue
                        i_real_time=query_realtime[0]["time"]
                        if time_alt > i_real_time and i_real_time >= time_smart:
                            time_alt = i_real_time
                            '''alt_sequence_id = seq_id - flag_sequence_id
                            alt_trip_id = i_trip_id'''
                        
                    # This means there's no alternative trip for this receiving trip. So you are doomed.
                    if time_alt == 9999999999:
                        time_alt = -1  # there's no an alternative trip.
                        '''alt_sequence_id = None
                        alt_trip_id = "-1"'''

                    if (time_smart - time_actual > criteria):
                        status = 1
                        dic_stops[stop_id]["miss_count_" + str(time_walking)] = dic_stops[stop_id]["miss_count_" + str(time_walking)] + 1
                    else:
                        status = 0
                    
                    dic_stops[stop_id]["diff_time_" + str(time_walking)] += (time_alt - time_smart) - (time_actual - time_normal)
                dic_stops[stop_id]["total_count"] = dic_stops[stop_id]["total_count"] + 1

    for stop_id in dic_stops.keys():
        if dic_stops[stop_id]['total_count'] != 0:
            dic_stops[stop_id]["diff_time_" + str(time_walking)] = dic_stops[stop_id]["diff_time_" + str(time_walking)]/(dic_stops[stop_id]['total_count'])
        db_today_smart_transit.insert(dic_stops[stop_id])
                

if __name__ == '__main__':
    date_list = []

    start_date = date(2018, 1, 31)
    end_date = date(2019, 1, 31)

    #analyze_transfer(start_date, end_date)

    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores)
    date_range = daterange(start_date, end_date)
    output=[]
    output=pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
