from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
import json

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


# main loop
# enumerate every day in the range
designated_route = 1
walking_time_limit = 10
buffer = 120
criteria = 5


def analyze_transfer(start_date, end_date):
    date_range = daterange(start_date, end_date)
    dic_stops = {}
    for single_date in date_range:
        if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
            summer_time = 0
        else:
            summer_time = 1
        today_date = single_date.strftime("%Y%m%d")  # date
        db_today_smart_transit = db_smart_transit[today_date]

        that_time_stamp = find_gtfs_time_stamp(single_date)

        rs_all_stop_time = list(db_today_smart_transit.find({"route_id" : designated_route}))

        count=0
        total_count= len(rs_all_stop_time)


        for single_stop_time in rs_all_stop_time:
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
                    print("Doomed: ", stop_id, trip_id)
                    continue
                if time_smart > time_actual:
                    dic_stops[stop_id]["miss_c_"+str(time_walking)]+=1
                dic_stops[stop_id]['wt_dif_'+str(time_walking)] += wait_diff
                dic_stops[stop_id]['dt_dif_'+str(time_walking)] += depart_diff
            dic_stops[stop_id]["totl_c"] += 1
                    
    json_string = json.dumps(dic_stops)
    location = 'D:\\Luyu\\SmartTransit\\Data\\AWT_json\\'+ str(designated_route) + ".json"
    print("Location: ", location)
    try:
        F = open(location ,"x")
    except:
        F = open(location ,"w")
    F.write(json_string)

    F.close()


if __name__ == '__main__':
    date_list = []

    start_date1 = date(2018, 1, 31)
    end_date1 = date(2018, 2, 1)

    analyze_transfer(start_date1, end_date1)
