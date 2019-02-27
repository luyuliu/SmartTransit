from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update


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
buffer=120


def analyze_transfer(start_date, end_date):
    date_range = daterange(start_date, end_date)
    dic_stops = {}
    for single_date in date_range:
        if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
            summer_time = 0
        else:
            summer_time = 1
        today_date = single_date.strftime("%Y%m%d")  # date
        that_time_stamp = find_gtfs_time_stamp(single_date)
        db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
        db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
        db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
        db_today_real_time = db_real_time["R" + today_date]
        db_today_trip_update = db_trip_update[today_date]

        # for single_trip in rs_all_trips:
        #    trip_id=single_trip["trip_id"]
        rs_all_trips = list(db_trips.find(
            {"route_id": "{:03d}".format(designated_route)}))

        single_trip = rs_all_trips[int(len(rs_all_trips)/3)]

        trip_id = single_trip["trip_id"]  # emurate rs_all_trips

        rs_all_stops = list(db_stop_times.find({"trip_id": trip_id}))

        single_stop_time = rs_all_stops[int(len(rs_all_stops)/3)]

        stop_id = single_stop_time["stop_id"]  # query stop_times

        for time_walking in range(walking_time_limit):
            time_smart = 999  # past_predicted_time + walking_time
            
            rs_all_trip_update = db_today_trip_update.find({"trip_id":trip_id}, no_cursor_timeout=True)

            time_current=0
            time_current_backup = 0
            time_feed = -1
            for single_feed in rs_all_trip_update:
                time_feed = 0
                time_current_backup = time_current
                time_current = single_feed["ts"]
                for each_stop in single_feed["seq"]:
                    if each_stop["stop"]==stop_id:
                        time_current_backup = time_current
                        time_feed = each_stop["arr"]
                        break
                if time_feed == 0:
                    break
                if time_feed!= 0 and time_current + time_walking*60 > time_feed:
                    time_smart = time_current_backup + time_walking*60
                    break

            if time_current + time_walking*60 +buffer > time_feed and time_current + time_walking*60 < time_feed:
                time_smart = time_current + time_walking*60

            
            time_normal = convert_to_timestamp(single_stop_time["arrival_time"], single_date, summer_time)  # schedule

            real_time = list(db_today_real_time.find({"stop_id":stop_id,"trip_id":trip_id}))

            if (len(real_time) == 0):
                continue
            else:
                time_actual = real_time[0]["time"]

            if (time_smart>time_actual):
                status = 1
            else:
                status = 0
            diff_time = time_smart - time_normal

            print(time_normal, time_smart, time_actual)
            print(today_date,time_walking, diff_time, status, summer_time)



if __name__ == '__main__':
    date_list = []

    start_date1 = date(2018, 5, 2)
    end_date1 = date(2018, 5, 3)

    analyze_transfer(start_date1, end_date1)
