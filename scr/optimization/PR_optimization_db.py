from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization


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

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

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


# A assumption number. When user see the expected arrival time is less than 120 seconds, then leave home.
walking_time_limit = 10
criteria = 5

is_paralleled = False


def analyze_transfer(buffer, each_date):
    single_date = each_date
    trips_collection = []
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
    db_today_smart_transit = db_smart_transit[today_date+"_"+str(buffer)]

    that_time_stamp = find_gtfs_time_stamp(single_date)
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
    db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
    db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
    db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_today_real_time = db_real_time["R" + today_date]
    db_today_trip_update = db_trip_update[today_date]

    rs_all_trips = list(db_trips.find({"service_id": str(
        service_id), "route_id": "002"}))  # Control the size of sampling
    count = 0
    total_count = len(rs_all_trips)

    print(today_date, ": start.")
    for single_trip in rs_all_trips:
        trip_id = single_trip["trip_id"]  # emurate rs_all_trips
        direction_id = int(single_trip["direction_id"])
        # route number: if direction_id=0 then tag=1; if direction_id=1 then tag=-1;
        route_id = int(single_trip["route_id"]) * (-direction_id*2+1)

        rs_all_stops = list(db_stop_times.find({"trip_id": trip_id}))

        realtime_error = 0

        rs_all_trip_update = list(db_today_trip_update.find(
            {"trip_id": trip_id}, no_cursor_timeout=True))  # All GTFS real-time feed
        
        expected_stop_times_list = {} # First key: stop, second key: T_cu. Value: T_ex
        for single_feed in rs_all_trip_update: # Todo
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

        for single_stop_time in rs_all_stops:
            stop_id = single_stop_time["stop_id"]  # query stop_times
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
            # normal users' actual boarding time (bus's actual boarding time)
            line["time_actual"] = 0
            # normal users' expected boarding time (bus's scheduled boarding time)
            line["time_normal"] = 0
            for time_walking in range(walking_time_limit):
                # smart users' actual boarding time
                line["time_alt_" + str(time_walking)] = 0
                # smart users' expected boarding time
                line["time_smart_" + str(time_walking)] = 0

            # Time Normal: Time for normal transit users, aka scheduled time follower
            line["time_normal"] = convert_to_timestamp(
                single_stop_time["arrival_time"], single_date, summer_time)  # schedule

            # Time Actual: Time for actual transit arrival time, which is the last time you should be
            real_time = list(db_today_real_time.find(
                {"stop_id": stop_id, "trip_id": trip_id}))

            if (len(real_time) == 0):
                line["time_actual"] = "no_realtime_trip"
                # print("no_realtime_trip: ", stop_id, trip_id, route_id, that_time_stamp)
                realtime_error += 1
                continue
            else:
                line["time_actual"] = real_time[0]["time"]

            alt_trips_list = list(db_seq.find({"service_id": str(
                service_id), "stop_id": stop_id, "route_id": route_id}))

            if len(alt_trips_list) == 0:
                for time_walking in range(walking_time_limit):
                    line["time_smart_" +
                         str(time_walking)] == "gtfs_static_error"
                # print("gtfs_static_error")
                continue
            alt_trips_list.sort(key=sortQuery)

            # This list is the alt trips' actual departure time. The sequence of this is the temporal sequence of trip sequence array in the SCHEDULE.
            alt_times_list = []
            # However, the order could be not strictly in temporal order.
            for each_trip in alt_trips_list:
                i_trip_id = each_trip["trip_id"]
                query_realtime = list(db_today_real_time.find(
                    {"stop_id": stop_id, "trip_id": str(i_trip_id)}))
                if query_realtime == []:  # the i_trip_id didn't exist.
                    continue
                i_real_time = query_realtime[0]["time"]
                alt_times_list.append(i_real_time)

            for time_walking in range(walking_time_limit):
                # Time Smart: Time for smart transit users' arrival time at the receiving stop
                # past_predicted_time + walking_time
                line["time_smart_" + str(time_walking)] = 0
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

                # Time Alt: Time for smart transit user's actual onboard time

                line["time_alt_" + str(time_walking)] = 9999999999

                for i_real_time in alt_times_list:
                    # relaxed
                    if line["time_alt_" + str(time_walking)] > i_real_time and i_real_time >= line["time_smart_" + str(time_walking)] - criteria:
                        line["time_alt_" + str(time_walking)] = i_real_time

                if line["time_alt_" + str(time_walking)] == 9999999999:
                    # there's no an alternative trip.
                    line["time_alt_" + str(time_walking)] = "critical_trip"

            trips_collection.append(line)

            if len(trips_collection) > 200:
                db_today_smart_transit.insert_many(trips_collection)
                trips_collection = []

        #print(today_date, count, total_count, realtime_error, len(rs_all_stops))
        if round(count/total_count*100, 0) % 20 == 5:
            print(today_date, route_id, ": ", count /
                  total_count*100, "%% finished.")
        count += 1
    print(today_date, ": finished.")


if __name__ == '__main__':
    date_list = []

    insurance_buffers = range(0, 301, 10)

    start_date = date(2018, 9, 20)
    end_date = date(2019, 1, 31)

    if is_paralleled:
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=25)
        date_range = daterange(start_date, end_date)
        output = []
        output = pool.map(analyze_transfer, date_range)
        pool.close()
        pool.join()
    else:
        for each_date in daterange(start_date, end_date):
            cores = multiprocessing.cpu_count()
            pool = multiprocessing.Pool(processes=31)
            output = []
            output = pool.starmap(analyze_transfer, zip(insurance_buffers,[each_date]*len(insurance_buffers)))
            pool.close()
            pool.join()
