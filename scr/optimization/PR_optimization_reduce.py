from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization_result


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


walking_time_limit = 10
criteria = 5

is_paralleled = False


def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    db_pr_optimization_opt = db_smart_transit[today_date+"_opt"]
    
    records_dic = {}

    db_opt_result = list(db_pr_optimization_opt.find({}))

    for each_record in db_opt_result:
        stop_id = each_record["stop_id"]
        route_id = each_record["route_id"]
        try:
            records_dic[route_id]
        except:
            records_dic[route_id]={}
        else:
            pass

        try:
            records_dic[route_id][stop_id]
        except:
            records_dic[route_id][stop_id]={}
            records_dic[route_id][stop_id]["route_id"] = each_record['route_id']
            records_dic[route_id][stop_id]["stop_id"] = each_record['stop_id']
            records_dic[route_id][stop_id]["lat"] = each_record['lat']
            records_dic[route_id][stop_id]["lon"] = each_record['lon']

            for walking_time in range(walking_time_limit):
                records_dic[route_id][stop_id]["sum_buff_"+str(walking_time)] = each_record["optima_buffer_"+str(walking_time)]
                records_dic[route_id][stop_id]["max_buff_"+str(walking_time)] = each_record["optima_buffer_"+str(walking_time)]
                if each_record["time_actual"] != each_record["time_alt_"+str(walking_time)]:
                    records_dic[route_id][stop_id]["miss_risk_"+str(walking_time)] = 1
                else:
                    records_dic[route_id][stop_id]["miss_risk_"+str(walking_time)] = 0
            records_dic[route_id][stop_id]["trip_count"] = 1
        else:
            for walking_time in range(walking_time_limit):
                records_dic[route_id][stop_id]["sum_buff_"+str(walking_time)] += each_record["optima_buffer_"+str(walking_time)]
                records_dic[route_id][stop_id]["max_buff_"+str(walking_time)] = max(each_record["optima_buffer_"+str(walking_time)], records_dic[route_id][stop_id]["max_buff_"+str(walking_time)])
                records_dic[route_id][stop_id]["miss_risk_"+str(walking_time)]
                if each_record["time_actual"] != each_record["time_alt_"+str(walking_time)]:
                    records_dic[route_id][stop_id]["miss_risk_"+str(walking_time)] += 1
            records_dic[route_id][stop_id]["trip_count"] += 1

    for each_route, stop_collection in records_dic.items():
        for each_stop, each_record in stop_collection.items():
            db_smart_transit[today_date+"_route_reduced"].insert_one(each_record)

    


if __name__ == '__main__':
    single_date = date(2018, 2, 1)
    analyze_transfer(single_date)
    