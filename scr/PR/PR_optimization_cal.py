from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization
db_opt_result = client.cota_pr_optimization_result

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
    
    insurance_buffers = range(0, 301, 10)

    records_dic=[] # Avoid IO. But could be bad for small memory.

    for each_buffer in insurance_buffers:    
        print("Start: "+ today_date +" - ",each_buffer)
        db_today_smart_transit = db_smart_transit[today_date+"_"+str(each_buffer)]
        each_buffer_trip_collection = list(db_today_smart_transit.find({}))
        if each_buffer == insurance_buffers[0]:
            records_dic = each_buffer_trip_collection
            for each_record in records_dic:
                for walking_time in range(walking_time_limit):
                    each_record["optima_buffer_"+str(walking_time)] = 0
        else:
            for index in range(len(each_buffer_trip_collection)):
                for walking_time in range(walking_time_limit):
                    if type(each_buffer_trip_collection[index]["time_alt_"+str(walking_time)]) is not int or type(each_buffer_trip_collection[index]["time_smart_"+str(walking_time)]) is not int or each_buffer_trip_collection[index]["time_alt_"+str(walking_time)] == 0:
                        continue
                    if type(records_dic[index]["time_alt_"+str(walking_time)]) is not int or type(records_dic[index]["time_smart_"+str(walking_time)]) is not int or records_dic[index]["time_alt_"+str(walking_time)] == 0:
                        records_dic[index]["time_alt_"+str(walking_time)] = each_buffer_trip_collection[index]["time_alt_"+str(walking_time)]
                        records_dic[index]["time_smart_"+str(walking_time)] = each_buffer_trip_collection[index]["time_smart_"+str(walking_time)]
                        records_dic[index]["optima_buffer_"+str(walking_time)] = each_buffer
                        continue
                    if each_buffer_trip_collection[index]["time_alt_"+str(walking_time)] - each_buffer_trip_collection[index]["time_smart_"+str(walking_time)] < records_dic[index]["time_alt_"+str(walking_time)] - records_dic[index]["time_smart_"+str(walking_time)]:
                        records_dic[index]["time_alt_"+str(walking_time)] = each_buffer_trip_collection[index]["time_alt_"+str(walking_time)]
                        records_dic[index]["time_smart_"+str(walking_time)] = each_buffer_trip_collection[index]["time_smart_"+str(walking_time)]
                        records_dic[index]["optima_buffer_"+str(walking_time)] = each_buffer
    
    print("Start: "+ today_date +" - Database insert.")
    db_opt_result[today_date+"_opt"].insert_many(records_dic)
    


if __name__ == '__main__':
    single_date = date(2018, 2, 1)
    analyze_transfer(single_date)
    