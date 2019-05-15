from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import transfer_tools

client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_result = client.cota_ar

if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2018, 2, 2)
    for single_date in transfer_tools.daterange(start_date, end_date):
        that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
        db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
        db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
        db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
        db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
        today_weekday = single_date.weekday()  # day of week
        if today_weekday < 5:
            service_id = 1
        elif today_weekday == 5:
            service_id = 2
        else:
            service_id = 3


        col_real_time = db_real_time[single_date]
        result_real_time = list(col_real_time.find({}))
        for each_record in result_real_time:
            trip_id = each_record["trip_id"]
            stop_id = each_record["stop_id"]
            
            insert_query = {"_id": each_record["_id"]}
            try:
                result_trip = db_trips.find({"trip_id": trip_id})[0]
