
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime, pymongo
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')
sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))
import transfer_tools


db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization
db_opt_result = client.cota_pr_optimization_test
db_ar = client.cota_ar
db_er = client.cota_er

db_diff = client.cota_diff
db_diff_reduce = client.cota_diff_reduce

walking_time_limit = 10  # min
criteria = 5  # seconds
designated_route_id = 2


def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)
    
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        today_weekday = single_date.weekday()

        that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)
        # db_stops = db_GTFS[str(that_time_stamp) + "_stops"]
        # db_trips = db_GTFS[str(that_time_stamp) + "_trips"]
        # db_stop_times = db_GTFS[str(that_time_stamp) + "_stop_times"]
        # db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
        # col_real_time = db_real_time["R" + today_date]
        # col_trip_update = db_trip_update[today_date]
        col_rt = db_trip_update[today_date]
        col_tu = db_trip_update["full_trip_update"]
        rl_tu = col_tu.find({"start_date": today_date})#.sort([("trip_id", pymongo.ASCENDING)])

        count = 0
        error = 0
        dup = 0
        total_count = rl_tu.count()
        for tu_each_record in rl_tu:
            tu_trip_id = tu_each_record["trip_id"]
            ts = tu_each_record["ts"]
            rt_each_record = list(col_rt.find({"ts": ts, "trip_id": tu_trip_id}))
            if len(rt_each_record)!=1:
                # print(len(rt_each_record), count)
                
                # print(rt_each_record[0])
                # print(rt_each_record[1])
                if len(rt_each_record)==0:
                    error +=1
                    count += 1
                    continue
                else:
                    dup +=1
                    count += 1
                    continue
            else:
                rt_each_record = rt_each_record[0]
            # rt_trip_id = rt_each_record["trip_id"]
            # tu_seq = tu_each_record["seq"]
            # # print(rt_each_record)

            count += 1
            

            # if int(tu_trip_id)-int(rt_trip_id) != 0:
            #     error+=1
            #     print(tu_trip_id, rt_trip_id)
            
            if (count% 1000 ==1):
                print(count/total_count, error, count, dup)
        
        print("all done")





if __name__ == "__main__":
    start_date = date(2018, 9, 5)
    end_date = date(2018, 9, 6)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
