
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))
import transfer_tools
db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization
db_opt_result = client.cota_pr_optimization_result
db_ar = client.cota_ar
db_er = client.cota_er

db_diff = client.cota_diff
db_diff_reduce = client.cota_diff_reduce

walking_time_limit = 10  # min
criteria = 5  # seconds
designated_route_id = 2

db_PR = client.cota_pr_optimization


def reduce_diff(buffer):
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 30)
    date_range = transfer_tools.daterange(start_date, end_date)
    wt_time = 0
    wt_total_count = 0
    for single_date in date_range:
        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_PR[today_date + "_" + str(buffer)]

        rl_diff = list(col_diff.find({}))

        for each_record in rl_diff:
            for i in range(10):
                time_gr_alt = (each_record["time_alt_" + str(i)])
                time_gr_arr = (each_record["time_smart_" + str(i)])
                if time_gr_alt == 0 or time_gr_arr == 0:
                    continue
                try:
                    wt_time += time_gr_alt - time_gr_arr
                except:
                    continue
                else:
                    wt_total_count += 1

    print(buffer, wt_time/wt_total_count)
    return wt_time/wt_total_count


if __name__ == "__main__":
    
    insurance_buffers = range(0, 301, 10)
    pool = multiprocessing.Pool(processes=25)
    output = []
    output = pool.map(reduce_diff, insurance_buffers)
    pool.close()
    pool.join()

    print(output)
    # Todo: find skip reason
