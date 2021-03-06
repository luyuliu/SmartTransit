
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import transfer_tools

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_apc_pr_opt
db_ar = client.cota_apc_ar
db_er = client.cota_apc_er

db_diff = client.cota_apc_join
db_diff_reduce = client.cota_diff_reduce

walking_time_limit = 10  # min
criteria = 0  # seconds
designated_route_id = 2
db_PR = client.cota_apc_pr_cal


def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)
    wt_risk_count = [0] * 10 
    wt_total_count = [0] * 10 
    for single_date in date_range:
        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_PR[today_date + "_0"]

        rl_diff = list(col_diff.find({}))

        for each_record in rl_diff:
            for i in range(10):
                time_gr_alt = (each_record["time_alt_" + str(i)])
                time_gr_act = (each_record["time_actual"])
                if time_gr_alt == 0 or time_gr_act == 0:
                    continue
                try:
                    if time_gr_alt > time_gr_act:
                        wt_risk_count [i] += 1
                except:
                    continue
                else:
                    wt_total_count[i] += 1

            # wt_diff_temp = single_stop_time["wt_ar"]* single_stop_time["total"]
        print(today_date,len(rl_diff), wt_risk_count, wt_total_count)
    print(wt_risk_count, wt_total_count)


if __name__ == "__main__":
    start_date = date(2018, 5, 7)
    end_date = date(2019, 4, 30)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
