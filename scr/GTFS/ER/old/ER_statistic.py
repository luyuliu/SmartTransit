from pymongo import MongoClient
from datetime import timedelta, date
import datetime
import multiprocessing
import time

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools


client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_er

def analyze_transfer(start_date, end_date):
    records_dic={} # Avoid IO. But could be bad for small memory.
    for single_date in transfer_tools.daterange(start_date, end_date):
        
        today_date = single_date.strftime("%Y%m%d")  # date
        col_real_time = db_real_time["R"+today_date]
        rl_real_time = list(col_real_time.find({"stop_id": "MOUHIGW1", "route_id":-2}))
        if rl_real_time == []:
            print(None)
        else:
            for i in rl_real_time:
                print(i["time"]-i["scheduled_time"] )


if __name__ == '__main__':
    analyze_transfer(date(2018, 2, 1), date(2019, 8, 31))