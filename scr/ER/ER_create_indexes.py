
from pymongo import MongoClient, ASCENDING
from datetime import timedelta, date
import datetime
import multiprocessing
import time

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_er_val = client.cota_er_validation


def analyze_transfer(start_date, end_date, memory):
    wt_er = 0 
    wt_er_count = 0
    for single_date in transfer_tools.daterange(start_date, end_date):
        today_date = single_date.strftime("%Y%m%d")  # date
        col_er_val = db_er_val["er_min_" + str(memory) + "_" + today_date]
        col_er_val.create_index([("stop_id", ASCENDING), ("trip_id", ASCENDING)])
        print(today_date, memory)

if __name__ == '__main__':
    for i in range(1, 10):
        analyze_transfer(date(2018, 2, 1), date(2019, 1, 31), i)
