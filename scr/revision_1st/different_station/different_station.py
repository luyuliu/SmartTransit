

from pymongo import MongoClient, ASCENDING
from datetime import timedelta, date
import datetime
import multiprocessing
import time
from math import sin, cos, sqrt, atan2, pi, acos

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import transfer_tools

client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_apc_real_time
db_trip_update = client.trip_update
db_opt_result = client.cota_re_er_cal

walkSpeed = 1.4


def calculateDistance(latlng1, latlng2):
    R = 6373
    lat1 = float(latlng1["lat"])
    lon1 = float(latlng1["lon"])
    lat2 = float(latlng2["lat"])
    lon2 = float(latlng2["lon"])

    theta = lon1 - lon2
    radtheta = pi * theta / 180
    radlat1 = pi * lat1 / 180
    radlat2 = pi * lat2 / 180

    dist = sin(radlat1) * sin(radlat2) + cos(radlat1) * \
        cos(radlat2) * cos(radtheta)
    try:
        dist = acos(dist)
    except:
        dist = 0
    dist = dist * 180 / pi * 60 * 1.1515 * 1609.344

    return dist


def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    col_real_time = db_real_time[today_date]
    rl_real_time = list(col_real_time.find(
        {"$or": [{"route_id": 2}, {"route_id": -2}]}))
    for each_real_time in rl_real_time:
        try:
            each_real_time["stop_sequence"] = int(
                each_real_time["stop_sequence"])
        except:
            continue

    rl_real_time_sort = sorted(rl_real_time, key=lambda x: (
        x["trip_id"], x["stop_sequence"]))
    positive_counts = []
    negative_counts = []

    for i in range(0, 11):
        total_count = len(rl_real_time_sort)
        positive_count = 0  # this could save time for users
        # this could not save time for users, even if you run, there is no chance to save time.
        negative_count = 0

        for index in range(len(rl_real_time_sort)):
            first_stop = rl_real_time_sort[index]
            try:
                second_stop = rl_real_time_sort[index + 1]
            except:
                continue

            if type(first_stop["stop_sequence"]) is not int or type(second_stop["stop_sequence"]) is not int:
                continue
            
            if first_stop["stop_sequence"] != second_stop["stop_sequence"] -1:
                continue

            xo = i * 60 * walkSpeed
            distance = calculateDistance(first_stop, second_stop)
            xa = sqrt(xo**2 + distance ** 2)

            delta_t = (xa - xo)/walkSpeed
            delta_T = second_stop["actual_departure_time"] - \
                first_stop["actual_departure_time"]
            # print(xo, xa, distance)
            # print(first_stop, second_stop)
            t = int(delta_T - delta_t)
            # if abs(t) > 10000:
            #     print(first_stop, second_stop)
            if t < 0:
                negative_count += 1
                # print(t)
            elif t >= 0:
                positive_count += 1
        positive_counts.append(positive_count)
        negative_counts.append(negative_count)
    print(today_date, positive_counts, negative_counts, total_count)


if __name__ == '__main__':
    start_date = date(2018, 5, 7)
    end_date = date(2019, 5, 6)

    date_range = list(transfer_tools.daterange(start_date, end_date))
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=30)
    output = []
    output = pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()

    # analyze_transfer(start_date)
    # a = {
    #     "lat": "39.950679",
    #     "lon": "-83.147342",
    #     "stop_name": "WESTWOODS PARK AND RIDE"
    # }

    # b = {
    #     "lat": "39.951375",
    #     "lon": "-83.145425"
    # }

    # print(calculateDistance(a, b))
