

from pymongo import MongoClient, ASCENDING
from datetime import timedelta, date
import datetime
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_pr_optimization
db_opt_result = client.cota_pr_optimization_result
db_delay_reclamation = client.cota_delay_reclamation

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

walking_time_limit = 10
buffer = 5

is_paralleled = False


def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date

    col_delay_reclamation = db_delay_reclamation[today_date + "_60"]
    
    time_dic = {}

    col_real_time = db_real_time["R"+ today_date]

    records_dic=[] # Avoid IO. But could be bad for small memory.
    print(today_date +" - Initialization.")

    db_today_smart_transit = db_smart_transit[today_date+"_60"]
    each_buffer_trip_collection = list(db_today_smart_transit.find({}))
    
    count = 0
    for each_record in each_buffer_trip_collection:
        count += 1
        time_actual = each_record["time_actual"]
        time_normal = each_record["time_normal"]
        trip_id = each_record["trip_id"]
        stop_id = each_record["stop_id"]

        try:
            time_dic[trip_id]
        except:
            time_dic[trip_id] = {}
            rl_real_time = list(col_real_time.find({"trip_id": trip_id}).sort([("stop_sequence", ASCENDING)]))
            for each_time in rl_real_time:
                astop_id = each_time["stop_id"]
                time_dic[trip_id][astop_id] = each_time

        for time_walking in range(10):
            time_smart = each_record["time_smart_" + str(time_walking)]
            time_then = time_smart - time_walking*60
            the_index = list(time_dic[trip_id].keys()).index(stop_id)

            for index in range(the_index, -1, -1):
                the_item = list(time_dic[trip_id].values())[index]
                time_potential = the_item["time"]
                time_potential_scheduled = the_item["scheduled_time"]

                if time_potential <= time_then:
                    each_record['time_actual_then_' + str(time_walking)] = time_potential
                    each_record['time_normal_then_' + str(time_walking)] = time_potential_scheduled
                    break

        each_record.pop("_id", None)
        records_dic.append(each_record)

        if len(records_dic) == 2000:
            col_delay_reclamation.insert_many(records_dic)
            print(single_date, "Insert", count)
            records_dic = []

    if len(records_dic) != 0:
        col_delay_reclamation.insert_many(records_dic)
        records_dic = []
    
    print(single_date, "Finished.")
            
            

                

    
    
    


if __name__ == '__main__':
    # single_date = date(2018, 2, 1)
    # analyze_transfer(single_date)
    
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)

    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=25)
    date_range = transfer_tools.daterange(start_date, end_date)
    output = []
    output = pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
    
    # for single_date in transfer_tools.daterange(start_date, end_date):
    #     analyze_transfer(single_date)