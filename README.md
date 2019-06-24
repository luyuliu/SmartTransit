# SmartTransit

In this project, we will use GTFS (General Transit Feed Specification) real-time to simulate RTA (real-time transit apps) users and non-RTA users' waiting time difference.

## Environment setups ##
Before executing the codes, please make sure you install MongoDB, Python (and pyMongo library in Python), and a reliable MongoDB management tools.
To execute the codes, you should have several Mongo Databases ready.

### 1. GTFS Real-time ###
1.1. You need to collect data from the streaming API. Better solution will be a server with python script catching real-time feed and add to a Mongodb database.<br />
1.2. After getting the huge streaming database, run scr/database/divide_collection.py to divide the huge collection into several individual collection of each day.<br />
1.3. Then, run find_real_time.py to find the most accurate time from the real-time data.<br />
<br />
After running all of these above, you should be able to get a database of each day's all trips' real departure/arrival time at each stop. Please be advised that this time is independent from the GTFS static (though when the PT system agency is producing and streaming the real-time data, they will try to make them consistent), which means we can do all of these above regardless of the GTFS static data.

### 2. GTFS Static
2.1. Get GTFS static from an online source (a zip file) and store it to a Mongo database with each file in the zip as a collection.
2.2. You may want to revise these scripts since pratically I did not write these scripts. I only use these data exported from the data server. I used "collection_name"+ "_"+timestamp to name the collection to avoid ambiguous naming. The timestamp is the time when the gtfs static changed. <br />
3.2 Create indexes. Indexes can magnificantly improve the performance of those scripts. Try run scr/database/create_indexes.py.<br />
These indexes are:

database | collection | indexes
------------- | ------------- | -------------
trip_update | today_date | trip_id
cota_real_time | R + today_date | trip_id + stop_id; stop_id + trip_sequence




### 3. Additional GTFS Static
After finishing II:
3.1. Run scr/transfer/sort_stop_time.py to generate the trip_seq collection, which represents the sequence of each route's trips. 
For example, route 1 will go through stop "RIVHOSW". And there will be several different buses with different trip_id. Then trip_seq collection is to record these trips' temporal sequence at this stop. The sequence will start from 0.<br />
3.2 Create indexes, again. Indexes can magnificantly improve the performance of those scripts. Try run scr/database/create_indexes.py.<br />
These indexes are:

collection | indexes
------------- | -------------
stop_times| trip_id + stop_id;<br />
stops| stops_id;<br />
trip_seq | service_id + stop_id + route_id + trip_id; trip_id + stop_id; service_id + stop_id + route_id + seq_id;<br />
trip | trip_id + service_id; service_id + route_id;<br />

## Project scopes ##

### 1. PR (Prudent Relaxation) TPS (Trip Planning Strategy) optimization ###
First, we would like to optimize PR's waiting time over IB (insurance buffer).
### 2. Cross-compare different TPSs' waiting time difference
There are several TPSs to be compared: 

First TPS  | Second TPS
------------- | -------------
PR optimal  | ER
GR (formerly known as RR)  | NR
Static PR    | AR
### 3. Visualization of waiting time, waiting time difference, and IBs


## Code sequence/dependence
### 1. PR optimization ###
**PR_opt_calculate.py**: First, we need to validate each stop_time in GTFS with 10 minutes walking range.<br />
Run this code and the raw optimization results are in cota_pr_optimization database, with collection name of today_date + "_" + walking _time.

Pseudocode:<br />
For each buffer In the possible buffer list:<br />
&nbsp;&nbsp;&nbsp;For each date In Date list:<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;For each GTFS trip In the trip list:<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Query all real-time and scheduled time;<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;For each stop On the GTFS trip:<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Find the scheduled time T_sc;<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Find the real-time time T;<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;For each walking time from 0 to 10:<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Find the RTA users’ arrival time t;<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Find the actual departure time T(t) correspondingly;<br />

**PR_opt_optimize.py**: After validating each stop_time in GTFS, find the optimal one with minimal waiting time for each stop_time.
Run this code and the optimized results (minimal waiting time and corresponding IB) will be stored in cota_pr_optimization_result database with collection name of todaydate + "_opt_risk_averse".
You may encounter historic version of todaydate + "_opt", which is derived by averaging and not useable anymore.

**PR_opt_finalize.py**: Then, reduce every day's optimal IB into a day's schedule. Using maximum reducing rules. We tried average rule, but turns out the miss risk is high. <br />The results will be stored in the cota_pr_optimization_result database with collection name of pr_opt_ibs_risk_averse. The legacy version of "pr_opt_ibs" is not useful.

**PR_opt_revalidate.py**: Validate every day's stop_time again to test the effectiveness of the PR optimal strategy.
The result will be in cota_pr_optimization database with collection name of today_date + "_reval_max". "risk_averse" series will follow average rule, thus the results are not good.<br />

Find corresponding TPS's code in their folder and database when analyzing. E.g.: AR (scr/AR, cota_ar), ER (scr/ER, cota_er), PR_optimal (scr/PR, cota_pr_optimization_result/today_date + "_reval_max"). If you need GR (RR), just use collection with name of today_date + "_0" in the cota_pr_optimization database.

### 2. Cross-compare different TPSs' waiting time difference
The codes is in scr/diff. 
diff_join_risk_averse

### 3. Visualization of waiting time, waiting time difference, and IBs.

