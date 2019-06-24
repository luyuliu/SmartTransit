# SmartTransit

In this project, we will use GTFS (General Transit Feed Specification) real-time to simulate RTA (real-time transit apps) users and non-RTA users' waiting time difference.

## Environment setups ##
Before executing the codes, please make sure you install MongoDB, Python (and pyMongo library in Python), and a reliable MongoDB management tools.
To execute the codes, you should have several Mongo Databases ready.

### 1. GTFS Real-time ###
1.1. You need to use scr/mongodb/feed_collection.py to collect data from the streaming website. Better solution will be a server with python script catching real-time feed and add to a Mongodb database.<br />
1.2. After getting the huge streaming database, run divide_collection.py to divide the huge collection into several individual collection of each day.<br />
1.3. Then, run find_real_time.py to find the most accurate time from the real-time data.<br />
<br />
After running all of these above, you should be able to get a database of each day's all trips' real departure/arrival time at each stop. Please be advised that this time is independent from the GTFS static (though when the PT system agency is producing and streaming the real-time data, they will try to make them consistent), which means we can do all of these above regardless of the GTFS static data.

### 2. GTFS Static
2.1. Run scr/mongodb/feed_export_tripupdate_schedule_separation.py to get GTFS static from an online source (a zip file) and store it to a Mongo database with each file in the zip as a collection.
2.2. You may want to revise these scripts since pratically I did not write these scripts. I only use these data exported from the data server. I used "collection_name"+ "_"+timestamp to name the collection to avoid ambiguous naming.

### 3. Additional GTFS Static
After finishing II:
3.1. Run scr/transfer/find_transfer.py to generate the transfer schedule. Since transfer schedule is not explicitly defined in the GTFS schedule, we have to find all possible and closest transfer. Please check my paper to get more information about this. The generated transfer schedule file will be stored at gtfs database as "transfers" collection. So, please make sure to check if your GTFS static has an vanilla version of transfers file.
3.2. Run scr/transfer/sort_stop_time.py to generate the trip_seq collection, which represents the sequence of each route's trips. 
For example, route 1 will go through stop "RIVHOSW". And there will be several different buses with different trip_id. Then trip_seq collection is to record these trips' temporal sequence at this stop. The sequence will start from 0.

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

**PR_opt_finalize.py**: Then, reduce every day's optimal IB into a day's schedule. Using average reducing rules.

**PR_opt_revalidate.py**: Validate every day's stop_time again to test the effectiveness of the PR optimal strategy.

### 2. Cross-compare different TPSs' waiting time difference

**Todos**:
PR optimal versus ER/NR/AR and
RR optimal versus ER/NR/AR

### 3. Visualization of waiting time, waiting time difference, and IBs.

Todo: TPSs' waiting time;
waiting time difference.
IB

