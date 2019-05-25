# SmartTransit

In this project, we will use GTFS (General Transit Feed Specification) real-time to simulate RTA (real-time transit apps) users and non-RTA users' waiting time difference.

## Project scopes ##

### 1. PR (Prudent Relaxation) TPS (Trip Planning Strategy) optimization ###
First, we would like to optimize PR's waiting time over IB (insurance buffer).
### 2. Cross-compare different TPSs' waiting time difference
There are several TPSs to be compared: 

First TPS  | Second TPS
------------- | -------------
PR optimal  | ER
RR  | NR
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

**PR_opt_optimize.py**: After validating each stop_time in GTFS, then find the optimal one with minimal waiting time for each stop_time.

**PR_opt_finalize.py**: Then, reduce every day's optimal IB into a day's schedule. Using average reducing rules.

**PR_opt_revalidate.py**: Validate every day's stop_time again to test the effectiveness of the PR optimal strategy.
