# YACS
  
  ## analysis.py
  Make sure the csv log files for each worker is deleted. The analysis code is flexible and uses the config file to get the list of worker IDs. Since our worker logs are named worker_<wid>.log it can now get all the worker log files as df in pandas. As part of init() the log csv files are created. Both the master and worker have parameter to keep track of the start and end time. A record_log() is triggered when the end time for a task is logged.
  
  We need to run this for every batch of log files produced for every scheduling algo. It takes the job_pool.csv file as input and figures the worker log files on its own. Make sure they are in the same dir. Generates 2 graphs :
 * Number of tasks per worker in a bar graph : over all the jobs, the number of tasks are calculated
 * Number of tasks per worker against time : x-axis is time. y axis is number of tasks. at a given time x the worker is doing y number of tasks. This is generated per worker
 
