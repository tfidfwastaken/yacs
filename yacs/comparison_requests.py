import json
import socket
import time
import sys
import random
import numpy as np
import pathlib

def create_job_request(job_id):
	number_of_map_tasks=random.randrange(1,5)
	number_of_reduce_tasks=random.randrange(1,3)

	job_request={"job_id":job_id,"map_tasks":[],"reduce_tasks":[]}
	for i in range(0,number_of_map_tasks):
		map_task={"task_id":job_id+"_M"+str(i),"duration":random.randrange(1,5)}
		job_request["map_tasks"].append(map_task)
	for i in range(0,number_of_reduce_tasks):
		reduce_task={"task_id":job_id+"_R"+str(i),"duration":random.randrange(1,5)}
		job_request["reduce_tasks"].append(reduce_task)
	print(type(job_request))
	return job_request

def send_request(job_request):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect(("localhost", 5000))
		message=json.dumps(job_request)
		#send task
		s.send(message.encode())

if __name__ == '__main__':
	#if(len(sys.argv)!=2):
	#	print("Usage: python requests.py <number_of_requests>")
	#	exit()

	job_requests_list = [
	{'job_id': '0', 'map_tasks': [{'task_id': '0_M0', 'duration': 2}, {'task_id': '0_M1', 'duration': 3}, {'task_id': '0_M2', 'duration': 2}, {'task_id': '0_M3', 'duration': 4}], 'reduce_tasks': [{'task_id': '0_R0', 'duration': 2}, {'task_id': '0_R1', 'duration': 3}]},
	{'job_id': '1', 'map_tasks': [{'task_id': '1_M0', 'duration': 4}], 'reduce_tasks': [{'task_id': '1_R0', 'duration': 2}, {'task_id': '1_R1', 'duration': 3}]}, 
	{'job_id': '2', 'map_tasks': [{'task_id': '2_M0', 'duration': 1}, {'task_id': '2_M1', 'duration': 4}], 'reduce_tasks': [{'task_id': '2_R0', 'duration': 2}]}, 
	{'job_id': '3', 'map_tasks': [{'task_id': '3_M0', 'duration': 4}, {'task_id': '3_M1', 'duration': 1}, {'task_id': '3_M2', 'duration': 1}], 'reduce_tasks': [{'task_id': '3_R0', 'duration': 3}, {'task_id': '3_R1', 'duration': 1}]}, 
	{'job_id': '4', 'map_tasks': [{'task_id': '4_M0', 'duration': 1}, {'task_id': '4_M1', 'duration': 2}], 'reduce_tasks': [{'task_id': '4_R0', 'duration': 2}]}, 		{'job_id': '5', 'map_tasks': [{'task_id': '5_M0', 'duration': 3}, {'task_id': '5_M1', 'duration': 4}, {'task_id': '5_M2', 'duration': 2}], 'reduce_tasks': [{'task_id': '5_R0', 'duration': 2}]}, 
	{'job_id': '6', 'map_tasks': [{'task_id': '6_M0', 'duration': 4}, {'task_id': '6_M1', 'duration': 4}, {'task_id': '6_M2', 'duration': 4}], 'reduce_tasks': [{'task_id': '6_R0', 'duration': 1}]}, 
	{'job_id': '7', 'map_tasks': [{'task_id': '7_M0', 'duration': 2}], 'reduce_tasks': [{'task_id': '7_R0', 'duration': 4}]}, 		{'job_id': '8', 'map_tasks': [{'task_id': '8_M0', 'duration': 2}, {'task_id': '8_M1', 'duration': 2}, {'task_id': '8_M2', 'duration': 4}], 'reduce_tasks': [{'task_id': '8_R0', 'duration': 3}, {'task_id': '8_R1', 'duration': 4}]}, 
	{'job_id': '9', 'map_tasks': [{'task_id': '9_M0', 'duration': 4}, {'task_id': '9_M1', 'duration': 3}, {'task_id': '9_M2', 'duration': 2}, {'task_id': '9_M3', 'duration': 1}], 'reduce_tasks': [{'task_id': '9_R0', 'duration': 1}, {'task_id': '9_R1', 'duration': 3}]}]

	number_of_requests=10
	arrivals = np.random.exponential(1, size=number_of_requests-1)
	request_number=0
	current_time=last_request_time=time.time() # time 0
	while request_number<number_of_requests:
		interval=arrivals[request_number-1]
		while True:
			if(time.time()-last_request_time>=interval):
				break
			time.sleep(0.01)
		print("interval: ",interval,"\n Job request :",job_requests_list[request_number])
		send_request(job_requests_list[request_number])
		last_request_time=time.time()
		request_number += 1

