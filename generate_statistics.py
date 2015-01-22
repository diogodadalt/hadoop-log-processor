#!/usr/bin/python

import sys
import json

MAX = 2000
DIVISOR = 1
START_TIME = 'startTime'
FINISH_TIME = 'finishTime'
SHUFFLE_FINISHED = 'shuffleFinished'
SORT_FINISHED = 'sortFinished'
TASK_TYPE = 'taskType'

def readJsonData(fileName):
	json_data=open(fileName)
	data = json.load(json_data)
	json_data.close()
	return data

def getBaseTime(data):
	baseTimeStamp = -1
	for key, value in data.iteritems():
		if baseTimeStamp == -1 or baseTimeStamp > long(value[START_TIME]):
			baseTimeStamp = long(value[START_TIME])
	return baseTimeStamp / DIVISOR

def removeTrailingZeros(list):
	while list and list[-1] is 0:
   		list.pop()

def saveInFile(filename, list):
	file = open(filename, 'w')
	for i, item in enumerate(list):
		file.write(str(i) + ' ' + str(item) + '\n')
	file.close()

def saveMapInfo(data, baseTime):
	amountOfTasksMap, amountOfTasksShuffle, amountOfTasksReduce = [], [], []
	for i in range(MAX):
		amountOfTasksMap.append(0)
		amountOfTasksShuffle.append(0)
		amountOfTasksReduce.append(0)
	for key, value in data.iteritems():
		start = int(long(value[START_TIME]) / DIVISOR - baseTime)
		end = int(long(value[FINISH_TIME]) / DIVISOR - baseTime)
		if value[TASK_TYPE] == 's':
			end = int(long(value[FINISH_TIME]) / DIVISOR - baseTime)
			print 'start: ' + str(start) + ', end: ' + str(end) + ', baseTime: ' + str(baseTime) + ', start: ' + value[START_TIME] + ', end: ' + value[FINISH_TIME]
		for i in range(start+1, end+1):
			if value[TASK_TYPE] == 'm':
				amountOfTasksMap[i] += 1
			elif value[TASK_TYPE] == 'r':
				amountOfTasksReduce[i] += 1
			elif value[TASK_TYPE] == 's':
				print 'index: ' + str(i)
				amountOfTasksShuffle[i] += 1 
	removeTrailingZeros(amountOfTasksMap)
	removeTrailingZeros(amountOfTasksShuffle)
	removeTrailingZeros(amountOfTasksReduce)
	amountOfTasksMap.append(0)
	amountOfTasksShuffle.append(0)
	amountOfTasksReduce.append(0)
	saveInFile('maps.out', amountOfTasksMap)
	saveInFile('shuffles.out', amountOfTasksShuffle)
	saveInFile('reduces.out', amountOfTasksReduce)

jobId = sys.argv[1]
data = readJsonData(jobId + '.json')
baseTimeStamp = getBaseTime(data)
saveMapInfo(data, baseTimeStamp)