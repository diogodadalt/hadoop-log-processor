#!/usr/bin/python

import sys
import json

MAX = 2000
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
	return baseTimeStamp / 1000

def removeTrailingZeros(list):
	while list and list[-1] is 0:
   		list.pop()

def saveMapInfo(data, baseTime):
	amountOfTasksMap, amountOfTasksShuffle, amountOfTasksReduce = [], [], []
	for i in range(MAX):
		amountOfTasksMap.append(0)
		amountOfTasksShuffle.append(0)
		amountOfTasksReduce.append(0)
	for key, value in data.iteritems():
		if value[TASK_TYPE] == 'm':
			start = int(long(value[START_TIME]) / 1000 - baseTime)
		elif value[TASK_TYPE] == 'r':
			shuffleStart = int(long(value[START_TIME]) / 1000 - baseTime)
			for i in range(shuffleStart+1, start+1):
				amountOfTasksShuffle[i] += 1
			start = int(long(value[SHUFFLE_FINISHED]) / 1000 - baseTime)
		end = int(long(value[FINISH_TIME]) / 1000 - baseTime)
		for i in range(start+1, end+1):
			if value[TASK_TYPE] == 'm':
				amountOfTasksMap[i] += 1
			elif value[TASK_TYPE] == 'r':
				amountOfTasksReduce[i] += 1
	removeTrailingZeros(amountOfTasksMap)
	removeTrailingZeros(amountOfTasksShuffle)
	removeTrailingZeros(amountOfTasksReduce)
	mapsFile = open('maps.out', 'w')
	shufflesFile = open('shuffles.out', 'w')
	reducesFile = open('reduces.out', 'w')
	for i, item in enumerate(amountOfTasksMap):
		mapsFile.write(str(i) + ' ' + str(item) + '\n')
	for i, item in enumerate(amountOfTasksShuffle):
		shufflesFile.write(str(i) + ' ' + str(item) + '\n')
	for i, item in enumerate(amountOfTasksReduce):
		reducesFile.write(str(i) + ' ' + str(item) + '\n')	
	mapsFile.close()
	shufflesFile.close()
	reducesFile.close()

jobId = sys.argv[1]
data = readJsonData(jobId + '.json')
baseTimeStamp = getBaseTime(data)
saveMapInfo(data, baseTimeStamp)