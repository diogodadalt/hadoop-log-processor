#!/usr/bin/python

import sys
import re
import json
from datetime import datetime, timedelta
import collections
import glob

YEAR = 'year'
MONTH = 'month'
DAY = 'day'
HOUR = 'hour'
MINUTE = 'minute'
SECOND = 'second'
DATE = 'date'
TIME = 'time'
yearRegex = '[0-9]{4}'
monthRegex = '(0[1-9]|1[0-2])'
dayRegex = '(0[1-9]|[1-2][0-9]|3[0-1])'
hourRegex = '(2[0-3]|[01][0-9])'
minuteRegex = '[0-5][0-9]'
secondRegex = '[0-5][0-9]'
dateRegex = yearRegex + '-' + monthRegex + '-' + dayRegex
timeRegex = hourRegex + ':' + minuteRegex + ':' + secondRegex
numRegex = '[0-9]{3}'
jobIdRegex = '(\d+_\d+)'
taskIdRegex = '(\d+)'
taskTypeRegex = '([a-z])'
dateGroupRegex = '(?P<' + DATE + '>' + dateRegex + ')'
timeGroupRegex = '(?P<' + TIME + '>' + timeRegex + ')'
numGroupRegex = '(?P<num>' + numRegex + ')'
jobIdGroupRegex = '(?P<jobId>' + jobIdRegex + ')'
jobAndTaskRegex = jobIdGroupRegex + '_(?P<taskType>' + taskTypeRegex + ')_(?P<taskId>' + taskIdRegex + ')'
jobAndTaskRegex2 = jobIdRegex + '_' + taskTypeRegex + '_' + taskIdRegex
datePartsGroupRegex = '(?P<' + YEAR + '>' + yearRegex + ')-(?P<' + MONTH + '>' + monthRegex + ')-(?P<' + DAY + '>' + dayRegex + ')'
timePartsGroupRegex = '(?P<' + HOUR + '>' + hourRegex + '):(?P<' + MINUTE + '>' + minuteRegex + '):(?P<' + SECOND + '>' + secondRegex + ')'
mapHeaderRegex = 'MapAttempt TASK_TYPE="MAP"'
taskIdGroupRegex = 'TASKID="task_' + jobAndTaskRegex2 + '"'
attemptGroupRegex = 'TASK_ATTEMPT_ID="attempt_' + jobAndTaskRegex + '_(?P<attempt>(\d))"'
startTimeGroupRegex = 'START_TIME="(?P<startTime>(\d+))"'
taskStatusGroupRegex = 'TASK_STATUS="SUCCESS"'
finishTimeGroupRegex = 'FINISH_TIME="(?P<finishTime>(\d+))"'
reduceHeaderRegex = 'ReduceAttempt TASK_TYPE="REDUCE"'
shuffleFinishedGroupRegex = 'SHUFFLE_FINISHED="(?P<shuffleFinished>(\d+))"'
sortFinishedGroupRegex = 'SORT_FINISHED="(?P<sortFinished>(\d+))"'

mapStartedParts = [
	mapHeaderRegex,
	' ',
	taskIdGroupRegex,
	' ',
	attemptGroupRegex,
	' ',
	startTimeGroupRegex,
	' ',
	'.*'
]

mapFinishedParts = [
	mapHeaderRegex,
	' ',
	taskIdGroupRegex,
	' ',
	attemptGroupRegex,
	' ',
	taskStatusGroupRegex,
	' ',
	finishTimeGroupRegex,
	' ',
	'.*'
]

reduceStartedParts = [
	reduceHeaderRegex,
	' ',
	taskIdGroupRegex,
	' ',
	attemptGroupRegex,
	' ',
	startTimeGroupRegex,
	' ',
	'.*'
]

reduceFinishedParts = [
	reduceHeaderRegex,
	' ',
	taskIdGroupRegex,
	' ',
	attemptGroupRegex,
	' ',
	taskStatusGroupRegex,
	' ',
	shuffleFinishedGroupRegex,
	' ',
	sortFinishedGroupRegex,
	' ',
	finishTimeGroupRegex,
	' ',
	'.*'
]

taskAddedParts = [
	dateGroupRegex,
	' ',
	timeGroupRegex,
	',',
	numGroupRegex,
	' ',
	'INFO org.apache.hadoop.mapred.JobTracker: Adding task \((?P<operation>([A-Z_])\w+)\)',
	' ',
	'\'attempt_' + jobAndTaskRegex +'_(?P<attempt>(\d))\' to tip task_' + jobAndTaskRegex2,
	', ',
	'for tracker \'(?P<tracker>.*)\''
]

taskCompletedParts = [
	dateGroupRegex,
	' ',
	timeGroupRegex,
	',',
	numGroupRegex,
	' ',
	'INFO org.apache.hadoop.mapred.JobInProgress:',
	' ',
	'Task \'attempt_' + jobAndTaskRegex + '_(?P<attempt>(\d))\' has completed task_' + jobAndTaskRegex2 + ' successfully.'
]

taskRemovedParts = [
	dateGroupRegex,
	' ',
	timeGroupRegex,
	',',
	numGroupRegex,
	' ',
	'INFO org.apache.hadoop.mapred.JobTracker: Removing task \'attempt_' + jobAndTaskRegex + '_(?P<attempt>(\d))\''
]

jobCompletedParts = [
	dateGroupRegex,
	' ',
	timeGroupRegex,
	',',
	numGroupRegex,
	' ',
	'INFO org.apache.hadoop.mapred.JobInProgress: Job job_' + jobIdGroupRegex + ' has completed successfully.'
]

mapStartedPattern = re.compile(''.join(mapStartedParts))
mapFinishedPattern = re.compile(''.join(mapFinishedParts))
reduceStartedPattern = re.compile(''.join(reduceStartedParts))
reduceFinishedPattern = re.compile(''.join(reduceFinishedParts))
taskAddedPattern = re.compile(''.join(taskAddedParts))
taskCompletedPattern = re.compile(''.join(taskCompletedParts))
taskRemovedPattern = re.compile(''.join(taskRemovedParts))
jobCompletedPattern = re.compile(''.join(jobCompletedParts))
datePartsPattern = re.compile(''.join(datePartsGroupRegex))
timePartsPattern = re.compile(''.join(timePartsGroupRegex))

def totimestamp(dt, epoch=datetime(1970,1,1)):
    td = dt - epoch
    # return td.total_seconds()
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 1e6

def match(line, pattern, handler):
	m = pattern.match(line)
	if m is not None:
		handler(m.groupdict())

def key(dic):
	return dic['taskId'] + '-' + dic['taskType']

def updateDictionaryIfNone(dictionary, key, value):
	if (key not in dictionary):
		dictionary[key] = value

def getInfoFromFile(file, jobId):
	tasks = {}
	with open(file) as openfileobject:
		for line in openfileobject:
			if re.search(jobId, line):
				match(line, mapStartedPattern, 
					lambda taskInfo:
						updateDictionaryIfNone(tasks, key(taskInfo), taskInfo)
				)
				match(line, mapFinishedPattern, 
					lambda taskInfo:
						updateDictionaryIfNone(tasks[key(taskInfo)], 'finishTime', taskInfo['finishTime'])
					)
				match(line, reduceStartedPattern, 
					lambda taskInfo:
						updateDictionaryIfNone(tasks, key(taskInfo), taskInfo)
				)
				match(line, reduceFinishedPattern, 
					lambda taskInfo: [
							updateDictionaryIfNone(tasks[key(taskInfo)], 'finishTime', taskInfo['finishTime']),
							updateDictionaryIfNone(tasks[key(taskInfo)], 'shuffleFinished', taskInfo['shuffleFinished']),
							updateDictionaryIfNone(tasks[key(taskInfo)], 'sortFinished', taskInfo['sortFinished'])
						]
					)
	return tasks

def saveInfo(file, data):
	with open(file, 'w') as outfile:
		json.dump(data, outfile)

folder = sys.argv[1]
jobId = sys.argv[2]
logFile = ''
logFileRegex = folder + 'job_' + jobId + '*'
logFileDoNotOpenPattern = re.compile(folder + 'job_' + jobId + '(.*)(\.)(.*)')

for name in glob.glob(logFileRegex):
	if logFileDoNotOpenPattern.match(name) is None:
		logFile = name

data = getInfoFromFile(logFile, jobId)
sortedData = collections.OrderedDict(sorted(data.items()))
saveInfo(jobId + '.json', sortedData)