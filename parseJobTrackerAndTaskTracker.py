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
DURATION = 'duration'
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
sourceGroupRegex = 'src: (?P<srcip>([\d.]+)):(?P<srcport>(\d+))'
destinationGroupRegex = 'dest: (?P<destip>([\d.]+)):(?P<destport>(\d+))'
sizeGroupRegex = 'bytes: (?P<size>(\d+))'
durationGroupRegex = 'duration: (?P<duration>(\d+))'
mapHeaderRegex = 'MapAttempt TASK_TYPE="MAP"'
taskIdGroupRegex = 'TASKID="task_' + jobAndTaskRegex2 + '"'
attemptGroupRegex = 'TASK_ATTEMPT_ID="attempt_' + jobAndTaskRegex + '_(?P<attempt>(\d))"'
startTimeGroupRegex = 'START_TIME="(?P<startTime>(\d+))"'
taskStatusGroupRegex = 'TASK_STATUS="SUCCESS"'
finishTimeGroupRegex = 'FINISH_TIME="(?P<finishTime>(\d+))"'
reduceHeaderRegex = 'ReduceAttempt TASK_TYPE="REDUCE"'
shuffleFinishedGroupRegex = 'SHUFFLE_FINISHED="(?P<shuffleFinished>(\d+))"'
sortFinishedGroupRegex = 'SORT_FINISHED="(?P<sortFinished>(\d+))"'

mapAddedParts = [
	dateGroupRegex,
	' ',
	timeGroupRegex,
	',',
	numGroupRegex,
	' ',
	'INFO org.apache.hadoop.mapred.JobTracker: Adding task \(MAP\)',
	' ',
	'\'attempt_' + jobAndTaskRegex +'_(?P<attempt>(\d))\' to tip task_' + jobAndTaskRegex2,
	', ',
	'for tracker \'(?P<tracker>.*)\''
]

reduceAddedParts = [
	dateGroupRegex,
	' ',
	timeGroupRegex,
	',',
	numGroupRegex,
	' ',
	'INFO org.apache.hadoop.mapred.JobTracker: Adding task \(REDUCE\)',
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

shuffleTaskParts = [
	dateGroupRegex,
	' ',
	timeGroupRegex,
	',',
	numGroupRegex,
	' ',
	'INFO org.apache.hadoop.mapred.TaskTracker.clienttrace:',
	' ',
	sourceGroupRegex,
	', ',
	destinationGroupRegex,
	', ',
	sizeGroupRegex,
	', ',
	'op: MAPRED_SHUFFLE, cliID: attempt_' + jobAndTaskRegex + '_(?P<attempt>(\d))',
	', ',
	durationGroupRegex
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

mapAddedPattern = re.compile(''.join(mapAddedParts))
reduceAddedPattern = re.compile(''.join(reduceAddedParts))
taskCompletedPattern = re.compile(''.join(taskCompletedParts))
taskRemovedPattern = re.compile(''.join(taskRemovedParts))
shuffleTaskPattern = re.compile(''.join(shuffleTaskParts))
jobCompletedPattern = re.compile(''.join(jobCompletedParts))
datePartsPattern = re.compile(''.join(datePartsGroupRegex))
timePartsPattern = re.compile(''.join(timePartsGroupRegex))

def totimestamp(dt, epoch=datetime(1970,1,1)):
    td = dt - epoch
    # return td.total_seconds()
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 1e6

def getDatetime(date, time):
	date = datePartsPattern.match(date).groupdict()
	time = timePartsPattern.match(time).groupdict()
	return datetime(int(date[YEAR]), int(date[MONTH]), int(date[DAY]), int(time[HOUR]), int(time[MINUTE]), int(time[SECOND]))

def formatTimeStamp(timestamp):
	return str(long(timestamp))

def generateTimeStamp(date, time):
	return formatTimeStamp(totimestamp(getDatetime(date, time)))

def match(line, pattern, handler):
	m = pattern.match(line)
	if m is not None:
		handler(m.groupdict())

def key(dic, suffix=None):
	if suffix is None:
		return dic['taskId'] + '-' + dic['taskType']
	else:
		return dic['taskId'] + '-' + suffix


def performIfHasKey(key, dic, func):
	if key in dic:
		func()

def updateDictionaryIfNone(dictionary, key, value):
	if (key not in dictionary):
		dictionary[key] = value

def updateDictionary(dictionary, key, value):
	dictionary[key] = value

def extractFinishTime(dateStart, timeStart, duration):
	dtStart = getDatetime(dateStart, timeStart)
	dtEnd = dtStart + timedelta(microseconds=long(duration))
	return str(long(totimestamp(dtEnd)))

def getInfoFromFile(file, jobId):
	tasks = {}
	with open(file) as openfileobject:
		for line in openfileobject:
			if re.search(jobId, line):
				match(line, mapAddedPattern, 
					lambda taskInfo: [
						updateDictionaryIfNone(tasks, key(taskInfo), taskInfo),
						updateDictionaryIfNone(taskInfo, 'startTime', generateTimeStamp(taskInfo[DATE], taskInfo[TIME]))
					]
				)
				match(line, reduceAddedPattern, 
					lambda taskInfo: [
						updateDictionaryIfNone(tasks, key(taskInfo), taskInfo),
						updateDictionaryIfNone(taskInfo, 'startTime', generateTimeStamp(taskInfo[DATE], taskInfo[TIME]))
					]
				)
				match(line, taskCompletedPattern, 
					lambda taskInfo:
						performIfHasKey(key(taskInfo), tasks, lambda: 
							updateDictionaryIfNone(tasks[key(taskInfo)], 'finishTime', generateTimeStamp(taskInfo[DATE], taskInfo[TIME]))
						)							
				)
				match(line, shuffleTaskPattern, 
					lambda taskInfo: [
						updateDictionaryIfNone(tasks, key(taskInfo, 's'), taskInfo),
						updateDictionaryIfNone(taskInfo, 'startTime', generateTimeStamp(taskInfo[DATE], taskInfo[TIME])),
						updateDictionaryIfNone(taskInfo, 'finishTime', extractFinishTime(taskInfo[DATE], taskInfo[TIME], taskInfo[DURATION])),
						updateDictionary(tasks[key(taskInfo, 's')], 'taskType', 's')
					]
				)
	return tasks

def saveInfo(file, data):
	with open(file, 'w') as outfile:
		json.dump(data, outfile)

jobTracker = sys.argv[1]
taskTracker = sys.argv[2]
jobId = sys.argv[3]


mapAndReduceData = getInfoFromFile(jobTracker, jobId)
shuffleData = getInfoFromFile(taskTracker, jobId)
#mapAndReduceSortedData = collections.OrderedDict(sorted(mapAndReduceData.items()))
#shuffleSortedData = collections.OrderedDict(sorted(shuffleData.items()))
data = collections.OrderedDict(sorted(mapAndReduceData.items() + shuffleData.items()))
saveInfo(jobId + '.json', data)