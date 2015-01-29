#!/usr/bin/python

import sys
import re
import json
from datetime import datetime, timedelta
import collections
import glob

START_TIME = 'startTime'
FINISH_TIME = 'finishTime'
TASK_TYPE = 'taskType'
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
jobAndTaskRegex = (jobIdGroupRegex + '_(?P<taskType>' + taskTypeRegex + 
  ')_(?P<taskId>' + taskIdRegex + ')')
jobAndTaskRegex2 = jobIdRegex + '_' + taskTypeRegex + '_' + taskIdRegex
jobAndTaskReduceRegex = (jobIdGroupRegex + '_r_(?P<taskId>' + 
  taskIdRegex + ')')
datePartsGroupRegex = ('(?P<' + YEAR + '>' + yearRegex + ')-(?P<' + MONTH + 
  '>' + monthRegex + ')-(?P<' + DAY + '>' + dayRegex + ')')
timePartsGroupRegex = ('(?P<' + HOUR + '>' + hourRegex + '):(?P<' + MINUTE + 
  '>' + minuteRegex + '):(?P<' + SECOND + '>' + secondRegex + ')')
sourceGroupRegex = 'src: (?P<srcip>([\d.]+)):(?P<srcport>(\d+))'
destinationGroupRegex = 'dest: (?P<destip>([\d.]+)):(?P<destport>(\d+))'
sizeGroupRegex = 'bytes: (?P<size>(\d+))'
durationGroupRegex = 'duration: (?P<duration>(\d+))'
mapHeaderRegex = 'MapAttempt TASK_TYPE="MAP"'
taskIdGroupRegex = 'TASKID="task_' + jobAndTaskRegex2 + '"'
attemptGroupRegex = ('TASK_ATTEMPT_ID="attempt_' + jobAndTaskRegex + 
  '_(?P<attempt>(\d))"')
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
  ('\'attempt_' + jobAndTaskRegex +'_(?P<attempt>(\d))\' to tip task_' + 
    jobAndTaskRegex2),
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
  ('\'attempt_' + jobAndTaskRegex +'_(?P<attempt>(\d))\' to tip task_' + 
    jobAndTaskRegex2),
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
  ('Task \'attempt_' + jobAndTaskRegex + 
    '_(?P<attempt>(\d))\' has completed task_' + jobAndTaskRegex2 + 
    ' successfully.')
]

taskRemovedParts = [
  dateGroupRegex,
  ' ',
  timeGroupRegex,
  ',',
  numGroupRegex,
  ' ',
  ('INFO org.apache.hadoop.mapred.JobTracker: Removing task \'attempt_' + 
    jobAndTaskRegex + '_(?P<attempt>(\d))\'')
]

shuffleStartedParts = [
  dateGroupRegex,
  ' ',
  timeGroupRegex,
  ',',
  numGroupRegex,
  ' ',
  'INFO org.apache.hadoop.mapred.TaskTracker:',
  ' ',
  'attempt_' + jobAndTaskReduceRegex + '_(?P<attempt>(\d)) .*'
]

shuffleFinishedParts = [
  dateGroupRegex,
  ' ',
  timeGroupRegex,
  ',',
  numGroupRegex,
  ' ',
  'INFO org.apache.hadoop.mapred.TaskTracker:',
  ' ',
  ('Task attempt_' + jobAndTaskReduceRegex + '_(?P<attempt>(\d)) ' + 
    'is in commit-pending, task state:COMMIT_PENDING')
]

jobCompletedParts = [
  dateGroupRegex,
  ' ',
  timeGroupRegex,
  ',',
  numGroupRegex,
  ' ',
  ('INFO org.apache.hadoop.mapred.JobInProgress: Job job_' + 
    jobIdGroupRegex + ' has completed successfully.')
]

mapAddedPattern = re.compile(''.join(mapAddedParts))
reduceAddedPattern = re.compile(''.join(reduceAddedParts))
taskCompletedPattern = re.compile(''.join(taskCompletedParts))
taskRemovedPattern = re.compile(''.join(taskRemovedParts))
shuffleStartedPattern = re.compile(''.join(shuffleStartedParts))
shuffleFinishedPattern = re.compile(''.join(shuffleFinishedParts))
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
  return datetime(int(date[YEAR]), int(date[MONTH]), int(date[DAY]), 
    int(time[HOUR]), int(time[MINUTE]), int(time[SECOND]))

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

def updateDictionaryIfSmaller(dictionary, key, value):
  if (key not in dictionary or long(dictionary[key]) > long(value)):
    dictionary[key] = value

def updateDictionaryIfBigger(dictionary, key, value):
  if (key not in dictionary or long(dictionary[key]) < long(value)):
    dictionary[key] = value

def updateDictionary(dictionary, key, value):
  dictionary[key] = value

def extractFinishTime(dateStart, timeStart, duration):
  dtStart = getDatetime(dateStart, timeStart)
  dtEnd = dtStart +  timedelta(microseconds=long(float(duration)/1000.0))
  return str(long(totimestamp(dtEnd)))

def getMapFinishTime(data):
  finishTime = 0;
  for key, value in data.iteritems():
    if (value[TASK_TYPE] == 'm' and 
      long(value[FINISH_TIME]) > finishTime):
      finishTime = long(value[FINISH_TIME])
  return finishTime


def getInfoFromFile(file, jobId):
  tasks = {}
  with open(file) as openfileobject:
    for line in openfileobject:
      if re.search(jobId, line):
        match(line, mapAddedPattern, 
          lambda taskInfo: [
            updateDictionaryIfNone(tasks, key(taskInfo), taskInfo),
            updateDictionaryIfNone(taskInfo, START_TIME, 
              generateTimeStamp(taskInfo[DATE], taskInfo[TIME]))
          ]
        )
        match(line, reduceAddedPattern, 
          lambda taskInfo: [
            updateDictionaryIfNone(tasks, key(taskInfo), taskInfo),
            updateDictionaryIfNone(taskInfo, START_TIME, 
              generateTimeStamp(taskInfo[DATE], taskInfo[TIME]))
          ]
        )
        match(line, taskCompletedPattern, 
          lambda taskInfo:
            performIfHasKey(key(taskInfo), tasks, lambda: 
              updateDictionaryIfNone(tasks[key(taskInfo)], 
                FINISH_TIME, generateTimeStamp(taskInfo[DATE], 
                  taskInfo[TIME]))
            )
        )
  return tasks

def getShuffleInfoFromFile(file, jobId):
  tasks = {}
  with open(file) as openfileobject:
    for line in openfileobject:
      if re.search(jobId, line):
        match(line, shuffleStartedPattern, 
          lambda taskInfo: [
            updateDictionaryIfNone(tasks, key(taskInfo, 's'), 
              taskInfo),
            updateDictionaryIfNone(taskInfo, START_TIME, 
              generateTimeStamp(taskInfo[DATE], taskInfo[TIME]))
          ]
        )
        match(line, shuffleFinishedPattern, 
          lambda taskInfo: [
            performIfHasKey(key(taskInfo, 's'), tasks, lambda: [
                updateDictionaryIfNone(tasks[key(taskInfo, 's')], 
                  FINISH_TIME, generateTimeStamp(taskInfo[DATE], 
                    taskInfo[TIME])),
                updateDictionary(tasks[key(taskInfo, 's')], 
                TASK_TYPE, 's')
              ]
            )
          ]
        )
  return tasks

def fixReducesStartTime(data):
  mapFinishTime = getMapFinishTime(data);
  for key, value in data.iteritems():
    if (value[TASK_TYPE] == 'r' and 
      long(value[START_TIME]) < mapFinishTime):
      value[START_TIME] = str(mapFinishTime)
  return data

def saveInfo(file, data):
  with open(file, 'w') as outfile:
    json.dump(data, outfile)

jobTracker = sys.argv[1]
taskTracker = sys.argv[2]
jobId = sys.argv[3]

mapAndReduceData = fixReducesStartTime(getInfoFromFile(jobTracker, jobId))
shuffleData = getShuffleInfoFromFile(taskTracker, jobId)
data = collections.OrderedDict(sorted(mapAndReduceData.items() + 
  shuffleData.items()))
saveInfo(jobId + '.json', data)