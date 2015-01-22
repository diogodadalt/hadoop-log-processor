#!/usr/bin/python

import sys
import os

jobTrackerLog = sys.argv[1]
taskTrackerLog = sys.argv[2]
jobPrefix = sys.argv[3]
jobStart = sys.argv[4]
jobEnd = sys.argv[5]

for i in range(int(jobStart), int(jobEnd)+1):
	os.system('python parseJobTrackerAndTaskTracker.py ' + jobTrackerLog + ' ' + taskTrackerLog + ' ' + str(jobPrefix) + '_' + "{0:04d}".format(i))
	os.system('python generate_statistics.py ' + str(jobPrefix) + '_' + "{0:04d}".format(i))
	os.system('gnuplot graphics.gp')
	os.system('mv imagem.png imagem' + str(jobPrefix) + '_' + "{0:04d}".format(i) + '.png')
	os.system('rm maps.out shuffles.out reduces.out ' + str(jobPrefix) + '_' + "{0:04d}".format(i) + '.json')