#!/usr/bin/python

import sys
import os

folder = sys.argv[1]
jobPrefix = sys.argv[2]
jobStart = sys.argv[3]
jobEnd = sys.argv[4]

for i in range(int(jobStart), int(jobEnd)+1):
	os.system('python parseJobInfo.py ' + folder + ' ' + str(jobPrefix) + '_' + "{0:04d}".format(i))
	os.system('python generate_statistics.py ' + str(jobPrefix) + '_' + "{0:04d}".format(i))
	os.system('gnuplot graphics.gp')
	os.system('mv imagem.png imagem' + str(jobPrefix) + '_' + "{0:04d}".format(i) + '.png')
	os.system('rm maps.out shuffles.out reduces.out ' + str(jobPrefix) + '_' + "{0:04d}".format(i) + '.json')
