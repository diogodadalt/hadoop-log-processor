# hadoop-log-processor

This is scripts are intended to enable the retrieval of information from map, shuffle and reduce tasks.

### Requirements
* Linux
* python
* gnuplot

### Usage
python generate_graphics.py [1] [2] [3] [4]
[1] folder in which the logs are contained
[2] job id prefix. Ex.: in job_201101221816_0005 the prefix is 201101221816
[3] first job id
[4] last job id (this enables the processing of a range of jobs)

### Example
python generate_graphics.py ~/Desktop/masters/Experimentos-G5k-01-2015/log_nancy.9gb/opt/hadoop/logs/history/done/version-1/graphite-2.nancy.grid5000.fr_1420880670188_/2015/01/10/000000/ 201501100904 1 30