# hadoop-log-processor

These scripts are intended to enable the retrieval of information from map, shuffle and reduce tasks, regarding execution time.

### Requirements
* Linux
* python
* gnuplot

### Usage
```
python generate_graphics.py [1] [2] [3] [4] [5]
```
[1] job tracker file path
[2] task tracker file path
[3] job id prefix. Ex.: in job_201101221816_0005 the prefix is 201101221816
[4] first job id
[5] last job id (this enables the processing of a range of jobs)

### Example
```
./generate_graphics.py hadoop-root-jobtracker-graphite-2.nancy.grid5000.fr.log hadoop-root-tasktracker-graphite-2.nancy.grid5000.fr.log 201501100904 1 30
```