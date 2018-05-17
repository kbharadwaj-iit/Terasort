#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --output=8gb-log.log
#SBATCH --wait-all-nodes=1



hadoop jar DriverTotalOrderSorting.jar DriverTotalOrderSorting /input/data-8GB /user/kbharadwaj/part-hadoop /user/kbharadwaj/stg-hadoop /user/kbharadwaj/output-hadoop


hadoop jar /opt/hadoop-2.9.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.0.jar teravalidate /user/kbharadwaj/output-hadoop /user/kbharadwaj/report-hadoop


hadoop fs -get /user/kbharadwaj/report-hadoop/part-r-00000

mv part-r-00000 hadoop-8gb