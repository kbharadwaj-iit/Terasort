#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --output=spark-80g.log
#SBATCH --wait-all-nodes=1



spark-submit --class spark --master yarn --deploy-mode client --driver-memory 4g --executor-memory 4g --executor-cores 4 --num-executors 2 spark.jar /input/data-80GB /user/kbharadwaj/output-spark


hadoop jar /opt/hadoop-2.9.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.0.jar teravalidate /user/kbharadwaj/output-spark /user/kbharadwaj/report-spark


hadoop fs -get /user/kbharadwaj/report-spark/part-r-00000

mv part-r-00000 spark-80g