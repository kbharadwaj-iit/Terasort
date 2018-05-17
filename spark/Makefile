APP = spark

make:
	javac -classpath /opt/spark-2.3.0-bin-hadoop2.7/jars/spark-core_2.11-2.3.0.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/spark-sql_2.11-2.3.0.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/scala-compiler-2.11.8.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/scala-library-2.11.8.jar $(APP).java
	jar -cvf $(APP).jar *.class

spark-8g:
	sbatch spark-8g.slurm

spark-20g:
	sbatch spark-20g.slurm

spark-80g:
	sbatch spark-80g.slurm

clean: 
	rm -rf *.jar
	rm -rf *.class

