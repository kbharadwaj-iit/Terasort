APP = DriverTotalOrderSorting

make:
	javac -classpath $$(hadoop classpath)  $(APP).java 
	jar -cvf $(APP).jar *.class

hadoop-8g:
	sbatch hadoop-8gb.slurm

hadoop-20g:
	sbatch hadoop-20gb.slurm

hadoop-80g:
	sbatch hadoop-80gb.slurm

clean: 
	rm -rf *.jar
