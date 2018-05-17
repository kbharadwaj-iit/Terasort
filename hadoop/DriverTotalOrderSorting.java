import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class DriverTotalOrderSorting {
    
  	public static class TerasortMapper extends Mapper<Object, Text, Text, Text> {
        Text outkey = new Text();
        Text outval = new Text();
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String input = value.toString();

			 outkey.set(input.substring(0, 10));
			 outval.set(input.substring(10));
			context.write(outkey, outval);
		}

	}

	public static class TeraReducer extends Reducer<Text, Text, Text, Text> {
         Text outkey = new Text();
        Text outval = new Text();
		protected void reduce(Text key, Iterable<Text> vals, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 outkey = key;
			
			for (Text val : vals) {
				outval = val;
			}
			context.write(outkey, outval);
		}
	}

public static void main(String[] args)  throws Exception {


long s1 = System.currentTimeMillis();
FileUtils.deleteDirectory(new File(args[1]));
FileUtils.deleteDirectory(new File(args[2]));
FileUtils.deleteDirectory(new File(args[3]));
Configuration conf = new Configuration();
Path inputPath = new Path(args[0]);
Path partitionFile = new Path(args[1]);
Path outputStage = new Path(args[2]);
Path outputOrder = new Path(args[3]);
Job sampleJob = Job.getInstance(conf);
sampleJob.setJobName("first map");
sampleJob.setJarByClass(DriverTotalOrderSorting.class);
sampleJob.setMapperClass(TerasortMapper.class);
//sampleJob.setNumMapTasks(10);
sampleJob.setNumReduceTasks(0);
sampleJob.setOutputKeyClass(Text.class);
sampleJob.setOutputValueClass(Text.class);
TextInputFormat.setInputPaths(sampleJob, inputPath);
sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
TextOutputFormat.setOutputPath(sampleJob, outputStage);
System.out.println("done with mapping");
int code = sampleJob.waitForCompletion(true) ? 0 : 1;

if (code == 0) {
    System.out.println("starting second iteration");
Job orderJob = Job.getInstance(conf);
orderJob.setJarByClass(DriverTotalOrderSorting.class);
orderJob.setJobName("total order sort");
// Identity mapper to output the key/value pairs in the SequenceFile
orderJob.setMapperClass(Mapper.class);
orderJob.setReducerClass(TeraReducer.class);
orderJob.setNumReduceTasks(40);
orderJob.getConfiguration().set("mapred.min.split.size", "536870912"); 
orderJob.getConfiguration().set("mapred.max.split.size", "536870912");
// Use Hadoop's TotalOrderPartitioner class
orderJob.setPartitionerClass(TotalOrderPartitioner.class);
// Set the partition file
orderJob.getConfiguration().set("mapreduce.map.output.compress", "true");
TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);
System.out.println("here i am");
orderJob.setOutputKeyClass(Text.class);
orderJob.setOutputValueClass(Text.class);
// Set the input to the previous job's output
orderJob.setInputFormatClass(SequenceFileInputFormat.class);
TextInputFormat.setInputPaths(orderJob, outputStage);
// Set the output path to the command line parameter
TextOutputFormat.setOutputPath(orderJob, outputOrder);
orderJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
// Use the InputSampler to go through the output of the previous
// job, sample it, and create the partition file
double sample = 20d;
System.out.println("this is me");
      InputSampler.Sampler<Text, Text> sampler =
          new InputSampler.RandomSampler<Text, Text>(0.1, 10000, 10);
     
      InputSampler.writePartitionFile(orderJob, sampler);

      // Add to DistributedCache
    //  Configuration conf = orderJob.getConfiguration();
      /* String part = TotalOrderPartitioner.getPartitionFile(conf);*/
       URI partitionUri = new URI(partitionFile.toString() +
                               "#" + "_partition.lst");
    //  orderJob.addCacheFile(partitionUri); 
      DistributedCache.addCacheFile(partitionUri, orderJob.getConfiguration());
    DistributedCache.createSymlink(orderJob.getConfiguration());
// Submit the job
System.out.println("nowhere else on earth id rather be");
code = orderJob.waitForCompletion(true) ? 0 : 1;
}
long s2 = System.currentTimeMillis();
double time = ((double) s2 - s1) / 1000.0;
System.out.println("Total time to sort data on hadoop: " + time + " seconds");
System.exit(code);

}
}