

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class WordCountDriver {

	final static Logger logger = Logger.getLogger(WordCountMapper.class);

	public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();
		logger.info("starting driver");
		Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );

		Job job = Job.getInstance(conf, "Distinct Value");
		//Job job = new Job(conf, "StackOverflow Distinct Users");
		
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		//job.setCombinerClass(SODistinctUserReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		//&&&&&&&&&&&&&&&&&&&&&&
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//^^^^^^^^^^^^^^^^^^^^^^^^^^
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/hc/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/hc/output1"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
