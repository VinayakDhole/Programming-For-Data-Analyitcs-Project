import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;


public class DistinctUserDriver {
	
	public static void main(String[] args) throws Exception {
        //if (args.length != 2) {
          //  System.err.println("Usage: hadoopex <input path> <output path>");
            //System.exit(-1);
       // }
        Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
        
        // Create the job specification object
		Job job = new Job(conf, "StackOverflow Distinct Users");

		
		job.setJarByClass(DistinctUserDriver.class);
		job.setMapperClass(SODistinctUserMapper.class);
		job.setReducerClass(SODistinctUserReducer.class);


        // Setup input and output paths
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/hc"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/hc/output3"));

        

        // Specify the type of output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

        // Wait for the job to finish before terminating
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
