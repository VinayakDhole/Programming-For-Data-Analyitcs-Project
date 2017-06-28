import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;



public class Task1Driver 
{
	final static Logger logger = Logger.getLogger(Task1Mapper.class);

	
    public static void main(String[] args) throws Exception 
    {
    	BasicConfigurator.configure();
		logger.info("starting driver");
        Configuration conf = new Configuration();
       
        conf.set("fs.defaultFS", "hdfs://localhost:54310");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Job job = Job.getInstance(conf, "Distinct Value");
        job.setJarByClass(Task1Driver.class);
        job.setMapperClass(Task1Mapper.class);
        job.setReducerClass(Task1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    	job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/hc"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/hc/output"));

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
