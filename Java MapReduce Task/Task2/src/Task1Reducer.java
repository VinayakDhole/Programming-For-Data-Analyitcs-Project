import java.io.IOException;



import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;



public class Task1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	final static Logger logger = Logger.getLogger(Task1Reducer .class);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	int sum = 0;
        for (IntWritable value : values) {
        	sum+=value.get();
      }
		logger.info("reducer started");

        context.write(key, new IntWritable(sum));
    }
  }
