import java.io.IOException;

//import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	final static Logger logger = Logger.getLogger(WordCountReducer .class);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
     /* int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      context.write(key, new IntWritable(sum));
    }
    */
    	
    	int sum = 0;
		for (IntWritable value : values)
		{
			sum+=value.get();
		}
		
		logger.info("reducer started");
			
		context.write(key, new IntWritable(sum) );
		
		
	} // end of reduce
  
  
     
  }
