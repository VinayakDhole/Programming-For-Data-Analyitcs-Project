import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



	public class SODistinctUserMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
	{
		
	    
	    
	    @Override
	    public void map(LongWritable key, Text value,
	                    Context context) throws IOException, InterruptedException
	    {
			
			
			    
			    
	    	String[] line = value.toString().split(",", 12);

	        // Ignore invalid lines
	        if (line.length != 12) {
	            System.out.println("- " + line.length);
	            return;
	        }

	        // The output `key` is the name of the region
	        String outputKey = line[2];

	        // The output `value` is the magnitude of the earthquake
	        double outputValue = Double.parseDouble(line[9]);
	        
	        // Record the output in the Context object
	        context.write(new Text(outputKey), new DoubleWritable(outputValue));
	    }
	}