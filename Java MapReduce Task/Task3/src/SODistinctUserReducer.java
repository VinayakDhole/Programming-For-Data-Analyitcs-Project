import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;




	public  class SODistinctUserReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable>
	{
		
		
		@Override
		
		
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException
		{
			
			// Standard algorithm for finding the max value
	        double maxMagnitude=Double.MIN_VALUE ;
	        for (DoubleWritable value : values) {
	            maxMagnitude = Math.max(maxMagnitude, value.get());
	        }
	        
	        context.write(key, new DoubleWritable(maxMagnitude));
	    }
	}
