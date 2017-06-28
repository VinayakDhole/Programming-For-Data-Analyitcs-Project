import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	
    private final static IntWritable one = new IntWritable(1);
    final static Logger logger = Logger.getLogger(WordCountMapper.class);

            public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException
                    {
                Text outputkey = new Text();
                
              IntWritable outputvalue = new IntWritable();
                String[] arrLine = value.toString().split(",");
               
                String field1;
                String field6;
                String field24;
               
                field1=arrLine[0];
                field6=arrLine[5];
              //  outputvalue.set(parseInt.(field6));
                

				 field24 = field1.concat(field6);
				
			    //context.write(new Text(field24),one);
               
               
                outputkey.set(field24);
                
               
                context.write(outputkey,one);
               
               
        }
           
}