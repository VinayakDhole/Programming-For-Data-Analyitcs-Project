import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class Task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

    private final static IntWritable one = new IntWritable(1);
    final static Logger logger = Logger.getLogger(Task1Mapper.class);

            public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException
                    {
                Text outputkey = new Text();
               //IntWritable outputvalue = new IntWritable();
                String[] arrLine = value.toString().split(",");
               
                
                String field16;
               
               
                field16=arrLine[15];
                //outputvalue.set(Integer.parseInt(field16));
               
               
                outputkey.set(field16);
                
               
                context.write(outputkey,one);
               
               
        }
           
}