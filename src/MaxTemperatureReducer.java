
import java.io.IOException;
import static jdk.nashorn.internal.objects.NativeMath.max;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ujjwal
 */
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
  
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
        int maxValue = Integer.MIN_VALUE;
       for(IntWritable i: values){
          maxValue = Integer.max(maxValue, i.get());
       }
       context.write(key,new IntWritable(maxValue));
    }
         
}
