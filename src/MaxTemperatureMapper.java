
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ujjwal
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    
    
    
    @Override
    
    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
        
        String line = value.toString();
        String year = line.substring(15,19);
        int airTemperature = Integer.parseInt(line.substring(42,45));
        context.write(new Text(year), new IntWritable(airTemperature));
        
    }
    
}
