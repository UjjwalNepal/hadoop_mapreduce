
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ujjwal
 */
public class MaxTemperature {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
       
        // TODO code application logic here
        
        Job job = new Job();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("max temperature");
            
        FileInputFormat.addInputPath(job, new Path("/user/hadoop/temperature.txt"));
        FileOutputFormat.setOutputPath(job,new Path("output.txt"));
        
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
    
}
