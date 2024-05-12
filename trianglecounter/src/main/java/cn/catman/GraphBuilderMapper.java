package cn.catman;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text> { 
    @Override
    protected void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException
    { 
        
        String p1 = key.toString();
        String p2 = value.toString();
        Text edge = new Text();
        Text direction = new Text();

        if (p1.compareTo(p2) < 0) {
            edge.set(p1 + "@" + p2);
            direction.set("+");
            context.write(edge, direction);   
        } else if(p1.compareTo(p2) > 0) {
            edge.set(p2 + "@" + p1);
            direction.set("-");
            context.write(edge, direction);   
        } 
    }
}