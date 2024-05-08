package cn.catman;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text> { 
    @Override
    protected void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException
    { 
        
        Text edge = new Text(), direction = new Text();

        StringTokenizer itr = new StringTokenizer(value.toString(), ", \n\t\r\f");
        
        
        for(; itr.hasMoreTokens(); ) {
            String p1 = itr.nextToken();
            assert itr.hasMoreTokens();
            String p2 = itr.nextToken();
            
            if (p1.compareTo(p2) < 0) {
                edge.set(p1 + "@" + p2);
                direction.set("+");
            } else if(p1.compareTo(p2) > 0) {
                edge.set(p2 + "@" + p1);
                direction.set("-");
            } else {
                continue;
            }
            context.write(edge, direction);    
        }
    }
}