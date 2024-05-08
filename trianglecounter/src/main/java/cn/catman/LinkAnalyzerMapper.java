package cn.catman;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class LinkAnalyzerMapper extends Mapper<LongWritable, Text, Text, Text> { 
    @Override
    protected void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException
    { 
        
        Text link = new Text(), mid = new Text();

        StringTokenizer itr = new StringTokenizer(value.toString(), ", \n\t\r\f");
        
        
        for(; itr.hasMoreTokens(); ) {
            String p1p2 = itr.nextToken().toString();
            assert itr.hasMoreTokens();
            String middle = itr.nextToken().toString();

            link.set(p1p2);
            mid.set(middle);

            context.write(link, mid);
        }
    }
}