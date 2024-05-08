package cn.catman;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class LinkMakerMapper extends Mapper<LongWritable, Text, Text, Text> { 
    @Override
    protected void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException
    { 
        
        Text src = new Text(), tgt = new Text();

        StringTokenizer itr = new StringTokenizer(value.toString(), ", \n\t\r\f");
        
        for(; itr.hasMoreTokens(); ) {
            String p1 = itr.nextToken();
            assert itr.hasMoreTokens();
            String p2 = itr.nextToken();
            
            assert p1.compareTo(p2) < 0;
            
            src.set(p1);
            tgt.set(p2);
            context.write(src, tgt);    
        }
    }
}