package cn.catman;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class FinalCounterMapper extends Mapper<LongWritable, Text, Text, Text> { 
    @Override
    protected void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException
    { 
        StringTokenizer itr = new StringTokenizer(value.toString(), ", \n\t\r\f");
        int count = 0;
        while (itr.hasMoreTokens()) {
            String nextToken = itr.nextToken();
            int c = Integer.parseInt(nextToken);
            count += c;
        }

        if (count > 0) {
            context.write(new Text(), new Text(Integer.toString(count)));
        }
    }
}