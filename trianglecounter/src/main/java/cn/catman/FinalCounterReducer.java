package cn.catman;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FinalCounterReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException { 
        
        Iterator<Text> it = values.iterator();
        
        int count = 0;
        while(it.hasNext()) {
            int c = Integer.parseInt(it.next().toString());
            count += c;
        }
        
        context.write(new Text(), new Text(Integer.toString(count)));
    }
}
