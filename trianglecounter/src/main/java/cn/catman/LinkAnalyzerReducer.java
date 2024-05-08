package cn.catman;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LinkAnalyzerReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException { 
        
        Iterator<Text> it = values.iterator();
        
        int count = 0;
        int linkedDirectly = 0;
        while (it.hasNext()) {
            String mid = it.next().toString();
            if (mid.equals("-1")) {
                linkedDirectly = 1;
            } else {
                count ++;
            }
        }


        if(linkedDirectly == 1) {
            context.write(new Text(Integer.toString(count)), new Text());
        }
    }
    
    
}
