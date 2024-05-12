package cn.catman;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException { 
        
        Iterator<Text> it = values.iterator();
        Boolean hasPostive = false, hasNegative = false;
        
        while(it.hasNext()) {
            String dir = it.next().toString();
            if (dir.equals("+")) {
                hasPostive = true;
            } else if (dir.equals("-")){
                hasNegative = true;
            } 
        }
        if (hasPostive && hasNegative) {
            String[] src_dst = key.toString().split("@");
            assert src_dst.length == 2;
            context.write(new Text(src_dst[0]), new Text(src_dst[1]));
        }
    }
}
