package cn.catman;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LinkMakerReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException { 
        
        Iterator<Text> it = values.iterator();

        String p1 = key.toString();
        Text src = new Text(), tgt = new Text();
        ArrayList<String> targets = new ArrayList<>();
        

        while(it.hasNext()) {
            String p2 = it.next().toString();
            src.set(p1 + "@" + p2);
            tgt.set("-1");

            context.write(src, tgt);
            
            targets.add(p2);    
        }

        while(targets.size() >= 2) {
            String linkSrc = targets.remove(0);
            for (String linkTgt: targets) {
                if (linkSrc.compareTo(linkTgt) < 0) {
                    src.set(linkSrc + "@" + linkTgt);
                } else {
                    assert linkSrc.compareTo(linkTgt) > 0;
                    src.set(linkTgt + "@" + linkSrc);
                }
                
                tgt.set(p1);
                context.write(src, tgt);
            }
        }
    }
}
