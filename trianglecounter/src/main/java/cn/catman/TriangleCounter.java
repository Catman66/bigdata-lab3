package cn.catman;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.state.Graph;

import com.google.common.io.Files;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.nn_005fbrowsedfscontent_jsp;


//             Configuration conf = new Configuration();

//             Job job = Job.getInstance(conf);
//             job.setJobName("invert index");

            
//             job.setJarByClass(InvertedIndexer.class);
//             job.setInputFormatClass(TextInputFormat.class);
//             job.setMapperClass(InvertedIndexMapper.class);
//             job.setReducerClass(InvertedIndexReducer.class);
//             job.setOutputKeyClass(Text.class);
//             job.setOutputValueClass(Text.class);

//             FileInputFormat.addInputPath(job, new Path(args[0]));
//             FileOutputFormat.setOutputPath(job, new Path(args[1]));
//             System.exit(job.waitForCompletion(true) ? 0 : 1);
//         } catch (Exception e) { e.printStackTrace(); }
//     }
// }


public class TriangleCounter {
    public static void main(String[] args) {
        try {
            String input = args[0];
            String output = args[1];
            String nonDir = output + "/nonDirect";
            String link =   output + "/link";
            String counts = output + "/counts";
            String sums = output + "/sum";

            System.out.println(nonDir + " " + link + " " + counts);
            File nonDirFile = new File(nonDir), linkFile = new File(link), countsFile = new File(counts);

            assert !nonDirFile.exists() && !linkFile.exists() && !countsFile.exists();
            nonDirFile.mkdirs();
            linkFile.mkdirs();
            countsFile.mkdirs();

            String[] args1 = {input, nonDir};
            GraphBuilder.main(args1);

            String[] args2 = {nonDir, link};
            LinkMaker.main(args2);

            String[] args3 = {link, counts};
            LinkAnalyzer.main(args3);

            String[] args4 = {counts, sums};
            FinalCounter.main(args4);
        } catch (Exception e) {
            e.printStackTrace();    
            System.err.println("something error happened!");        
        }
    }
}

