package cn.catman;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;




public class GraphBuilder
{
    
    public static void main( String[] args )
    {
        try { 
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);
            job.setJobName("building graph");
            
            
            job.setJarByClass(TriangleCounter.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(GraphBuilderMapper.class);
            job.setReducerClass(GraphBuilderReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            boolean succ = job.waitForCompletion(true);
            if (!succ) {
                System.err.println("fail in GraphBuilder");
                System.exit(1);
            }
            // System.exit( ? 0 : 1);
        } catch (Exception e) { e.printStackTrace(); }
    }
}

