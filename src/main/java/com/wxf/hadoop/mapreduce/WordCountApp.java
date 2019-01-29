package com.wxf.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;

public class WordCountApp {

    public Integer run(String...args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. get configuration
        Configuration configuration = new Configuration();
        //create job
        Job job =  Job.getInstance(configuration,this.getClass().getSimpleName());
        //run job
        job.setJarByClass(this.getClass());

        //3. set job
        //int -> map -> reduce -> output
        //3.1 input
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);

        //3.2 map
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3.3 reduce
        job.setReducerClass(WrappedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //3.4 output
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        //4. submit job
        boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 0:1;
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new WordCountApp().run(args);
    }
}
