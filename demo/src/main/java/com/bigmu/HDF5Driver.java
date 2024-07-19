package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * This is the Driver class for reading HDF5 files.
 * 
 * Design approach: Set Mapper and Reducer classes, set input and output paths.
 * 
 * Input format: HDF5 file
 * Output format: key as song_id, value as song attributes
 * Data types: input Text, Text; output Text, Text
 * 
 */
public class HDF5Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HDF5Driver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HDF5 Extractor");

        job.setJarByClass(HDF5Driver.class);
        job.setMapperClass(HDF5Mapper.class);
        job.setReducerClass(HDF5Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
