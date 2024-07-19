package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

import java.util.StringTokenizer;


/**
 *  Map phase
 *  Input: Each line in the text file.
 *  Processing:
 *   Process regular lines in the map method: Input format user_id\tsong_id\tplay_count\n
 *      For regular lines, output key-value pairs (user_id, <song_id, play_count>).
 *
 *  Reduce phase
 *  Input: Key-value pairs from the map phase.
 *  Processing:
 *     Directly output key-value pairs.
 */

public class UsersWorker {
    
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line, "\t");
            if (itr.countTokens() == 3) {
                Text userId = new Text(itr.nextToken());
                Text songId = new Text(itr.nextToken());
                Text playCount = new Text(itr.nextToken());
                context.write(userId, new Text(songId + "," + playCount));                
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                Text result = new Text(key.toString() + "," + val.toString());
                context.write(new Text(""), result);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "user preprocess");
        job.setJarByClass(UsersWorker.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Set custom OutputFormat class
        job.setOutputFormatClass(CustomTextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
