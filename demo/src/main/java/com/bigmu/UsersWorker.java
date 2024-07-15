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
 *  Map阶段
 *  输入: 文本文件中的每一行。
 *  处理:
 *   在map方法中处理普通行: 输入格式 user_id\tsong_id\tplay_count\n
 *      对于普通行，输出键值对 (user_id, <song_id, play_count>)。 
 *
 *  Reduce阶段
 *  输入: Map阶段输出的键值对。
 *  处理:
 *     直接输出键值对。
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
        // 设置自定义的 OutputFormat 类
        job.setOutputFormatClass(CustomTextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
