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

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


/**
 *  Map阶段
 *  输入: 文本文件中的每一行。
 *  处理:
 *   在setup方法中处理单词表行:
 *      读取输入分片。
 *      查找以%开头的行，解析并存储单词到列表或数组中。
 *   在map方法中处理普通行:
 *      忽略以#或%开头的行。
 *      对于普通行，使用存储的单词表将单词下标转换为实际单词，并输出相应的键值对。
 *
 *  Reduce阶段
 *  输入: Map阶段输出的键值对。
 *  处理:
 *     对于每个track_id，聚合所有的单词计数对。
 *     将所有单词计数对格式化为[(word1:count1),(word2:count2),...]的形式。
 *     输出: 每个track_id及其对应的单词计数列表。
 */

public class LyricsCountWorker {
   

    
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final List<String> wordList = new ArrayList<>();
        
        private Text word = new Text();
        private Text trackId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(line.startsWith("%")) {
                String[] words = line.substring(1).split(",");
                wordList.add("DUMMY"); // 占位符
                for (String w : words) {
                    wordList.add(w);
                }
            }

            if (!line.startsWith("#") && !line.startsWith("%")) {
                StringTokenizer itr = new StringTokenizer(line, ",");
                if (itr.hasMoreTokens()) {
                    trackId.set(itr.nextToken()); // 获取track_id
                    itr.nextToken(); // 跳过mxm_track_id
                    while (itr.hasMoreTokens()) {
                        String[] wordCount = itr.nextToken().split(":");
                        int wordIndex = Integer.parseInt(wordCount[0]);
                        if (wordIndex < wordList.size()) {
                            word.set(wordIndex + ":" + wordCount[1]);
                        }
                        context.write(trackId, word);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder wordCountsBuilder = new StringBuilder(); 
            wordCountsBuilder.append(key.toString()).append(",[");
            
            boolean first = true;
            for (Text val : values) {
                if (!first) {
                    wordCountsBuilder.append(",");
                } else {
                    first = false;
                }
                wordCountsBuilder.append("(").append(val.toString()).append(")");
            }
            wordCountsBuilder.append("]");
            
            // 只输出 wordCounts 不输出 key
            context.write(new Text(""), new Text(wordCountsBuilder.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "lyrics wordscount preprocess");
        job.setJarByClass(LyricsCountWorker.class);
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