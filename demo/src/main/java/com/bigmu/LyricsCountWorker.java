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
 *  Map phase
 *  Input: Each line in the text file.
 *  Processing:
 *   In the setup method, process the word list line:
 *      Read input splits.
 *      Look for lines starting with %, parse and store words in a list or array.
 *   In the map method, process regular lines:
 *      Ignore lines starting with # or %.
 *      For regular lines, use the stored word list to convert word indexes to actual words and output the corresponding key-value pairs.
 *
 *  Reduce phase
 *  Input: Key-value pairs output from the map phase.
 *  Processing:
 *     For each track_id, aggregate all word count pairs.
 *     Format all word count pairs as [(word1:count1),(word2:count2),...] and output.
 *     Output: track_id and its corresponding word count list.
 * 
 * Data type: Text
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
                wordList.add("DUMMY"); // Placeholder
                for (String w : words) {
                    wordList.add(w);
                }
            }

            if (!line.startsWith("#") && !line.startsWith("%")) {
                StringTokenizer itr = new StringTokenizer(line, ",");
                if (itr.hasMoreTokens()) {
                    trackId.set(itr.nextToken()); // Get track_id
                    itr.nextToken(); // Skip mxm_track_id
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
            
            // Only output wordCounts, do not output key
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
        // Set custom OutputFormat class
        job.setOutputFormatClass(CustomTextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}