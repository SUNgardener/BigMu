package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinWorker {

    public static class DataJoinMapper extends Mapper<Object, Text, Text, Text> {

        private String songPath;
        private String lyricsPath;
        private String genresPath;
        private String usersPath;
        private String source;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            songPath = conf.get("songs.path");
            lyricsPath = conf.get("lyrics.path");
            genresPath = conf.get("genres.path");
            usersPath = conf.get("users.path");

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path filePath = fileSplit.getPath();
            source = filePath.toString();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String trackId;
            String songId = null;
            String recordType;
            Configuration conf = context.getConfiguration();

            if (source.contains(songPath)) {
                trackId = fields[1];
                songId = fields[0];
                recordType = "S";
                conf.set("track." + trackId, songId); // 使用参数传递储存trackId和songId的映射
                context.write(new Text(trackId + '\t' + songId), new Text(recordType + value.toString()));
            } else if (source.contains(lyricsPath)) {
                trackId = fields[0];
                recordType = "L";
                context.write(new Text(trackId), new Text(recordType + value.toString()));
            } else if (source.contains(genresPath)) {
                trackId = fields[0];
                recordType = "G";
                context.write(new Text(trackId), new Text(recordType + value.toString()));
            } else if (source.contains(usersPath)) {
                songId = fields[1];
                recordType = "U";
                context.write(new Text(songId), new Text(recordType + value.toString()));
            } else {
                return;
            }

        }
    }


    public static class DataJoinReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> songs = new ArrayList<>();
            List<String> lyrics = new ArrayList<>();
            List<String> genres = new ArrayList<>();
            List<String> users = new ArrayList<>();

            for (Text value : values) {
                String record = value.toString();
                char recordType = record.charAt(0);
                switch (recordType) {
                    case 'S':
                        songs.add(record.substring(1));
                        break;
                    case 'L':
                        lyrics.add(record.substring(1));
                        break;
                    case 'G':
                        genres.add(record.substring(1));
                        break;
                    case 'U':
                        users.add(record.substring(1));
                        break;
                }
            }

            if (!songs.isEmpty() && !users.isEmpty() && !lyrics.isEmpty() && !genres.isEmpty()) {
                for (String song : songs) {
                    multipleOutputs.write("songs", null, new Text(song), "songs/part");
                }
                for (String user : users) {
                    multipleOutputs.write("users", null, new Text(user), "users/part");
                }
                for (String lyric : lyrics) {
                    multipleOutputs.write("lyrics", null, new Text(lyric), "lyrics/part");
                }
                for (String genre : genres) {
                    multipleOutputs.write("genres", null, new Text(genre), "genres/part");
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: JoinWorker <songs> <lyrics> <genres> <users> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("songs.path", args[0]);
        conf.set("lyrics.path", args[1]);
        conf.set("genres.path", args[2]);
        conf.set("users.path", args[3]);

        Job job = Job.getInstance(conf, "Join Worker");
        job.setJarByClass(JoinWorker.class);
        job.setMapperClass(DataJoinMapper.class);
        job.setReducerClass(DataJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        // Setup multiple outputs
        MultipleOutputs.addNamedOutput(job, "songs", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "lyrics", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "genres", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "users", TextOutputFormat.class, Text.class, Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

