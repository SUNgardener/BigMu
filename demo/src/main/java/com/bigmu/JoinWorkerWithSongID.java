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
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.s;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinWorkerWithSongID {

    public static class DataJoinMapper extends Mapper<Object, Text, Text, Text> {

        private String source;
        private String songsPath;
        private String usersPath;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            songsPath = conf.get("songsPath");
            usersPath = conf.get("usersPath");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path filePath = fileSplit.getPath();
            source = filePath.toString();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String songId;
            String recordType;

            if (source.contains(songsPath)) {
                songId = fields[0];
                recordType = "S";
            } else if (source.contains(usersPath)) {
                songId = fields[1];
                recordType = "U";
            } else {
                return;
            }

            context.write(new Text(songId), new Text(recordType + value.toString()));
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
            List<String> users = new ArrayList<>();

            for (Text value : values) {
                String record = value.toString();
                char recordType = record.charAt(0);
                switch (recordType) {
                    case 'S':
                        songs.add(record.substring(1));
                        break;
                    case 'U':
                        users.add(record.substring(1));
                        break;
                }
            }

            if (!songs.isEmpty() && !users.isEmpty()) {
                for (String song : songs) {
                    multipleOutputs.write("songs", null, new Text(song), "songs/part");
                }
                for (String user : users) {
                    multipleOutputs.write("users", null, new Text(user), "users/part");
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: JoinWorker <songs> <users> <output>");
            System.exit(2);
        }

        String songsPath = new Path(args[0]).getParent().toString();
        String usersPath = new Path(args[1]).getParent().toString();
        Configuration conf = new Configuration();
        conf.set("songsPath", songsPath);
        conf.set("usersPath", usersPath);
        Job job = Job.getInstance(conf, "Join Worker");
        job.setJarByClass(JoinWorkerWithSongID.class);
        job.setMapperClass(DataJoinMapper.class);
        job.setReducerClass(DataJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        MultipleOutputs.addNamedOutput(job, "songs", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "users", TextOutputFormat.class, Text.class, Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
