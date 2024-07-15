package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: JoinDriver <songs> <lyrics> <genres> <users> <output>");
            System.exit(2);
        }

        String outputPath = args[4];
        String songsPath = new Path(args[0]).getParent().toString();
        String lyricsPath = new Path(args[1]).getParent().toString();
        String genresPath = new Path(args[2]).getParent().toString();
        String usersPath = new Path(args[3]).getParent().toString();

        Configuration conf1 = new Configuration();
        conf1.set("songs.path", songsPath);
        conf1.set("users.path", usersPath);
        conf1.set("songs.output.path", "songs/part");
        conf1.set("users.output.path", "users/part");

        Job job1 = Job.getInstance(conf1, "Join with Song ID");
        job1.setJarByClass(JoinDriver.class);
        job1.setMapperClass(JoinWithSongIdMapper.class);
        job1.setReducerClass(JoinWithSongIdReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileInputFormat.addInputPath(job1, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "/tmp1"));

        MultipleOutputs.addNamedOutput(job1, "songs", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "users", TextOutputFormat.class, Text.class, Text.class);

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("songs.path", outputPath + "/tmp1/songs");
        conf2.set("lyrics.path", lyricsPath);
        conf2.set("genres.path", genresPath);
        conf2.set("songs.output.path", "songs/part");
        conf2.set("lyrics.output.path", "lyrics/part");
        conf2.set("genres.output.path", "genres/part");

        Job job2 = Job.getInstance(conf2, "Join with Track ID");
        job2.setJarByClass(JoinDriver.class);
        job2.setMapperClass(JoinWithTrackIdMapper.class);
        job2.setReducerClass(JoinWithTrackIdReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(outputPath + "/tmp1/songs/part-r-00000"));
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/tmp2"));

        MultipleOutputs.addNamedOutput(job2, "songs", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job2, "lyrics", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job2, "genres", TextOutputFormat.class, Text.class, Text.class);

        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        conf3.set("songs.path", outputPath + "/tmp2/songs");
        conf3.set("users.path", outputPath + "/tmp1/users");
        conf3.set("songs.output.path", "songs/part");
        conf3.set("users.output.path", "users/part");

        Job job3 = Job.getInstance(conf3, "Final Join with Song ID");
        job3.setJarByClass(JoinDriver.class);
        job3.setMapperClass(JoinWithSongIdMapper.class);
        job3.setReducerClass(JoinWithSongIdReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(outputPath + "/tmp2/songs/part-r-00000"));
        FileInputFormat.addInputPath(job3, new Path(outputPath + "/tmp1/users/part-r-00000"));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath + "/final"));

        MultipleOutputs.addNamedOutput(job3, "songs", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job3, "users", TextOutputFormat.class, Text.class, Text.class);

        boolean success = job3.waitForCompletion(true);

        if (success) {
            FileSystem fs = FileSystem.get(new Configuration());

            // Copy lyrics and genres from tmp2 to final
            FileUtil.copy(fs, new Path(outputPath + "/tmp2/lyrics"), fs, new Path(outputPath), false,
                    new Configuration());
            FileUtil.copy(fs, new Path(outputPath + "/tmp2/genres"), fs, new Path(outputPath), false,
                    new Configuration());
            FileUtil.copy(fs, new Path(outputPath + "/final/users"), fs, new Path(outputPath), false,
                    new Configuration());
            FileUtil.copy(fs, new Path(outputPath + "/final/songs"), fs, new Path(outputPath), false,
                    new Configuration());
            // Delete tmp1 and tmp2 directories
            fs.delete(new Path(outputPath + "/tmp1"), true);
            fs.delete(new Path(outputPath + "/tmp2"), true);
            fs.delete(new Path(outputPath + "/final"), true);
        }

        System.exit(success ? 0 : 1);
    }
}
