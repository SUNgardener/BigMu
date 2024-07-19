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

/**
 * This is a driver program for joining multiple datasets.
 * 
 * Design:
 *      1. The first MapReduce task joins the songs dataset and the users dataset together using song_id as the key.
 *         The Mapper class is JoinWithSongIdMapper, and the Reducer class is JoinWithSongIdReducer.
 * 
 *      2. The second MapReduce task joins the songs dataset from the first task with the lyrics dataset and the genres dataset using track_id as the key.
 *         The Mapper class is JoinWithTrackIdMapper, and the Reducer class is JoinWithTrackIdReducer.
 * 
 *      3. The third MapReduce task joins the songs dataset from the second task with the users dataset using song_id as the key.
 *         The Mapper class is JoinWithSongIdMapper, and the Reducer class is JoinWithSongIdReducer.
 * 
 *      4. The final result will include the songs, lyrics, genres, and users datasets.
 *         The HDFS API is used to copy these datasets to the final output path and delete the temporary directories.
 * 
 * In summary, since only the songs dataset contains both song_id and track_id, we need to filter out the songs dataset first and then join it with other datasets.
 */
public class JoinDriver {

    public static void main(String[] args) throws Exception {
        // Check if the number of input parameters is correct
        if (args.length != 5) {
            System.err.println("Usage: JoinDriver <songs> <lyrics> <genres> <users> <output>");
            System.exit(2);
        }

        // Set the output path and paths for each input dataset
        String outputPath = args[4];
        String songsPath = new Path(args[0]).getParent().toString();
        String lyricsPath = new Path(args[1]).getParent().toString();
        String genresPath = new Path(args[2]).getParent().toString();
        String usersPath = new Path(args[3]).getParent().toString();

        // Configure parameters for the first MapReduce task
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

        // Set the input and output paths for the first task
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileInputFormat.addInputPath(job1, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "/tmp1"));

        // Configure MultipleOutputs to support multiple output paths
        MultipleOutputs.addNamedOutput(job1, "songs", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "users", TextOutputFormat.class, Text.class, Text.class);

        // Wait for the first task to complete
        job1.waitForCompletion(true);

        // Configure parameters for the second MapReduce task
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

        // Set the input and output paths for the second task
        FileInputFormat.addInputPath(job2, new Path(outputPath + "/tmp1/songs/part-r-00000"));
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/tmp2"));

        // Configure MultipleOutputs to support multiple output paths
        MultipleOutputs.addNamedOutput(job2, "songs", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job2, "lyrics", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job2, "genres", TextOutputFormat.class, Text.class, Text.class);

        // Wait for the second task to complete
        job2.waitForCompletion(true);

        // Configure parameters for the third MapReduce task
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

        // Set the input and output paths for the third task
        FileInputFormat.addInputPath(job3, new Path(outputPath + "/tmp2/songs/part-r-00000"));
        FileInputFormat.addInputPath(job3, new Path(outputPath + "/tmp1/users/part-r-00000"));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath + "/final"));

        // Configure MultipleOutputs to support multiple output paths
        MultipleOutputs.addNamedOutput(job3, "songs", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job3, "users", TextOutputFormat.class, Text.class, Text.class);

        // Wait for the third task to complete and check if it is successful
        boolean success = job3.waitForCompletion(true);

        if (success) {
            FileSystem fs = FileSystem.get(new Configuration());

            // Copy the results to the final output path
            FileUtil.copy(fs, new Path(outputPath + "/tmp2/lyrics"), fs, new Path(outputPath), false,
                    new Configuration());
            FileUtil.copy(fs, new Path(outputPath + "/tmp2/genres"), fs, new Path(outputPath), false,
                    new Configuration());
            FileUtil.copy(fs, new Path(outputPath + "/final/users"), fs, new Path(outputPath), false,
                    new Configuration());
            FileUtil.copy(fs, new Path(outputPath + "/final/songs"), fs, new Path(outputPath), false,
                    new Configuration());

            // Delete the temporary directories
            fs.delete(new Path(outputPath + "/tmp1"), true);
            fs.delete(new Path(outputPath + "/tmp2"), true);
            fs.delete(new Path(outputPath + "/final"), true);
        }

        // Exit the program with a success or failure status
        System.exit(success ? 0 : 1);
    }
}
