package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinWithSongIdReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;
    private String songsOutputPath;
    private String usersOutputPath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        multipleOutputs = new MultipleOutputs<>(context);
        songsOutputPath = conf.get("songs.output.path");
        usersOutputPath = conf.get("users.output.path");
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
                multipleOutputs.write("songs", null, new Text(song), songsOutputPath);
            }
            for (String user : users) {
                multipleOutputs.write("users", null, new Text(user), usersOutputPath);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
