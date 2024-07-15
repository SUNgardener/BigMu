package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinWithSongIdMapper extends Mapper<Object, Text, Text, Text> {

    private String source;
    private String songsPath;
    private String usersPath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        songsPath = conf.get("songs.path");
        usersPath = conf.get("users.path");
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
