package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinWithTrackIdMapper extends Mapper<Object, Text, Text, Text> {

    private String source;
    private String songsPath;
    private String lyricsPath;
    private String genresPath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        songsPath = conf.get("songs.path");
        lyricsPath = conf.get("lyrics.path");
        genresPath = conf.get("genres.path");
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path filePath = fileSplit.getPath();
        source = filePath.toString();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String trackId;
        String recordType;

        if (source.contains(songsPath)) {
            trackId = fields[1];
            recordType = "S";
        } else if (source.contains(lyricsPath)) {
            trackId = fields[0];
            recordType = "L";
        }else if (source.contains(genresPath)) {
            trackId = fields[0];
            recordType = "G";
        }else {
            return;
        }

        context.write(new Text(trackId), new Text(recordType + value.toString()));
    }
}

