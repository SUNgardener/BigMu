package com.bigmu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinWithTrackIdReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;
    private String songsOutputPath;
    private String lyricsOutputPath;
    private String genresOutputPath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
        songsOutputPath = context.getConfiguration().get("songs.output.path");
        lyricsOutputPath = context.getConfiguration().get("lyrics.output.path");
        genresOutputPath = context.getConfiguration().get("genres.output.path");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> songs = new ArrayList<>();
        List<String> lyrics = new ArrayList<>();
        List<String> genres = new ArrayList<>();

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
            }
        }

        if (!songs.isEmpty() && !lyrics.isEmpty() && !genres.isEmpty()) {
            for (String song : songs) {
                multipleOutputs.write("songs", null, new Text(song), songsOutputPath);
            }
            for (String lyric : lyrics) {
                multipleOutputs.write("lyrics", null, new Text(lyric), lyricsOutputPath);
            }
            for (String genre : genres) {
                multipleOutputs.write("genres", null, new Text(genre), genresOutputPath);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
