package com.bigmu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/*
 * This is the Reducer class that joins multiple datasets based on track_id.
 * 
 * Design approach:
 *     1. Set variables songsOutputPath, lyricsOutputPath, and genresOutputPath to represent the output paths for songs, lyrics, and genres datasets.
 *        Get these paths using the setup() method.
 *     2. In the reduce() method, check if the current key has records for songs, lyrics, and genres datasets. If so, output these records to the corresponding datasets.
 * 
 * Input format: key is track_id, value is the record type and record content.
 * Output format: key is null, value is the record content.
 * Data type: Text
 * 
 * songs dataset format: song_id, track_id, song_name, artist_id, artist_name, album_id, album_name, duration, year
 * lyrics dataset format: track_id, lyric
 * genres dataset format: track_id, genre
 */
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
