package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/*
 * This is the Mapper class that joins multiple datasets based on track_id.
 * 
 * Design approach:
 *      1. Set variables songsPath, lyricsPath, and genresPath to represent the paths of songs, lyrics, and genres datasets.
 *         Get the input file path through the setup() method.
 *      2. In the map() method, determine which dataset is currently being processed based on the input file path,
 *         and mark the type of the current record.
 *         Then, output the track_id as the key and the record type and record content as the value.
 * 
 * Input format: Text file, with each line representing a record and fields separated by commas.
 * Output format: key as track_id, value as record type and record content.
 * Data type: Text
 * 
 * songs dataset format: song_id, track_id, song_name, artist_id, artist_name, album_id, album_name, duration, year
 * lyrics dataset format: track_id, lyric
 * genres dataset format: track_id, genre
 */
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
        } else if (source.contains(genresPath)) {
            trackId = fields[0];
            recordType = "G";
        } else {
            return;
        }

        context.write(new Text(trackId), new Text(recordType + value.toString()));
    }
}
