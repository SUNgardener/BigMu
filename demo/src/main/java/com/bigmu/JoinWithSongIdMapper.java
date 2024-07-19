package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
/*
 * This is the Mapper class that joins multiple datasets based on song_id.
 * 
 * Design:
 *      1. Set variables songsPath and usersPath to represent the paths of the songs and users datasets.
 *         Get the input file path through the setup() method.
 *      2. In the map() method, determine which dataset is being processed based on the input file path,
 *         and mark the type of the current record.
 *         Then, output the song_id as the key and the record type and record content as the value.
 * 
 * Input format: Text file, each line represents a record with fields separated by commas.
 * Output format: key as song_id, value as record type and record content.
 * Data type: Text
 * 
 * songs dataset format: song_id,track_id,song_name,artist_id,artist_name,album_id,album_name,duration,year
 * users dataset format: user_id,song_id,play_count
 */
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
