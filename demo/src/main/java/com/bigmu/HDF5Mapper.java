package com.bigmu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.URI;
/*
 * This is the Mapper class for reading HDF5 files.
 * 
 * Design idea: Call the getResult method of the H5ToString class to convert the song data in the HDF5 file to a string.
 * 
 * Input format: HDF5 file
 * Output format: key as song_id, value as song attributes
 * Data types: input LongWritable, Text; output Text, Text
 * 
 */
public class HDF5Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the file path
        URI uri = ((FileSplit) context.getInputSplit()).getPath().toUri();
        String hdf5FilePath = uri.getPath();

        System.out.println("\n\nProcessing file: " + hdf5FilePath + "\n\n");        

        H5ToString h5ToString = new H5ToString(); 
        String result = h5ToString.getResult(hdf5FilePath);

        // Get song_id as key
        String songId = result.split(",")[0];

        // Write to context
        context.write(new Text(songId), new Text(result));

    }
}
