package com.bigmu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
/*
 * This is the Reducer class for reading HDF5 files.
 * 
 * Design approach: Output the first value of the input to remove redundant data.
 * 
 * Input format: key is song_id, value is song attributes.
 * Output format: key is null, value is song attributes.
 * Data types: Input Text, Text; Output Text, Text.
 * 
 */
public class HDF5Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        if (iterator.hasNext()) {
            Text firstValue = iterator.next();
            context.write(null, firstValue);
        }
    }
}

