package com.bigmu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.URI;

public class HDF5Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取文件路径
        URI uri = ((FileSplit) context.getInputSplit()).getPath().toUri();
        String hdf5FilePath = uri.getPath();

        System.out.println("\n\nProcessing file: " + hdf5FilePath + "\n\n");        

        H5ToString h5ToString = new H5ToString(); 
        String result = h5ToString.getResult(hdf5FilePath);

        // 获取song_id作为key
        String songId = result.split(",")[0];

        // 写入上下文
        context.write(new Text(songId), new Text(result));

    }
}
