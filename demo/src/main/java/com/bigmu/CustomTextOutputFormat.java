package com.bigmu;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.io.DataOutputStream;
import java.io.IOException;
// Custom OutputFormat class
public class CustomTextOutputFormat extends FileOutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // Get the configuration
        Configuration conf = job.getConfiguration();
        // Create a file system object
        FileSystem fs = FileSystem.get(conf);
        // Create the output file path
        Path file = getDefaultWorkFile(job, ".txt");
        // Create the output stream
        DataOutputStream out = fs.create(file, false);
        
        return new LineRecordWriter(out);
    }

    // Custom RecordWriter class
    protected static class LineRecordWriter extends RecordWriter<Text, Text> {
        private DataOutputStream out;

        public LineRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            // Write the value directly, followed by a newline character
            out.write(value.toString().getBytes("UTF-8"));
            out.write("\n".getBytes("UTF-8"));
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            out.close();
        }
    }
}
