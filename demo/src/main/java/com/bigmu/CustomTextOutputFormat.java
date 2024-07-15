package com.bigmu;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.io.DataOutputStream;
import java.io.IOException;

// 自定义 OutputFormat 类
public class CustomTextOutputFormat extends FileOutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // 获取配置
        Configuration conf = job.getConfiguration();
        // 创建文件系统对象
        FileSystem fs = FileSystem.get(conf);
        // 创建输出文件路径
        Path file = getDefaultWorkFile(job, ".txt");
        // 创建输出流
        DataOutputStream out = fs.create(file, false);
        
        return new LineRecordWriter(out);
    }

    // 自定义 RecordWriter 类
    protected static class LineRecordWriter extends RecordWriter<Text, Text> {
        private DataOutputStream out;

        public LineRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            // 直接写入 value，每个 value 后面跟一个换行符
            out.write(value.toString().getBytes("UTF-8"));
            out.write("\n".getBytes("UTF-8"));
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            out.close();
        }
    }
}