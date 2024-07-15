package com.bigmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class DataPreprocessor {

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Usage: DataPreprocessor <input-features-file> <input-labels-file> <output-features-path> <output-labels-path>");
            System.exit(-1);
        }

        String inputFeaturesFile = args[0];
        String inputLabelsFile = args[1];
        Path outputFeaturesPath = new Path(args[2]);
        Path outputLabelsPath = new Path(args[3]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Write features
        try (FSDataInputStream fis = fs.open(new Path(inputFeaturesFile));
             BufferedReader br = new BufferedReader(new InputStreamReader(fis));
             SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(outputFeaturesPath),
                     SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(VectorWritable.class))) {

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",", 2);
                String trackId = parts[0];
                String[] wordCounts = parts[1].replace("[(", "").replace(")]", "").split("\\),\\(");

                Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE);//构建一个长度为Integer.MAX_VALUE的稀疏向量
                for (String wc : wordCounts) {
                    String[] wcParts = wc.split(":");
                    int word = Integer.parseInt(wcParts[0].trim());
                    int count = Integer.parseInt(wcParts[1].trim());
                    vector.setQuick(word, count);
                }

                writer.append(new Text(trackId), new VectorWritable(vector));
            }
        }

        // Write labels
        try (FSDataInputStream fis = fs.open(new Path(inputLabelsFile));
             BufferedReader br = new BufferedReader(new InputStreamReader(fis));
             SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(outputLabelsPath),
                     SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class))) {

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String trackId = parts[0];
                String label = parts[1];
                writer.append(new Text(trackId), new Text(label));
            }
        }
    }
}
