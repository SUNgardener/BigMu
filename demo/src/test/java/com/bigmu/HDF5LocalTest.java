package com.bigmu;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import java.nio.file.Paths;

public class HDF5LocalTest {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java HDF5LocalTest <path-to-hdf5-file>");
            System.exit(1);
        }

        String filePath = args[0];
        System.out.println("Processing file: " + filePath);

        // 打开 HDF5 文件
        try (HdfFile hdfFile = new HdfFile(Paths.get(filePath))) {
            // 读取所需的属性
            String songId = readString(hdfFile, "/metadata/songs/song_id");
            String trackId = readString(hdfFile, "/analysis/songs/track_id");
            String title = readString(hdfFile, "/metadata/songs/title");
            String release = readString(hdfFile, "/metadata/songs/release");
            String artistId = readString(hdfFile, "/metadata/songs/artist_id");
            String artistName = readString(hdfFile, "/metadata/songs/artist_name");
            int mode = readInt(hdfFile, "/analysis/songs/mode");
            float energy = readFloat(hdfFile, "/analysis/songs/energy");
            float tempo = readFloat(hdfFile, "/analysis/songs/tempo");
            float loudness = readFloat(hdfFile, "/analysis/songs/loudness");
            float duration = readFloat(hdfFile, "/analysis/songs/duration");
            float danceability = readFloat(hdfFile, "/analysis/songs/danceability");
            int year = readInt(hdfFile, "/musicbrainz/songs/year");

            // 拼接结果字符串
            String result = String.format("%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%d",
                songId, trackId, title, release, artistId, artistName, mode, energy, tempo,
                loudness, duration, danceability, year);
            
            // 输出结果
            System.out.println("Result: " + result);
        } 
    }

    private static String readString(HdfFile hdfFile, String path) {
        Dataset dataset = hdfFile.getDatasetByPath(path);
        if (dataset != null) {
            return dataset.getData().toString();
        } else {
            System.err.println("Dataset not found: " + path);
            return "";
        }
    }

    private static int readInt(HdfFile hdfFile, String path) {
        Dataset dataset = hdfFile.getDatasetByPath(path);
        if (dataset != null) {
            int[] data = (int[]) dataset.getData();
            return data[0];
        } else {
            System.err.println("Dataset not found: " + path);
            return -1;
        }
    }

    private static float readFloat(HdfFile hdfFile, String path) {
        Dataset dataset = hdfFile.getDatasetByPath(path);
        if (dataset != null) {
            float[] data = (float[]) dataset.getData();
            return data[0];
        } else {
            System.err.println("Dataset not found: " + path);
            return -1.0f;
        }
    }
}
