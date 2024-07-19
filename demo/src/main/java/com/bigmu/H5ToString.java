package com.bigmu;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * This file is used to convert the song data in the HDF5 file to a string for storage.
 * 
 * Design approach:
 *      1. Design a Map to store the attributes of the song, where the key is the attribute name and the value is the attribute value.
 *      2. Recursively traverse the tree structure of the HDF5 file to find the songs dataset.
 *      3. Extract the attributes of the song from the songs dataset.
 *      4. Convert the attributes of the song to a string.
 * 
 */
public class H5ToString {

    private Map<String, String> allowedItems = new HashMap<>(); // Create a Map to store the attributes of the song
    
    public H5ToString() {
        allowedItems.put("song_id", "");
        allowedItems.put("track_id", "");
        allowedItems.put("title", "");
        allowedItems.put("release", "");
        allowedItems.put("artist_id", "");
        allowedItems.put("artist_name", "");
        allowedItems.put("mode", "");
        allowedItems.put("energy", "");
        allowedItems.put("tempo", "");
        allowedItems.put("loudness", "");
        allowedItems.put("duration", "");
        allowedItems.put("danceability", "");
        allowedItems.put("year", "");
    }

    public String getResult(String filename) throws IOException {
        
        // Configure Hadoop
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(configuration);
        InputStream in = fs.open(new Path(filename));

        String result = "";
        try (HdfFile hdfFile = HdfFile.fromInputStream(in)) {
            // Recursively print all attributes of the root node and its children
            recursivePrintGroup(hdfFile);
            result = getSongAttribute();
        
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    // Note the tree structure of the H5 file, recursively traverse all nodes
    private void recursivePrintGroup(Group group) {
        
        for (Node node : group) {
  
            if (node instanceof Group) {
                recursivePrintGroup((Group) node);
            }
            
            if(node instanceof Dataset) {
                if(node.getName().equals("songs")) {
                    Dataset dataset = (Dataset) node;
                    Object data = dataset.getData();
                    
                    // Song data is stored in a compound dataset type
                    if (dataset.isCompound()) { 
                        
                        Map<String, Object> compoundData = (Map<String, Object>) data;  // Perform type casting according to the interface information provided by getData()
                        for(Map.Entry<String, Object> entry : compoundData.entrySet()) {
                            String key = entry.getKey();
                            if(!allowedItems.containsKey(key)) { // Do not process attributes not in allowedItems
                                continue;
                            }

                            Object value = entry.getValue();
                            String valueString = valueToString(value); // Need to convert manually
                            allowedItems.put(key, valueString); 
                        }
                    }                   


                }
            } 
        }
    }

    private static String valueToString(Object value) {
        String result;
        if (value instanceof int[]) {
            result = Arrays.toString((int[]) value);
        } else if (value instanceof double[]) {
            result = Arrays.toString((double[]) value);
        } else if (value instanceof Object[]) {
            result = Arrays.deepToString((Object[]) value);
        } 
        else {
            return value.toString();
        }

        // Remove the leading and trailing [] from the string
        return result.substring(1, result.length() - 1);
    }

    private String getSongAttribute() {
        // ATTENTION: Concatenate the strings in the order of allowedItems. Map is unordered!
        String result = "";
        result += allowedItems.get("song_id") + ",";
        result += allowedItems.get("track_id") + ",";
        result += allowedItems.get("title") + ",";
        result += allowedItems.get("release") + ",";
        result += allowedItems.get("artist_id") + ",";
        result += allowedItems.get("artist_name") + ",";
        result += allowedItems.get("mode") + ",";
        result += allowedItems.get("energy") + ",";
        result += allowedItems.get("tempo") + ",";
        result += allowedItems.get("loudness") + ",";
        result += allowedItems.get("duration") + ",";
        result += allowedItems.get("danceability") + ",";
        result += allowedItems.get("year");
        return result;
    }
}