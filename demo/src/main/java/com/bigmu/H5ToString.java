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

public class H5ToString {

    private Map<String, String> allowedItems = new HashMap<>(); // 直接打表
    
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
        
        // 配置Hadoop
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(configuration);
        InputStream in = fs.open(new Path(filename));

        String result = "";
        try (HdfFile hdfFile = HdfFile.fromInputStream(in)) {
            // 递归输出根节点及其子节点的所有属性
            recursivePrintGroup(hdfFile);
            result = getSongAttribute();
        
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

	// 注意H5文件的树状结构，递归遍历所有节点
    private void recursivePrintGroup(Group group) {
		
        for (Node node : group) {
  
            if (node instanceof Group) {
				recursivePrintGroup((Group) node);
			}
            
            if(node instanceof Dataset) {
                if(node.getName().equals("songs")) {
                    Dataset dataset = (Dataset) node;
                    Object data = dataset.getData();
                    
                    // song所需数据放在复合数据集类型
                    if (dataset.isCompound()) { 
                        
                        Map<String, Object> compoundData = (Map<String, Object>) data;  // 按照给出的 getData() 有关的接口信息进行强制类型转换
                        for(Map.Entry<String, Object> entry : compoundData.entrySet()) {
                            String key = entry.getKey();
                            if(!allowedItems.containsKey(key)) { // 不在allowedItems中的属性不处理
                                continue;
                            }

                            Object value = entry.getValue();
                            String valuString = valueToString(value); // 需要自行转换
                            allowedItems.put(key, valuString); 
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

        // 移除字符串开头和结尾的[]
        return result.substring(1, result.length() - 1);
    }

    private String getSongAttribute() {
        // ATTENTION: 按照allowedItems中的顺序拼接字符串. Map是无序的!
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