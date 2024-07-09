package com.bigmu;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Paths;

/**
 * Example application for reading a dataset from HDF5
 */
public class JHDF5TestDir {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java JHDF5TestDir <path-to-hdf5-file>");
            System.exit(1);
        }

        String filePath = args[0];
        System.out.println("Processing file: " + filePath);

        // 打开 HDF5 文件
        try (HdfFile hdfFile = new HdfFile(Paths.get(filePath))) {
            System.out.println(hdfFile.getFile().getName()); //NOSONAR - sout in example
            recursivePrintGroup(hdfFile, 0);

            // 打印每个组的songs这个dataset的所有字段
            printDatasetFields(hdfFile, "/metadata/songs");
            printDatasetFields(hdfFile, "/analysis/songs");
            printDatasetFields(hdfFile, "/musicbrainz/songs");
        } catch (Exception e) {
            System.err.println("Error processing file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void recursivePrintGroup(Group group, int level) {
        level++;
        String indent = StringUtils.repeat("    ", level);
        for (Node node : group) {
            System.out.println(indent + node.getName()); //NOSONAR - sout in example
            if (node instanceof Group) {
                recursivePrintGroup((Group) node, level);
            }
        }
    }

    private static void printDatasetFields(HdfFile hdfFile, String datasetPath) {
        Dataset dataset = hdfFile.getDatasetByPath(datasetPath);
        if (dataset != null) {
            Object data = dataset.getData();
            if (data instanceof Object[]) {
                for (Object row : (Object[]) data) {
                    if (row instanceof Object[]) {
                        for (Object field : (Object[]) row) {
                            System.out.print(field + " ");
                        }
                    } else {
                        System.out.print(row + " ");
                    }
                    System.out.println();
                }
            } else if (data instanceof java.util.Map) {
                java.util.Map<?, ?> map = (java.util.Map<?, ?>) data;
                for (java.util.Map.Entry<?, ?> entry : map.entrySet()) {
                    System.out.println(entry.getKey() + ": " + entry.getValue());
                }
            } else {
                System.out.println(data);
            }
        } else {
            System.err.println("Dataset not found: " + datasetPath);
        }
    }
}



