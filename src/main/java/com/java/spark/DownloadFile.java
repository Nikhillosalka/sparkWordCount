package com.java.spark;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Nikhil on 25/06/18.
 */
public class DownloadFile {
    Configuration hadoopConfig = new Configuration();
    HdfsFunctionCaller hdfsFunctionCaller = new HdfsFunctionCaller() {

        public boolean copyMerge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, Configuration conf, String addString) throws IOException {
            return FileUtil.copyMerge(srcFS, srcDir, dstFS, dstFile, deleteSource, conf, addString);
        }


        public FileSystem getHdfsFileSystem(String uriPath, Configuration conf) throws IOException, URISyntaxException {
            return FileSystem.get(new URI(uriPath), conf);
        }


        public DistributedFileSystem getDistributedFileSystem(final Configuration conf, final String hdfsPath) throws URISyntaxException, IOException {

            return new DistributedFileSystem() {
                {
                    initialize(new URI(hdfsPath), conf);
                }
            };
        }
    };

    public static void main(String[] args) throws Exception {
        DownloadFile file = new DownloadFile();
//        String input = "/user/hdfs/wiki/testwikiResult";
//        String output = "/Users/Nikhil/Desktop/download";
//        String hdfs_root = "hdfs://localhost:9000";
        String hdfs_root = args[0];
        String input = args[1];
        String output = args[2];
        file.copy(input, output, hdfs_root);
    }

    void copy(String input, String output, String hdfs_root) throws Exception {
        //"/user/hdfs/wiki/testwikiResult"
        //"/Users/Nikhil/Desktop/download"
        //hdfs root "hdfs://localhost:9000"
        hdfsFunctionCaller.copyMerge(getSourceFS(hdfs_root), new Path(input), getDestinationFS(), new Path(output), false, getHDFSConf(), null);
    }

    Configuration getHDFSConf() {
        hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return hadoopConfig;
    }

    FileSystem getDestinationFS() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        return FileSystem.getLocal(conf);
    }

    DistributedFileSystem getSourceFS(String hdfsPath) throws URISyntaxException, IOException {
        System.setProperty("HADOOP_USER_NAME", "Nikhil");
        Configuration conf = getHDFSConf();
        DistributedFileSystem dFS = hdfsFunctionCaller.getDistributedFileSystem(conf, hdfsPath);
        return dFS;
    }
}

interface HdfsFunctionCaller {
    boolean copyMerge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, Configuration conf, String addString) throws IOException;

    FileSystem getHdfsFileSystem(String uriPath, Configuration conf) throws IOException, URISyntaxException;

    DistributedFileSystem getDistributedFileSystem(Configuration conf, String hdfsPathFinal) throws URISyntaxException, IOException;
}