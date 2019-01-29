package com.wxf.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class HdfsApp {
    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = getFileSystem();
//        String fileName = "/user/wxf/mapreduce/wordcount/input/wc.input";
//        read(fileName, fileSystem);
        String fileName = "/user/wxf/put-wc.input";



    }

    /**
     * write data
     * @param fileName
     * @param fileSystem
     * @throws Exception
     */
    public static void write(String fileName, FileSystem fileSystem)throws Exception{
        Path path = new Path(fileName);

        //output Stream
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        //file input Stream
        FileInputStream fis = new FileInputStream(new File("/user/loacl/software/hadopp/data"));
        try {
            IOUtils.copyBytes(fis,fsDataOutputStream,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fsDataOutputStream);
        }
    }


    /**
     * read data
     * @param fileName
     * @param fileSystem
     * @throws IOException
     */
    public static void read(String fileName, FileSystem fileSystem) throws IOException {
//        读取路径
        Path path = new Path(fileName);
        //打开文件
        FSDataInputStream open = fileSystem.open(path);
        try {
            IOUtils.copyBytes(open,System.out,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(open);
        }
    }
    /**
     * FileSystem
     * @return
     * @throws IOException
     */
    public static FileSystem getFileSystem()throws IOException {
        //core-site.xml,core-default.xml,hdfs-site.xml,hdfs-default.xml
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        System.out.println(fileSystem);
        return fileSystem;
    }
}
