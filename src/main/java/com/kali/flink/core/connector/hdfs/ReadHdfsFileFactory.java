package com.kali.flink.core.connector.hdfs;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


/**
 *  flink读取hdfs上的文件
 */
public class ReadHdfsFileFactory {

    // 读取hdfs上的txt文件
    private DataSet<String> readHdfsTextFile(ExecutionEnvironment env,String path){
        return env.readTextFile(path);
    }



}
