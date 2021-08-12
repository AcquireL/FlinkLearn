package com.kali.flink.core.connector.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.types.Row;
import parquet.schema.MessageType;

/**
 *  flink读取hdfs上的文件
 */
public class ReadHdfsFileFactory {

    // 读取hdfs上的txt文件
    private DataSet<String> readHdfsTextFile(ExecutionEnvironment env,String path){
        return env.readTextFile(path);
    }



}
