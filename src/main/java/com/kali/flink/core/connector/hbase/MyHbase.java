package com.kali.flink.core.connector.hbase;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.client.Table;

import org.apache.zookeeper.server.ConnectionBean;

import java.io.IOException;

public class MyHbase {

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum","acquirel");

        config.set("hbase.zookeeper.property.clientPort","2181");

        Connection connection = ConnectionFactory.createConnection(config);

        Table table = connection.getTable(TableName.valueOf("t1"));

        Put put = new Put("3".getBytes());

        put.addColumn("f1".getBytes(),"c1".getBytes(),"hehe".getBytes());


        table.put(put);

        table.close();
        connection.close();

    }

}