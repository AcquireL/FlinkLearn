package com.kali.flink.connector.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Table}

object hbaseOperator {
  def main(args: Array[String]): Unit = {

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "acquirel")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val connection: Connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("t1"))

    // Perform operations on the HBase table here
    val put: Put = new Put("5".getBytes)
    put.addColumn("f1".getBytes, "c1".getBytes, "hehe".getBytes)


    table.put(put)
    table.close()
    connection.close()
  }

}
