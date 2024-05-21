package com.kali.flink.connector.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object hdfsOperatior {
  def main(args: Array[String]): Unit = {
    val hdfsPath = new Path("hdfs://acquirel:9000/data/derby.log")
    val fs = FileSystem.get(new Configuration())
    val inputStream = fs.open(hdfsPath)

    // Read the file content
    val content = scala.io.Source.fromInputStream(inputStream).getLines().mkString("\n")
    inputStream.close()

    // Print the content
    println(content)

    // Don't forget to close the FileSystem
    fs.close()
  }
}
