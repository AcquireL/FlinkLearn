package com.kali.flink.connector.generator


import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.datagen.functions.FromElementsGeneratorFunction
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


class RandomGeneratorTest {

}
// scala 可能不支持
object RandomGeneratorTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val gengerationFunction =new MyGeneratorFunction()

//    val source =new DataGeneratorSource[String](gengerationFunction,2L,Types.STRING)

//    val dataStreamSource = env.fromSource(, WatermarkStrategy.noWatermarks, "data-generator")


    env.execute("generator")
  }
}

class MyGeneratorFunction extends GeneratorFunction[Long, String]() {
  override def open(readerContext: SourceReaderContext): Unit = super.open(readerContext)

  override def close(): Unit = super.close()

  override def map(value: Long) = "test"
}