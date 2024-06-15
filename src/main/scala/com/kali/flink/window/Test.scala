package com.kali.flink.window

import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.{TypeInfo, TypeInformation, Types}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}


case class User(name:String,age:Int,ts:Long)

class Test {

}

object Test{
  def main(args: Array[String]): Unit = {
//    val env =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
//
//    val source = new DataGeneratorSource(new MyGeneratorFunctios, 5, Types.STRING)
//
//
//    val stream = env.fromSource(source, WatermarkStrategy.noWatermarks, "Generator Source")
//
//    stream.print()
//
//    env.execute("Generator Test")
  }
}

class MyGeneratorFunctios extends GeneratorFunction[Long,String]{
  override def map(value: Long): String = "test"
}