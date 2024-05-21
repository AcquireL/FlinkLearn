package com.kali.flink.connector.socket

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object Socket {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("acquirel", 9999)

    val counts = text.flatMap {
        _.toLowerCase.split("\\W+") filter {
          _.nonEmpty
        }
      }
      .map {
        (_, 1)
      }
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")

  }

}
