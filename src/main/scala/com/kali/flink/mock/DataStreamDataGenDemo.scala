package com.kali.flink.util

import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.datagen.{DataGeneratorSource, RandomGenerator, SequenceGenerator}
import org.apache.flink.streaming.api.scala._

object DataStreamDataGenDemo {
  def main(args: Array[String]): Unit = {

    val configuration = new Configuration()

    val env = StreamExecutionEnvironment
      .createLocalEnvironmentWithWebUI(configuration)

    env.setParallelism(1)
    env.disableOperatorChaining()



    // 1.0 生成随机数据RandomGenerator
    val orderInfods = env.addSource(
      new DataGeneratorSource[OrderInfo](
        new RandomGenerator[OrderInfo]() {
          override def next() = {
            OrderInfo(
              random.nextInt(1, 100000),
              random.nextLong(1, 100000),
              random.nextUniform(1, 1000),
              System.currentTimeMillis()
            )
          }
        }
      ))
    // 1.0 生成序列数据SequenceGenerator
    val userInfods = env.addSource(
      new DataGeneratorSource[UserInfo](
        new SequenceGenerator[UserInfo](1,1000000) {
          val random = new RandomDataGenerator()
          override def next() = {
            UserInfo(
              // poll拿出不放回
              // peek拿出放回
              valuesToEmit.poll().intValue(),
              valuesToEmit.poll().longValue(),
              random.nextInt(1, 100),
              random.nextInt(0, 1)
            )
          }
        }
      ))
    orderInfods.print("orderinfo>>>>>>>>>>>>>>>>")
    userInfods.print("userinfo>>>")
    env.execute()

  }
}

case class OrderInfo(
                      id: Int,
                      user_id: Long,
                      total_amount: Double,
                      create_time: Long
                    )

case class UserInfo(
                     id: Int,
                     user_id: Long,
                     age: Int,
                     sex: Int
                   )
