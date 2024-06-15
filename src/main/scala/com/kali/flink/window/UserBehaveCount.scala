package com.kali.flink.window


case class UserBehavior(
                         var userId: String,
                         var behavior: String,
                         var channel: String,
                         var ts: Long
                       )

class UserBehaveCount {

}

object UserBehaveCount {
  def main(args: Array[String]): Unit = {

  }
}

