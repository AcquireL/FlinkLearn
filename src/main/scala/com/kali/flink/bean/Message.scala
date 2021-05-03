package com.kali.flink.bean

import com.alibaba.fastjson.JSON


case class Message(
                    clickLog: ClickLog,
                    count: Long,
                    timeStamp: Long
                  )

case class ClickLog(
                     channelID: String, //频道ID
                     categoryID: String, //产品类别ID
                     produceID: String, //产品ID
                     country: String, //国家
                     province: String, //省份
                     city: String, //城市
                     network: String, //网络方式
                     source: String, //来源方式
                     browserType: String, //浏览器类型
                     entryTime: String, //进入网站时间
                     leaveTime: String, //离开网站时间
                     userID: String //用户的ID
                   )

object ClickLog {
  def apply(json: String): ClickLog = {
    JSON.parseObject(json, classOf[ClickLog])
  }
}