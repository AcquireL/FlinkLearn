package com.kali.flink.bean

case class ClickLogWide(
   channelID: String,
   categoryID: String,
   produceID: String,
   country: String,
   province: String,
   city: String,
   network: String,
   source: String,
   browserType: String,
   entryTime: String,
   leaveTime: String,
   userID: String,
   count: Long, //用户访问的次数
   timestamp: Long, //用户访问的时间
   address: String, //国家省份城市-拼接
   yearMonth: String, //年月
   yearMonthDay: String, //年月日
   yearMonthDayHour: String, //年月日时
   isNew: Int, //是否为访问某个频道的新用户——0表示否，1表示是
   isHourNew: Int, //在某一小时内是否为某个频道的新用户——0表示否，1表示是
   isDayNew: Int, //在某一天是否为某个频道的新用户—0表示否，1表示是
   isMonthNew: Int //在某一个月是否为某个频道的新用户——0表示否，1表示是
)