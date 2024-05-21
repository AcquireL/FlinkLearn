//package com.kali.flink.task
//
//
//import com.kali.flink.bean.ClickLogWide
//import com.kali.flink.util.{HBaseUtil, TimeUtil}
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.functions.sink.SinkFunction
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//
///**
// * Author itcast
// * Desc 实时统计各个频道各个时间段的pv/uv
// */
//object ChannelPvUvTask {
//
//  case class ChannelRealPvUv(channelId: String, monthDayHour: String, pv: Long, uv: Long)
//
//  def process(clickLogWideDS: DataStream[ClickLogWide]) = {
//    //对于一条日志信息进来需要统计各个时间段(月/日/小时--3个维度)的结果,也就是一条进来多条出去
//    //回忆之前的的api,"hello word" 一行进去, 出来 [hello,word]用的flatMap,所以这里也一样,应该使用flatMap来处理
//    val resultDS: DataStream[ChannelRealPvUv] = clickLogWideDS.flatMap(log => {
//      List(
//        ChannelRealPvUv(log.channelID, TimeUtil.parseTime(log.timestamp, "yyyyMMddHH"), 1, log.isHourNew),
//        ChannelRealPvUv(log.channelID, TimeUtil.parseTime(log.timestamp, "yyyyMMdd"), 1, log.isDayNew),
//        ChannelRealPvUv(log.channelID, TimeUtil.parseTime(log.timestamp, "yyyyMM"), 1, log.isMonthNew)
//      )
//    }).keyBy("channelId", "monthDayHour")
//      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .reduce((a, b) => {
//        ChannelRealPvUv(a.channelId, a.monthDayHour, a.pv + b.pv, a.uv + b.uv)
//      })
//
//    resultDS.addSink(new SinkFunction[ChannelRealPvUv] {
//      override def invoke(value: ChannelRealPvUv, context: SinkFunction.Context): Unit = {
//        //查
//        val tableName = "channel_pvuv"
//        val rowkey = value.channelId + ":" + value.monthDayHour
//        val columnFamily = "info"
//        val queryColumn1 = "pv"
//        val queryColumn2 = "uv"
//
//        //pvuvMap: Map[pv, 100]
//        //pvuvMap: Map[uv, 100]
//        val pvuvMap: Map[String, String] = HBaseUtil.getMapData(tableName, rowkey, columnFamily, List(queryColumn1, queryColumn2))
//        //注意:返回的map本身不为null,但是里面有可能没有pv/uv对应的值
//
//        val historyPv: String = pvuvMap.getOrElse(queryColumn1, "0")
//        val historyUv: String = pvuvMap.getOrElse(queryColumn2, "0")
//
//        //合
//        val resultPV: Long = value.pv + historyPv.toLong
//        val resultUV: Long = value.uv + historyUv.toLong
//
//        //存
//        HBaseUtil.putMapData(tableName, rowkey, columnFamily, Map(
//          queryColumn1 -> resultPV.toString, //第一个列的列名和对应的值
//          queryColumn2 -> resultUV.toString //第二个列的列名和对应的值
//        ))
//      }
//    })
//  }
//}
