package com.kali.flink

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject}
import com.kali.flink.bean.{ClickLog, ClickLogWide, Message}
import com.kali.flink.task.{ChannelHotTask, ChannelPvUvTask, DataToWideTask}
import org.apache.commons.lang3.SystemUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Author itcast
 * Desc scala-flink程序入口类
 */
object App {
  def main(args: Array[String]): Unit = {
    //TODO 0.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //TODO ===Checkpoint参数设置
    //===========类型1:必须参数=============
    //设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
    env.enableCheckpointing(1000)
    //设置State状态存储介质/状态后端
    if (SystemUtils.IS_OS_WINDOWS) {
      env.setStateBackend(new FsStateBackend("file:///D:/ckp"))
    }
    else {
      env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink-checkpoint/checkpoint"))
    }
    //===========类型2:建议参数===========
    //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
    //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500) //默认是0

    //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
    //env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(10) //默认值为0，表示不容忍任何检查点失败

    //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //===========类型3:直接使用默认的即可===============
    //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
    env.getCheckpointConfig.setCheckpointTimeout(60000) //默认10分钟

    //设置同一时间有多少个checkpoint可以同时执行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) //默认为1


    //TODO ===配置重启策略:
    //1.配置了Checkpoint的情况下不做任务配置:默认是无限重启并自动恢复,可以解决小问题,但是可能会隐藏真正的bug
    //2.单独配置无重启策略
    //env.setRestartStrategy(RestartStrategies.noRestart());
    //3.固定延迟重启--开发中常用
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        3, // 最多重启3次数
        Time.of(5, TimeUnit.SECONDS)
      )
    )
    // 重启时间间隔)
    //上面的设置表示:如果job失败,重启3次, 每次间隔5s
    //4.失败率重启--开发中偶尔使用
    /*env.setRestartStrategy(RestartStrategies.failureRateRestart(
            3, // 每个测量阶段内最大失败次数
            Time.of(1, TimeUnit.MINUTES), //失败率测量的时间间隔
            Time.of(3, TimeUnit.SECONDS) // 两次连续重启的时间间隔
    ));*/
    //上面的设置表示:如果1分钟内job失败不超过三次,自动重启,每次重启间隔3s (如果1分钟内程序失败达到3次,则程序退出)

    //TODO 1.source-kafka-pyg主题
    //准备kafka连接参数
    val props: Properties = new Properties
    props.setProperty("bootstrap.servers", "node1:9092") //集群地址
    props.setProperty("group.id", "flink") //消费者组id
    props.setProperty("auto.offset.reset", "latest") //latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
    props.setProperty("flink.partition-discovery.interval-millis", "5000") //会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
    //props.setProperty("enable.auto.commit", "true") //自动提交(提交到默认主题,后续学习了Checkpoint后随着Checkpoint存储在Checkpoint和默认主题中)
    //props.setProperty("auto.commit.interval.ms", "2000") //自动提交的时间间隔
    //使用连接参数创建FlinkKafkaConsumer/kafkaSource
    val kafkaSource: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("pyg", new SimpleStringSchema, props)
    kafkaSource.setCommitOffsetsOnCheckpoints(true)//执行Checkpoint的时候提交offset到Checkpoint
    //使用kafkaSource
    import org.apache.flink.streaming.api.scala._
    //DataStream[里面就是一条条的json数据]
    val kafkaDS: DataStream[String] = env.addSource(kafkaSource)
    //kafkaDS.print()

    //TODO 2.transformation
    //TODO ===数据预处理-将json转为样例类
    val messageDS: DataStream[Message] = kafkaDS.map(jsonStr => {
      //jsonStr转为jsonObject
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      val count: Long = jsonObj.getLong("count")
      val timeStamp: Long = jsonObj.getLong("timeStamp")
      val messageJsonStr: String = jsonObj.getString("message")
      val clickLog: ClickLog = JSON.parseObject(messageJsonStr, classOf[ClickLog])
      Message(clickLog, count, timeStamp)

      //注意:得使用上面的一步步的转换,不能够偷懒使用下面的这一行,因为原始json是嵌套的,且字段名和样例类中不匹配
      //val message: Message = JSON.parseObject(jsonStr,classOf[Message])
    })
    //messageDS.print()
    //Message(ClickLog(12,7,12,china,HeNan,LuoYang,电信,必应跳转,qq浏览器,1577876460000,1577898060000,19),1,1611283392078)
    val messageDSWithWatermark: DataStream[Message] = messageDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Message](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[Message] {
        override def extractTimestamp(element: Message, recordTimestamp: Long): Long = element.timeStamp
      })
    )

    //TODO ===数据预处理-将Message拓宽为ClickLogWide
    val clickLogWideDS: DataStream[ClickLogWide] = DataToWideTask.process(messageDSWithWatermark)
    //clickLogWideDS.print()

    //TODO ===实时频道热点统计分析
    ChannelHotTask.process(clickLogWideDS)

    //TODO ===实时各个频道各个时间段的PvUv
    ChannelPvUvTask.process(clickLogWideDS)

    //TODO 3.sink

    //TODO 4.execute
    env.execute()

  }
}
