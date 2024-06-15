package com.kali.flink.core.operator.window.processtime;

import com.kali.flink.core.operator.window.Entity.WordCountCart;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

/**
 * 滚动窗口运用
 * 统计每5秒钟,各个路口通过红绿灯汽车的数量
 * 按照 时间 来进行窗口划分,每次窗口的 滑动距离 等于窗口的长度,这样数据不会重复计算
 */
public class StreamingTumblingTimeWindow {
    public static void main(String[] args) throws Exception {
        // 1.创建运行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime );
        // 2.定义数据源
        DataStreamSource<String> textStream = env.socketTextStream("acquirel", 9999);

        textStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)));
        //3.转换数据格式，text->CarWc
        SingleOutputStreamOperator<WordCountCart> data = textStream.map(line -> {
            String[] split = line.split(",");
            return new WordCountCart(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
        });
        // 4.执行统计操作，每个sensorId一个tumbling窗口，窗口的大小为5秒。
        SingleOutputStreamOperator<WordCountCart> result = data.keyBy(WordCountCart::getSen)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .reduce((a, b) -> new WordCountCart(a.getSen(), a.getCardNum() + b.getCardNum()));
//        new ProcessFunction<String,String>(     );
        // 5.显示统计结果
        result.print();
        // 6.触发流计算
        env.execute();
    }
}
