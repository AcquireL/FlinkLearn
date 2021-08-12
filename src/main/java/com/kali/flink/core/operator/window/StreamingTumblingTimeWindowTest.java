package com.kali.flink.core.operator.window;

import com.kali.flink.core.operator.window.Entity.WordCountCart;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 *  滚动窗口运用
 *  按照 时间 来进行窗口划分,每次窗口的 滑动距离 等于窗口的长度,这样数据不会重复计算
 */
public class StreamingTumblingTimeWindowTest {
    public static void main(String[] args) throws Exception {
        // 1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.定义数据源
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9999);
        //3.转换数据格式，text->CarWc
        SingleOutputStreamOperator<WordCountCart> data = socketStream.map(line -> {
            String[] split = line.split(",");
            return new WordCountCart(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
        });

        data.writeAsText("");

        // data.windowAll(new TumblingProcessingTimeWindows(Time.seconds(1)));

        // 5.显示统计结果
        //result.print();
        // 6.触发流计算
        env.execute();
    }
}
