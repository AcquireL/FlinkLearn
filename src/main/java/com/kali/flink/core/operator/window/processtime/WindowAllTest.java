package com.kali.flink.core.operator.window.processtime;

import com.kali.flink.core.operator.window.Entity.WordCountCart;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * 滚动窗口运用
 * 按照 时间 来进行窗口划分,每次窗口的 滑动距离 等于窗口的长度,这样数据不会重复计算
 */
public class WindowAllTest {
    public static void main(String[] args) throws Exception {
        // 1.创建运行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 2.定义数据源
        DataStreamSource<String> socketStream = env.socketTextStream("acquirel", 9999);
        //3.转换数据格式，text->CarWc
        SingleOutputStreamOperator<WordCountCart> data = socketStream.map(line -> {
            String[] split = line.split(",");
            return new WordCountCart(Integer.parseInt(split[0]), Integer.valueOf(split[1]));
        });

        SingleOutputStreamOperator<WordCountCart> sum = data
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(20)))
                .trigger(ContinuousProcessingTimeTrigger.of(Duration.ofSeconds(20)))
                .apply(new AllWindowFunction<WordCountCart, WordCountCart, TimeWindow>() {
                    int count = 0;

                    @Override
                    public void apply(TimeWindow window, Iterable<WordCountCart> values, Collector<WordCountCart> out) throws Exception {

                        for (WordCountCart value : values) {
                            count += value.getCardNum();
                        }
                        out.collect(new WordCountCart(1, count));
                    }
                });

        // 5.显示统计结果
        sum.print();
        // 6.触发流计算
        env.execute();
    }

}
