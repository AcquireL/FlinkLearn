package com.kali.flink.core.operator.window;

import com.kali.flink.core.operator.window.Entity.WordCountCart;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *  滚动窗口运用
 *  按照 时间 来进行窗口划分,每次窗口的 滑动距离 等于窗口的长度,这样数据不会重复计算
 */
public class StreamingTumblingTimeWindow {
    public static void main(String[] args) throws Exception {
        // 1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.定义数据源
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);
        //3.转换数据格式，text->CarWc
        SingleOutputStreamOperator<WordCountCart> data = textStream.map(line -> {
            String[] split = line.split(",");
            return new WordCountCart(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
        });
        // 4.执行统计操作，每个sensorId一个tumbling窗口，窗口的大小为5秒
        // 也就是说，每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量。
        KeyedStream<WordCountCart, Integer> keybyData = data.keyBy(x ->x.getSen());
        // 无重叠数据，所以只需要给一个参数即可，每5秒钟统计一下各个路口通过红绿灯汽车的数量
        keybyData.timeWindow(Time.seconds(5)).reduce(new ReduceFunction<WordCountCart>() {
            @Override
            public WordCountCart reduce(WordCountCart value1, WordCountCart value2) throws Exception {
                return new WordCountCart(value1.getSen(),value1.getCardNum()+value2.getCardNum());
            }
        });
        // 5.显示统计结果
        //result.print();
        // 6.触发流计算
        env.execute();
    }
}
