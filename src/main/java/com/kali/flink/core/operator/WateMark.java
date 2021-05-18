package com.kali.flink.core.operator;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor.IgnoringHandler;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;

public class WateMark {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //界面上指定参数(本地测试可以忽略，设置了默认值)
        final ParameterTool params = ParameterTool.fromArgs(args);
        String hostName = params.get("hostname", "localhost");
        int port = params.getInt("port", 9999);

        //数据来源(获取sourceStream)
        DataStream<String> sourceStream = env.socketTextStream(hostName, port, "\n");


        // 固定时间做为watemark
        SingleOutputStreamOperator<Tuple2<Long, String>> map = sourceStream.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String s) throws Exception {
                Tuple2<Long, String> data = new Tuple2<Long, String>();
                String[] split = s.split(",");
                data.setFields(Long.valueOf(split[0]), split[1]);
                return data;
            }
        });
        // 固定时间做为watemark
        SingleOutputStreamOperator<Tuple2<Long, String>> tuple2SingleOutputStreamOperator = map
                .assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarksAdapter.Strategy<Tuple2<Long, String>>(
                                new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, String>>(Time.milliseconds(30)) {
            @Override
            public long extractTimestamp(Tuple2<Long, String> element) {
                return element.f0;
            }
        }));


        // 使用前一条数据的tiimestime+某个时间作为watermark
        SingleOutputStreamOperator<Tuple2<Long, String>> tuple2SingleOutputStreamOperator1 = map
                .assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarksAdapter.Strategy<Tuple2<Long, String>>(
                                new AscendingTimestampExtractor<Tuple2<Long, String>>() {
                                    @Override
                                    public long extractAscendingTimestamp(Tuple2<Long, String> element) {
                                        return element.f0+30;
                                    }
                                }));

        // 自定义实现WatermarkStrategy



    }
}
