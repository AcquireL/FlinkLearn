package com.kali.flink.core.operator.window.eventtime;

import com.alibaba.fastjson2.JSONObject;
import com.kali.flink.core.connector.generator.GeneratorSource;
import com.kali.flink.core.connector.generator.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WindowAllTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.disableOperatorChaining();
        // env.getConfig().setAutoWatermarkInterval(10); // 单位是ms, 默认是200ms;
        DataStreamSource<String> source = new GeneratorSource().dataStreamSource(env);
        DataStream<User> transferData = source.map((MapFunction<String, User>) s -> JSONObject.parseObject(s, User.class));

        // transferData.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks()); //禁用时间时间的推进机制
        // transferData.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()); //紧跟最大时间时间
        // transferData.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator()); //自定义watermark生成算法

        SingleOutputStreamOperator<User> watermarksData = transferData.assignTimestampsAndWatermarks(WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((user, l) -> user.getTs()));

        watermarksData.process(new ProcessFunction<User, User>() {
            @Override
            public void open(OpenContext openContext) throws Exception {
                super.open(openContext);
            }

            @Override
            public void processElement(User value, ProcessFunction<User, User>.Context ctx, Collector<User> out) throws Exception {
                Long ts = ctx.timestamp();
                long watermark = ctx.timerService().currentWatermark();
                System.out.println("ts:" + ts + "  watermark:" + watermark);
                out.collect(value);
            }
        });

        // watermarksData.print();
        env.execute();
    }
}
