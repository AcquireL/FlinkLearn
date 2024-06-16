package com.kali.flink.core.operator.broadcast;

import com.kali.flink.core.operator.broadcast.Entity.DimData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class KeyByBroadTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 从数据源获取维表数据流
        ArrayList<DimData> list = new ArrayList<>();
        list.add(new DimData("bob", "100"));
        list.add(new DimData("lucy", "10"));
        DataStream<DimData> dimDataStream = env.fromCollection(list);

        // 创建维表广播状态描述符
        MapStateDescriptor<String, DimData> dimStateDescriptor = new MapStateDescriptor<>(
                "DimBroadcastState",
                Types.STRING,
                Types.POJO(DimData.class)
        );

        // 广播维表数据流
        BroadcastStream<DimData> broadcastDimStream = dimDataStream.broadcast(dimStateDescriptor);

        // 主流数据
        KeyedStream<Tuple2<String, Integer>, String> factStream = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] split = s.split(":");
                        return new Tuple2<>(split[0], Integer.valueOf(split[1]));
                    }
                })
                .keyBy(value -> value.f0);

        DataStream<Tuple2<String, Integer>> dataStream = factStream.connect(broadcastDimStream)
                .process(new KeyedBroadcastProcessFunction<String, Tuple2<String, Integer>, DimData, Tuple2<String, Integer>>() {
                    private transient BroadcastState<String, DimData> broadcastState;

                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedBroadcastProcessFunction<String, Tuple2<String, Integer>, DimData, Tuple2<String, Integer>>.ReadOnlyContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        DimData dimData = broadcastState.get(value.f0);
                        if (dimData != null) {
                            out.collect(new Tuple2<>(value.f0, value.f1 + Integer.parseInt(dimData.getValue())));
                        }
                    }

                    @Override
                    public void processBroadcastElement(DimData value, KeyedBroadcastProcessFunction<String, Tuple2<String, Integer>, DimData, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        broadcastState = ctx.getBroadcastState(dimStateDescriptor);
                        // 更新广播状态
                        broadcastState.put(value.getKey(), value);
                    }
                });
        dataStream.print();

        env.execute("MapStateBroadTest");
    }
}


