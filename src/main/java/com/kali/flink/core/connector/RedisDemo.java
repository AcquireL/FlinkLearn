package com.kali.flink.core.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * Author itcast
 * Desc 演示Flink-Connectors-三方提供的RedisSink
 */
public class RedisDemo {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<String> lines = env.socketTextStream("learn", 9999);

        //TODO 2.transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).sum(1);


        //TODO 3.sink
        result.print();

        FlinkJedisPoolConfig  conf= new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<Tuple2<String, Integer>>(conf,new MyRedisMapper());
        result.addSink(redisSink);

        //TODO 4.execute
        env.execute();
    }
    public static class MyRedisMapper implements RedisMapper<Tuple2<String, Integer>>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            //我们选择的数据结构对应的是 key:String("wcresult"),value:Hash(单词,数量),命令为HSET
            return new RedisCommandDescription(RedisCommand.HSET,"wcresult");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> t) {
            return  t.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> t) {
            return t.f1.toString();
        }
    }
}
