package com.kali.flink.core.connector.hbase;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kali.flink.core.connector.kafka.consumer.MyKafkaConsumer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

public class HBaseSinkFunctionTest {

    // {"aid":"1234","sid":"abc","svid":"qaz"}
    // 测试kafkaToHbase的用法
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setRestartStrategy(RestartStrategies.noRestart());

        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaConsumer.getKafkaConsumer();

        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);

        dataStreamSource.print();

        SingleOutputStreamOperator<Tuple2<Boolean, Row>> map = dataStreamSource.map(new MapFunction<String, Tuple2<Boolean, Row>>() {
            @Override
            public Tuple2<Boolean, Row> map(String json) throws Exception {
                JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
                String aid = jsonObject.get("aid").getAsString();
                String sid = jsonObject.get("sid").getAsString();
                String svid = jsonObject.get("svid").getAsString();
                Row row = new Row(3);
                row.setField(0, aid);
                row.setField(1, sid);
                row.setField(2, svid);
                Row familyRow = new Row(2);
                familyRow.setField(0, aid);
                familyRow.setField(1, row);
                return new Tuple2<Boolean, Row>(true, familyRow);
            }
        });
        map.print();
        HBaseSinkFunction<Tuple2<Boolean, Row>> hbaseSinkFunction = MyHbaseBaseSinkFunction.getHbaseSinkFunction();

        map.addSink(hbaseSinkFunction);

        env.execute("sinkToHbase");
    }


}
