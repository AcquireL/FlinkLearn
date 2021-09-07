package com.kali.flink.core.feature;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConnectStateToSinkExactlyOnce {
    public final static MapStateDescriptor<String, String> myStateDescriptor = new MapStateDescriptor<>(
            "MyBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        //开启Checkpoint
        //===========类型1:必须参数=============
        //设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
        env.enableCheckpointing(1000);
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/GitSpace/git-hub/FlinkLearn"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://learn:9000/flink-checkpoint/checkpoint"));
        }

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //TODO ===配置重启策略:
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启3次数
                Time.of(5, TimeUnit.SECONDS) // 重启时间间隔
        ));

        //TODO 1.source-主题:flink-kafka1
        //准备kafka连接参数
        Properties props1 = new Properties();
        props1.setProperty("bootstrap.servers", "learn:9092");//集群地址
        props1.setProperty("group.id", "flink");//消费者组id
        props1.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("flink_kafka1", new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                byte[] value = consumerRecord.value();
                return new String(value);
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(new TypeHint<String>() {
                });
            }
        }, props1);
        //kafkaSource.setCommitOffsetsOnCheckpoints(true);//默认就是true//在做Checkpoint的时候提交offset到Checkpoint(为容错)和默认主题(为了外部工具获取)中

        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);

        BroadcastStream<String> brocastStream = env.fromElements("abc").broadcast(myStateDescriptor);

        SingleOutputStreamOperator<String> process = kafkaDS.connect(brocastStream).process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                String mysqlData = ctx.getBroadcastState(ConnectStateToSinkExactlyOnce.myStateDescriptor).get("MyBroadcastState");
                out.collect(mysqlData + value);
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                System.out.println(value);
                ctx.getBroadcastState(ConnectStateToSinkExactlyOnce.myStateDescriptor).put("MyBroadcastState", value);
            }
        });
        process.print();

        //TODO 3.sink-主题:flink-kafka2
        Properties props2 = new Properties();
        props2.setProperty("bootstrap.servers", "learn:9092");
        props2.setProperty("transaction.timeout.ms", "5000");

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "flink_kafka2",                  // target topic
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String s, Long aLong) {
                        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<byte[], byte[]>("", s.substring(0, 1).getBytes(), s.getBytes());
                        return producerRecord;
                    }
                },
                props2,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

        //result.addSink(kafkaSink);
        process.addSink(kafkaSink);

        //TODO 4.execute
        env.execute();
    }
}
